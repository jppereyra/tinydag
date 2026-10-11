# Scheduler Plan

Assessment of the codebase (October 2026) and the phased plan for the scheduler milestone. Approved 2026-10-10.

## Context

The assessment was done at `aabf241`, right after the efficiency work (version hashing, python worker pool, bash caches, executor cap), the ratatui UI was parked on `wip/tui`, and batch `tinydag add` shipped. The next big step is the scheduler. The owner wants it to be the most elegantly engineered part of the system: a contiguous, cache-line-aligned in-memory working set, a transparent snapshot process, resume-and-reconcile after restart, and designed for 10⁵–10⁶ pipelines on one box even though 10k is the realistic near-term target.

This plan has two halves: (1) an honest assessment of where the code stands (48 findings, each checked independently for accuracy and for whether it matters; every claim backed by file:line evidence), and (2) the design and phased plan for the scheduler, with the prep work the scheduler would otherwise sit on top of.

Decisions the owner has already made (not up for re-litigation):
- Scheduler lives in the library crate; the CLI gains a long-running `tinydag serve`.
- Triggers: manual + run registry first, then cron, then pipeline_completion, then event.
- Pure in-memory hot state; persistence is a periodic copy of the whole working set, transparent to the hot path, eventually by a separate process.
- Hot working set = contiguous region of fixed-size, cache-line rows; variable/immutable data cold; history on the event stream.
- Restart = load last snapshot, resume, reconcile in-flight work with executors (which may be remote with their own resiliency). Cron catch-up off by default, opt-in per pipeline.
- Speed for speed's sake: no O(n) per tick, no lock on the hot path.

## Status

| Phase | State |
|---|---|
| 0a Refactors, no behaviour change | Done: `1576b00`, `4849f2f`, `02f2cc9` |
| 0b Defect fixes on the unary protocol | Done: `25ba055` … `7075b3d` (one commit per defect, see Part 1) |
| 1 Region | Next |
| 2–10 | Not started |

Deviations from the plan so far:
- `tempfile` stays a regular dependency: the `bash -n` check of inline commands uses it outside tests (`src/operators/bash.rs`).
- The tokio `signal` feature moves to Phase 5, where `serve` first uses it.
- Defect 3 (cancellation) is partly done: aborting a dispatch now sends SIGTERM to its operator (`1c159e3`). The full cancel path arrives with Session v2 in Phase 3.
- A duplicate `dag_id` within one `tinydag add` is an error, but which file wins depends on compile completion order. Phase 2's registration rework gives re-registration proper semantics.

## Part 1: Assessment

### Overall

Every subsystem is **working-but-rough**: the shape is right and tests pass (89 lib + 4 bin, clippy clean), but the layer the scheduler needs (run identity, per-task state, cancellation, a clock, shutdown) does not exist yet, and there are a handful of real defects at the executor/runner seam. The compile side (Starlark DSL, validation, hashing, batch add) is solid and can be left alone apart from two bugs.

Calibration the critic insisted on, and it's right: today one task costs a subprocess spawn plus a gRPC round-trip (milliseconds). Scheduler microseconds are headroom, not end-to-end latency. We build for the headroom anyway because that's the stated goal; just don't expect faster pipelines from it.

### What's solid (keep)
- Graph is acyclic by construction (`depends_on` takes task values; BFS from terminals dedups by identity) — `src/compiler.rs:247-292`.
- Validation aggregates all structural errors with fix hints — `src/validation.rs:88-253`.
- Content-addressed version hash, stable across node/param order — `src/dag.rs:168-221`.
- Validation is bounded and cached (python pool, `bash -n` caches), which is what makes batch add of hundreds of pipelines feasible.
- `Executor` is a one-method trait: the seam for remote backends is in the right place.
- Batch `add` already reports progress as events (`src/batch.rs`), the same pattern the scheduler will generalize.

### Confirmed defects (verified in code)

Line references in this section are to the code at `aabf241`; fixes since then have moved them.

Executor / operator seam:
1. **Timeout kill orphans the user workload.** *[Fixed in `25ba055`.]* `start_kill()` is SIGKILL (`src/executor.rs:396`, `:419`); the operator's cleanup is a SIGTERM handler that `killpg`s its `setsid` child (`src/operators/runtime.rs:37-45`, `bash.rs:251`). SIGKILL never runs it, so the script keeps running re-parented to init and the work dir leaks. Fix: SIGTERM → grace → SIGKILL, and `kill_on_drop`.
2. **The child is never watched.** *[Fixed in `e6c7df7`.]* The dispatch loop awaits only `event_rx.recv()` (`src/executor.rs:344`); `child.wait()` runs only after a terminal event. An operator that crashes or exits early is reported 90 s later as `TaskTimedOut`. Fix: `select!` on `child.wait()` too.
3. **No cancellation path.** *[Partly fixed in `1c159e3`; rest in Phase 3.]* Dropping a dispatch future leaves the process running; the trait has no cancel; the proto is one-directional. (Needed by shutdown, by failure-cancels-siblings, and by reconcile.)
4. **FailedEvent `error_type`/`message` discarded** *[Fixed in `6bd3c46` (failure type and message). Task stdout is still discarded.]* at the executor boundary (`src/executor.rs:388`); `TaskFailed.stderr` is always empty; task stdout is `Stdio::null()`.
5. **Metrics cardinality** *[Fixed in `6aff5e4`.]*: `node.id`/`dag.id` attributes on per-task histograms (`src/executor.rs:263`) overflow the OTel SDK's 2000-set limit at scale, silently losing series.

Runner / scheduler:
6. **A panicking dispatch task hangs the run forever.** *[Fixed in `5578533`.]* Spawned tasks are never joined (`src/runner.rs:197-210`); if `dispatch` panics before `send`, `in_flight` never decrements and `recv()` waits forever. Fix: `JoinSet`, map `JoinError` to task failure.
7. **On failure, siblings are neither cancelled nor collected** *[Fixed in `1c159e3`.]* (`src/runner.rs:223`): a run with one failed node and a sibling running `sleep 3600` stays "running" for an hour.
8. **`RunError` loses run identity and partial state** *[Partly fixed: the `<unknown>` metric label is gone (`6aff5e4`); run identity on errors and spans is Phase 2.]*; `run.id` never lands on the span (field not declared, `runner.rs:79`); failed runs are recorded under `dag.id="<unknown>"`.
9. **Per-dispatch linear scans and whole-struct clones** *[Fixed in `1576b00` (index-based plan, shared `Arc<DagDef>`). `params` is still cloned per payload.]*: `nodes.iter().find`, edge filter per node, `DagDef` cloned per trigger (`src/scheduler.rs:46`), `params` cloned per payload.
10. **Dispatch order is nondeterministic** *[Fixed in `1576b00`.]* (ready set seeded from a HashMap), so no fairness or priority can be layered on.

Compiler / batch:
11. **`FanInOutputConflict` rejects valid fan-in** *[Fixed in `c47d49a`.]* (`src/validation.rs:172`): it ignores whether the consumer declares the colliding output as an input; the runner only forwards declared inputs. The README's `regional()` pattern with homogeneous outputs cannot compile.
12. **Duplicate `dag_id` in a batch silently overwrites and double-runs** *[Fixed in `d6b4196`.]* (`src/batch.rs:160`, `scheduler.rs:33`); winner is nondeterministic.
13. **`max_attempts`/`delay_secs` accepted by the DSL and silently ignored** *[Open: Phase 2.]*; retry/timeout/trigger excluded from the version hash (TODO says v0.2 retries — fine, but the DSL should not accept what it ignores, or the hash should include it).
14. **Validation caches never invalidate** *[Fixed in `e85e6be`.]* (`bash.rs:155-162`, `python_pool.rs:147`): fine for one-shot CLI, wrong inside a daemon that re-registers edited pipelines. Key on (path, mtime, size) or content hash before `serve` exists.
15. **Exit codes diverge from TODO** *[Fixed in `7075b3d`.]* (3 for task failure, spec says 2; usage errors share 1 with compile errors).

Process:
16. **No signal handling, no graceful shutdown** *[Open: Phase 5.]*: tokio `signal` feature is off, control server is a detached task with no shutdown handle (`control_server.rs:158-164`).
17. **Control server acks events for unknown (run, task) keys** *[Fixed in `2b884b4`.]* silently (`control_server.rs:74-83`); late events from killed operators vanish without a log line.

### Design/documentation drift (fix docs or code; several are decisions)
- DSL cannot express `event` or `pipeline_completion` triggers; cron string is never parsed (`compiler.rs:351-354`). Phase 2/5 work.
- `load()`/`.tdg` libraries, `//` paths, `tinydag.toml`, `resolver=`, `preconditions=`, `late()`, `none()` are documented in present tense and do not exist (`design/03`).
- PythonOperator is a script runner, not a "callable reference"; inputs are a JSON file, not env vars (`design/03`, `04`, `06`, `08`). `design/03` now carries a status note (`02f2cc9`); the broken `examples/README.md` link is fixed.
- "Inter-pipeline dependency field from day one" (`design/02:96`) is absent from `DagDef`.
- `pipeline_id` is optional and non-unique while `PipelineCompletion` keys on it; `pipeline_id` defaults to `""`. Resolved by D1: completion triggers key on `dag_id`; `pipeline_id` stays an optional grouping label (no default change, no hash churn).
- Absolute script paths baked into `TaskRef` and the hash make "compile in CI, run on the scheduler box" impossible as documented.
- Dependency hygiene: two tonic + two prost versions linked (opentelemetry-otlp 0.31 → tonic 0.14); operator binaries link starlark + OTel; no `rust-version`; `tempfile` should be dev-only.
- ~~TODO.md is stale~~: fixed in `02f2cc9` and `7075b3d`.

### Test gaps (fill as part of the phases, not before)
- No mock `Executor`; all runner/scheduler tests spawn the real binary.
- Nothing tests output propagation, parallel-branch overlap, failure-with-siblings-in-flight, heartbeat expiry, the control server, `run_batch`, or any DAG larger than 4 nodes. No benchmark exists.

### Dropped after verification (so they don't come back)
- "DagDef is Serialize-only / not loadable": true, but deferred by design until the registration model is settled.
- "Control server panic unobserved": tonic's accept loop logs and continues; effectively unreachable.
- "`bash -n` subprocesses uncapped": validation runs serially per DAG; not a real cost.

## Part 2: Scheduler design and phases

### Decisions from the owner Q&A (override the design panels where they conflict)

**D1. `pipeline_completion` keys on `dag_id`.** The registry is keyed by `dag_id`, and `dag_id` gets enforced unique at registration (fixes defect 12). `pipeline_id` stays an optional grouping label; pipeline-level "all DAGs in P completed" semantics are deferred to `design/08`. No compiler default change, so no version-hash churn.
- The trigger key names the *source*; each completion event carries the upstream `run_id` as the *instance*. One upstream success → one downstream trigger, carrying `_upstream_run_id`. Concurrent upstream runs therefore produce distinct, attributable downstream triggers.
- Concurrent runs of the same DAG don't collide at the execution layer today (control-server keys are `(run_id, node_id)`, work dirs are per process+nanos). What's missing is *policy*: `Scheduler::trigger` spawns unconditionally. The design's per-DAG `Limits { max_active: 1, max_queued: 1, overlap: Queue }` default is what prevents it; scheduled triggers obey it, a manual trigger that hits it gets an explicit error.

**D2. Script identity = project-relative path + content hash; delivery = content-addressed bundle.**
- Today `resolve()` canonicalizes to an absolute host path (`bash.rs:39`, `python.rs:30`) and `hash_into` hashes that path (`bash.rs:101`), so the same pipeline compiled in two checkout directories gets two version hashes. That's a bug in its own right.
- Compile-time: resolve each script relative to the **project root = nearest ancestor directory containing `tinydag.toml`** (owner decision; falls back to the `.star` file's directory when there is none). Scripts outside the root are a compile error. Validate, then store in `TaskRef` `{ rel_path, sha256 }`. The version hash covers `rel_path` + content, never a host path. The same root later anchors `//` paths for `load()`. `tinydag.toml` starts empty; add one at the repo root and in `examples/`.
- Registration ships script bytes into a content-addressed store keyed by `sha256` (scheduler-side directory). Dispatch payload carries `{ rel_path, sha256 }`. The executor materializes the script into the task work dir from its local CAS cache, fetching from the scheduler's artifact endpoint on a miss (LocalExecutor: same filesystem, zero copy; remote: fetch once per hash, then cached forever). Hash mismatch on materialize → fail fast with `INVALID_CONFIG`.

**D3. Operators are the source of truth for task state and report directly to the scheduler; no executor status query.**
- Control-layer signals (started, heartbeat, succeeded+outputs, failed) go operator → scheduler control endpoint over gRPC, which is already the protocol; it becomes the scheduler's ingress lane instead of an executor-private channel. Data-layer telemetry (logs, metrics, spans) stays on OTel and never enters the scheduler loop (`design/05`).
- Reconcile after restart is *passive*: restored `dispatched` rows become `unknown`; operators keep running, reconnect with backoff, and their next heartbeat or terminal report (keyed by `run_id, node_id, attempt`) resolves the row. Rows still silent after one heartbeat timeout → `lost` → retry policy (new `attempt`) or fail.
- Requirements this puts on the protocol and operators:
  - `attempt` on every message; scheduler ignores reports for superseded attempts.
  - Terminal reports must be delivered at-least-once: operator retries `ReportSucceeded`/`ReportFailed` with backoff until acked (today they're fire-and-forget, `runtime.rs:130-153`). Scheduler handles duplicates idempotently.
  - Server must distinguish "accepted" from "unknown task": no more silent ack for unknown keys (`control_server.rs:74-83`).
  - Stable control endpoint across restarts (fixed UDS path in the state dir or a configured port), not an ephemeral port per `LocalExecutor`.
  - **Liveness channel = one bidirectional gRPC stream per running task (owner decision).** The operator opens `rpc Session(stream OperatorMsg) returns (stream SchedulerMsg)`: `OperatorMsg` = `Hello{run_id, node_id, attempt}` | `Started` | `Succeeded{outputs}` | `Failed{error_type, message, exit_code}`; `SchedulerMsg` = `Ack{seq}` | `Cancel{reason}`. gRPC/HTTP2 keepalive pings replace the 30 s application heartbeat; a dropped stream is the death signal (with a short grace for reconnect after scheduler restart). Cancel arrives instantly. Terminal messages carry a sequence number and are re-sent on reconnect until acked. The four unary RPCs are retired (proto is versioned; both sides ship together).
  - Executor-side kill remains as the fallback for operators that are wedged or unreachable.
- What the executor keeps: `dispatch(payload)` carrying the idempotency key `(run_id, node_id, attempt)`, best-effort `cancel`, and (LocalExecutor only) a fast "process exited without reporting" signal from `child.wait()` so crashes are detected in milliseconds instead of a heartbeat timeout. Air-gapped backends where operators can't reach the scheduler use an executor-side adapter that synthesizes the same events (`design/05` note); that's a later concern.

### Shape

One driver task owns a pure core, `Core::handle(input, now_ms, &mut effects)`. All mutable scheduler state is a **region**: one contiguous, pointer-free mapping of 64-byte rows (pipelines, DAG versions, runs, run contexts, tasks) that reference each other by `u32` index + `u32` generation. Everything variable-size or immutable is **cold**, reached by slot: `Arc<DagDef>`, a compiled `Plan`, outputs, params, sessions, scripts. Timers and the ready ring live outside the region and are rebuilt from row fields on restore. **A run is a block of task rows; the loop is the runner** (`src/runner.rs:104-260`'s async-fn-per-run goes away). Operators report over one bidirectional stream per task and are the source of truth (D3). Snapshot = memcpy of the region at a closed tick, written by a dedicated thread (Phase A); later a sidecar process reads a shared memfd under a seqlock (Phase B). Restart loads the snapshot, marks in-flight rows Unknown, and reconnecting operators resolve them.

Calibration (stated once): a task costs a subprocess spawn + gRPC round trip today, so milliseconds. Scheduler microseconds are headroom, not end-to-end latency.

### Hot region (`src/scheduler/region.rs`)

- **Backing:** one fixed virtual reservation sized from configured maxima (default 2× expected pipelines). Phase A: `memmap2::MmapMut::map_anon` (safe, lazily committed). Phase B: memfd with identical offsets. Tables at fixed 64 KiB-aligned offsets; growth only raises `hwm` — no realloc, no remap. Hitting a cap → `TriggerError::Capacity` / `RunSkipped{Capacity}`.
- **Safety:** `#![forbid(unsafe_code)]` in the region module. Rows derive zerocopy 0.8 `FromBytes, IntoBytes, KnownLayout, Immutable`; explicit `_pad`; `const` size/offset asserts; a pinned `layout_hash` test. Enums are `#[repr(transparent)] struct X(u8)`; `NONE = u32::MAX`.
- **Single write chokepoint:** rows only via `Tables::{pipe,dag,run,ctx,task}(idx)` / `…_mut(idx)`; header via `hdr_mut()`. First `_mut` touch of a chunk per tick stamps `epoch[c]` (~2 ns; no-op cost until Phase B). No raw `&mut [Row]` slices.
- **Chunk 0:** `StaticHeader {magic, layout_version, abi, layout_hash, boot, total_len, chunk_size, n_chunks}` + `TableDesc[8]`; separate cache lines for `LoopLine {seq, now_ms, gap_granted}`, `PersistLine {durable_seq, reader_wants_gap}`, `ColdLine {cold_durable_seq}`; `AllocState[8]`; then `epoch[]`/`prev_epoch[]`.

| Row (64 B each) | Key fields |
|---|---|
| `PipelineRow` (per `dag_id`) | `next_due_ms, last_fire_ms, dag, gen, q_head/q_tail, next, active, queued, max_active, max_queued, overlap, trigger, lifecycle` |
| `DagSlot` (per live `version_hash`) | `hash:[u8;32], refs, gen, pipeline, n_nodes, class` |
| `RunRow` (Queued/Running only) | `run_id:[u8;16], queued_ms, started_ms, pipeline, dag, task_base, gen, next, remaining, live, failed_node, state, trigger` |
| `RunCtxRow` (parallel to runs) | `upstream_run_id, scheduled_ms, upstream_pipeline, params_blob` |
| `TaskRow` (per (run,node), contiguous block per run) | `dispatched_ms, deadline_ms, grace_ms, attempt: AttemptId(u64 = boot48<<16 \| n16), run, gen, ready_seq, session_nonce, started_delta_ms, deps_left, node, state, error, flags(IN_RING\|HELD\|RESTORED), retries_used, applied_seq` |

- **Allocation:** bump + LIFO free list per table (O(1), cache-warm reuse). A run's task block is allocated when it *starts*, from 13 power-of-two size classes (≤4096 nodes; compile error above that). Queued runs own no task rows. `gen` counts occupancy only (bumped on free). `(idx, gen)` never leaves the process; external keys are `(run_id, node_ix, AttemptId)`. Restore bumps `gen` on free slots + `gen_floor` (defense in depth).
- **Sizes** (every pipeline cron'd, 20-node DAGs, 10% running capped at 64k, herd = every pipeline holds one queued run):

| | 10k | 100k | 1M |
|---|---|---|---|
| Region | ≈4.8 MB | ≈48 MB | ≈400 MB |
| Wheel + ready ring (outside) | <1 MB | ≈4 MB | ≈41 MB |

- **The memory ceiling is the cold DagDefs, not the region**: ~15 KB per 20-node DagDef → ~15 GB at 1M (the hash covers `dag_id`, `src/dag.rs:171`, so version dedupe doesn't help). Lever when needed: keep only compiled `Plan`s (~1.5 KB) resident and load DagDef bodies from `defs/<hash>.json` through an LRU at dispatch.

### Cold stores

| Store | Indexed by | Persisted as |
|---|---|---|
| `plans: Vec<Option<Arc<Plan>>>` | DagIdx | rebuilt from `defs/<hash>.json` |
| `by_hash`, `by_name`, `run_index` | key | rebuilt |
| `schedules` (deduped by expr), `cron_of` | PipelineIdx | rebuilt |
| `dependents: HashMap<PipelineIdx, Box<[(PipelineIdx, gen)]>>`, `by_event` | key | rebuilt; purged on unregister |
| outputs | run slot → node | `outputs/<run_id>/<node>.<attempt>.json`, removed when the run is freed |
| params | `RunCtxRow.params_blob` | `runs/<run_id>/params.json` |
| sessions `Vec<Option<Sender>>` | task row + `session_nonce` | not persisted |
| scripts | sha256 | `cas/<ab>/<sha256>` (D2) |

`Plan { dag, node_ix, succ_off/succ (CSR), indeg0, roots (declaration order), input_src, timeouts_s, retry, params_json }` replaces the per-run maps and linear scans at `src/runner.rs:117-135,159,163`. Registration writes `defs/` and CAS entries before the Register command reaches the loop; **the loop does no I/O**. Version swap: running runs keep their `dag`; queued runs bind the current one at start; a DagSlot frees at `refs == 0`.

### The loop

One iteration: `seq += 1` (odd) → drain ≤4096 due timers → `try_recv` ≤B operator/executor messages (ingress bounded at 64k; full → HTTP/2 flow control pushes back) → `try_recv` ≤B commands → pump → `seq += 1` (even, tick closed) → snapshot copy if due → **execute effects and publish events (every external effect follows a closed tick)** → if idle, `select!` on lanes + `clock.sleep_until_ms(next_wake)`. Injected `Clock` (i64 ms), `TestClock` (oneshot waiter list), `quiesce()`.

- **Admission (D1):** over `max_active + max_queued`, or at `max_active` with `overlap == Skip` → `RunSkipped` (manual trigger → `TriggerError::Overlap`). Else start, or enqueue on the pipeline FIFO / global admission list. Finishing releases pipeline then global slot, admitting O(1). No polling.
- **Ready ring + pump:** `VecDeque<(task, gen)>`; push only if `!IN_RING` (cannot overflow). Pump pops while `slots_in_use < budget`, skips stale/HELD, builds `DispatchRequest` from `Arc` clones. `slots_in_use` counts operators until `Exited`/final ack. After `Saturated`, pause until a capacity notice.
- **Applying results:** first transition of the *current attempt* to Succeeded stores outputs and decrements successors' `deps_left`; a successor is enqueued only if `deps_left == 0 && state == Pending && run.state == Running`. Duplicate terminals are re-acked, not reapplied. Lower attempts get `Cancel{SUPERSEDED}`.
- **Retries** (honours `RetryPolicy`, `src/dag.rs:21-26`): retryable = TASK_FAILED, RUNTIME_ERROR, UNSPECIFIED, LOST, EXITED_NO_REPORT. RetryWait with `deadline_ms = now + delay`; on fire, new `AttemptId`, Ready. Retry + timeout enter the version hash in the same phase (defect 13).
- **Fail-fast:** walk the run's block: Pending/Ready/RetryWait → Skipped; Dispatched/Running → `Cancel` → Cancelling with a 5 s grace, then `Executor::cancel`. O(nodes in run).
- **Exited without terminal** (LocalExecutor `child.wait()`) → `Failed{EXITED_NO_REPORT}`, retryable.
- **Timers:** 1 s × 4096-slot wheel (68 min horizon) + 4-ary far heap, in loop memory. Lazy deletion: an entry is honoured only if row `gen` matches and the row's field for that kind equals `due_ms` (row is truth, timer is hint). Occupancy bitmap for `next_wake`. At the 1M `:00` herd: one slot of 1M entries walked sequentially, ~0.15–0.3 µs each, in 1 ms batches (a binary heap would be ~1–2 s of pops). Nothing per tick is O(n).
- **Cron:** wrapper over `croner`, validated at compile time (`src/compiler.rs:351-354` never parses it today). Catch-up off by default → `CronMissed`; opt-in `catchup = "latest" | all(max=N)`.
- **Completion/event triggers:** `RunFinished(Succeeded)` → `admit(dep, Upstream{run_id})` for each live `(dep, gen)`; `_upstream_run_id` injected; trigger cycles rejected at register.
- **Event stream:** one `Arc<[Event]>` batch per iteration, global `seq` + `at_ms`; `try_send` to each subscriber's bounded mpsc; full → drop and count (`dropped: AtomicU64`), consumers detect gaps by `seq`. The only exit for completed-run history.

### Operator protocol and reconcile (refines D3)

```proto
package operator_control.v2;
service OperatorControl {
  rpc Session(stream OperatorMsg) returns (stream SchedulerMsg);
  rpc FetchBlob(BlobRequest) returns (stream BlobChunk);   // D2
}
message OperatorMsg { uint32 seq = 1; oneof msg { Hello hello = 2; Started started = 3; Succeeded succeeded = 4; Failed failed = 5; } }
message Hello { uint32 protocol = 1; bytes run_id = 2; string node_id = 3; fixed64 attempt = 4; }
message Failed { FailedErrorType error_type = 1; string message = 2; int32 exit_code = 3; }  // + CANCELLED, TIMED_OUT
message SchedulerMsg { oneof msg { Ack ack = 1; Cancel cancel = 2; } }   // Ack cumulative; Cancel{reason, grace_ms}
```

- **Endpoint:** UDS at `<state_dir>/control.sock` (0600), stable across restarts, bound only after restore; replaces `127.0.0.1:0` (`control_server.rs:155`). Peer death on UDS = immediate EOF; the 30 s heartbeat thread (`runtime.rs:88-111`) is removed. TCP listener deferred until a remote executor exists.
- **Lifecycle:** Hello within 5 s, routed via `run_index` + `plan.node_ix`. Stream drop → `grace_ms = now + 10 s` → Lost. Operator reconnect: full-jitter 50 ms → 2 s, up to `orphan_timeout` (1 h), then kill child and exit.
- **Ack mode:** `Durable` when a state dir is configured (ack only once the snapshot containing the apply is written, so a crash can't lose a success); `Applied` for ephemeral use (`add`, tests); falls back to Applied with a `SnapshotUnhealthy` event if snapshots fail for >30 s. Each pending ack owns its sender + nonce.
- **Cancel on the operator:** SIGTERM to the child's process group → SIGKILL after `grace_ms` → report `Failed{CANCELLED|TIMED_OUT}`. Grandchild gets `setsid` + `PR_SET_PDEATHSIG(SIGKILL)` (spawned from a long-lived thread).
- **Passive reconcile after restart:** (1) restore, `boot += 1` durably; (2) Dispatched/Running → Unknown with a grace; other non-terminal rows of restored running runs → HELD under one RestoreHold timer; (3) bind endpoint (earlier attempts got ECONNREFUSED and backed off); (4) each Hello: unknown/retired run → `Cancel{UNKNOWN_TASK}`; a restored row receiving a higher prior-boot attempt adopts it and supersedes lower ones (any row state); same attempt binds and its resent terminal resolves it; (5) retry decisions on restored rows stay HELD during the hold; (6) hold expiry: Unknown → Lost → retry/fail; HELD cleared, Ready enqueued. Manual triggers accepted after the last snapshot are lost (bounded by snapshot cadence; documented).

```rust
pub struct IdempotencyKey { pub run_id: [u8; 16], pub node_ix: u16, pub attempt: AttemptId }
pub enum DispatchRejected { Saturated(DispatchRequest), UnknownOperator(&'static str) }
pub enum ExecSignal { SpawnFailed { key: IdempotencyKey, error: Box<str> },
                      Exited { key: IdempotencyKey, code: Option<i32>, signal: Option<i32> } }
pub trait Executor: Send + Sync + 'static {
    fn dispatch(&self, req: DispatchRequest) -> Result<(), DispatchRejected>; // never blocks
    fn cancel(&self, key: &IdempotencyKey, mode: KillMode);                   // best-effort
}
```
LocalExecutor v2: spawner task over a 4096-slot channel, keeps the fd semaphore; a reaper per child sends `Exited` and frees the permit.

- **D2 materialization:** reject absolute/`..` paths → `$TINYDAG_CAS_DIR` lookup → `FetchBlob` on miss (hash while writing, rename) → mismatch/NOT_FOUND = `INVALID_CONFIG` → copy into `work_dir/rel_path` and run there (replaces absolute reads at `bash.rs:225-231`, `python.rs:122-128`).

### Snapshot and restore

**Phase A (in-process):**
- At the closed-tick boundary when `every` (1 s) elapsed or the oldest pending durable ack is >250 ms old; never more than once per 250 ms; final snapshot on drain. Above a 64 MB region, `every` scales to keep disk writes ≤64 MB/s.
- The loop copies `[0..hwm)` of each table + chunk 0 into a pre-faulted staging buffer (est. 0.5 ms at 10k, 5 ms at 100k, 40 ms at the 1M herd; a release bench measures it) and hands it to the `tinydag-snap` thread over `sync_channel(1)`.
- Writer: fsync referenced output files → write `snap.<gen>.tmp` (512 B header: magic, format, `layout_version`, `layout_hash`, `boot`, `gen`, `seq`, `wall_ms`, per-table `{file_off, hwm, row_size, xxh3}`, `header_xxh3`; then table prefixes) → fsync, rename, fsync dir; keep 3 generations → send `Durable{seq}` (loop releases pending acks). Failures return the buffer, mark unhealthy, back off; **the loop never blocks**.
- **No replayed shadow copy**: copying bytes cannot diverge from the live state; a reducer-fed mirror can. This replaces the earlier panel's shadow + Resync machinery.

**Restore:** newest file whose magic + header hash + table hashes check (layout mismatch = hard error naming both; other failures fall back a generation) → boot counter → map region, copy prefixes, one O(n) semantic pass (enum ranges, indices `< hwm`, generation refs, counters recomputed), bump free-slot gens → rebuild cold indexes (Plans eager only for active runs), wheel from `next_due_ms/deadline_ms/grace_ms`, ring from Ready rows in `ready_seq` order → passive reconcile → cron catch-up (≤60 s late fires; older slots reported as one `CronMissed{first,last,count}` unless opted in; clock going backwards fires nothing early) → bind endpoint.

**Phase B (later, x86_64 only):** region on `memfd_create`, fd passed to a sidecar via SCM_RIGHTS; `shm.rs` (~40 audited lines) is the only `unsafe` in the design. Seqlock: loop writes `seq` odd at tick start, stamps chunk epochs on first touch, `seq` even at tick end (Release); sidecar copies chunks, later recopies only those with newer epochs, validates `seq` even + epochs ≤ copied + `seq` unchanged (~3 µs scan at 1M). Starvation: after 30 s of no gap, sidecar sets `reader_wants_gap`; loop grants ≤50 µs (≤5 ms once per snapshot). Cold data: in-process cold writer publishes `cold_durable_seq`; sidecar finalizes only images with `seq ≤ cold_durable_seq`. Scheduler dies mid-tick (odd `seq`, seen via pidfd): the sidecar reconstructs `s−1` from clean chunks it holds, else the last durable file stands — both correct because effects follow closed ticks and acks are durability-gated. x86_64-only because plain row stores after a Release fence are only ordered under TSO.

### Public API

```rust
let s = SchedulerBuilder::new(executor).clock(c).default_limits(Limits::default())
    .max_running_runs(n).state_dir(dir).snapshot_every(d).ack_mode(AckMode::Durable).build().await?;
s.register(dag) -> Result<RegisterOutcome, RegisterError>;   // DuplicateDagId | InvalidCron | TriggerCycle | Capacity
s.trigger(dag_id, opts) -> Result<RunHandle, TriggerError>;  // RunHandle { run_id, done: oneshot::Receiver<RunResult> }
s.cancel(run_id); s.emit_event(ty, payload); s.subscribe(opts) -> EventStream;
s.run_status(run_id); s.pipeline_status(dag_id); s.stats();
s.quiesce().await; s.shutdown(Shutdown::Drain { timeout } | Shutdown::Abort).await;
```
`RunHandle` registers the waiter in the admitting command, so a fast run can't finish and free its row before the caller waits (`batch.rs:179-186` relies on this). `tinydag serve` hosts one scheduler and drains on SIGINT/SIGTERM.

### New dependencies

- `zerocopy 0.8` (already in Cargo.lock at 0.8.48; becomes direct) — safe byte views of rows.
- `memmap2` — safe anonymous reservation (A) and memfd mapping (B).
- `xxhash-rust` (xxh3) — snapshot checksums at >30 GB/s.
- `croner` — 5-field cron with DST, validated at compile time.
- `hyper-util` + `tower` (already in lock) — UDS connector for the tonic 0.12 operator client.
- `proptest` (dev) — allocator and restore round-trip properties.
- tokio `signal` feature; `tempfile` → dev-only; `libc` already present.

### Phases (each lands with CI green: fmt, clippy `-D warnings`, build, test)

| # | Phase | Scope | Verification | Status |
|---|---|---|---|---|
| 0a | Refactors, no behaviour change | `Arc<DagDef>` (`scheduler.rs:46`); `DagDef: Deserialize` (`dag.rs:79`); `Plan`/CSR used by today's runner (`runner.rs:117-135,159,163`); deterministic roots; `MockExecutor`; tokio `signal`; `tempfile` dev-only | existing tests pass; serde round-trip keeps `version_hash`; dispatch-order determinism test | Done |
| 0b | Defect fixes on today's unary protocol, one commit + test each | defects 1 (SIGTERM→grace→SIGKILL, PDEATHSIG), 2 (`select!` on `child.wait()`), 4 (keep error_type/message), 5 (drop high-cardinality metric attrs), 6 (`JoinSet`), 7 (cancel siblings on failure), 11 (fan-in rule), 12 (duplicate `dag_id` error), 14 (content-keyed validation caches), 17 (log unknown keys). Defect 15 (exit codes) as its own commit | no process with the task's pgid survives a `sleep 3600` timeout; operator `kill -9` reported in <100 ms; failure with a long sibling finishes promptly | Done |
| 1 | Region | `src/scheduler/region.rs`: header, chunk 0, rows, accessor chokepoint, free lists, size classes, generations | const asserts; pinned `layout_hash`; proptest random alloc/free (live `(idx,gen)` resolve, freed fail); debug test that every `_mut` marks its chunk | Next |
| 2 | Core loop, manual runs | `core.rs, driver.rs, plan.rs, clock.rs, events.rs, api.rs`; admission/Limits, ring/pump, fail-fast, retries (+ defect 13), outputs, events, `RunHandle`/`RunResult` (defect 8). Runs over a thin adapter around today's `LocalExecutor::dispatch`; `runner::run` and `Scheduler::trigger` stay as wrappers; `batch.rs` ported | ported runner/scheduler/batch tests; MockExecutor + TestClock: diamond, fail-fast with long sibling, retry, duplicate terminal, Overlap error |  |
| 3 | Session v2 + Executor v2 + UDS + cancel | proto v2, `control_server.rs` → `session.rs`, `executor.rs`, `operators/{runtime,bash,python}.rs`; adapter retired (defect 3) | in-process gRPC tests of seq/ack/resend; Cancel → `Failed{CANCELLED}` with no orphan; `Exited` <100 ms; 10k-session burst bench (loop µs, memory per connection) |  |
| 4 | D2 scripts | `tinydag.toml` root, `TaskRef {rel_path, sha256}` (`bash.rs:39,101`, `python.rs:30`), CAS, `FetchBlob`, materialization | two checkouts → one hash; tampered blob → `INVALID_CONFIG`; empty cache fetches once then hits |  |
| 5 | Cron wheel + `serve` + drain | `timers.rs, cron.rs, main.rs` (defect 16) | TestClock: fire, miss, catch-up, stale-entry discard; `#[ignore]` bench: 1M entries due at once drain <0.5 s, no iteration >2 ms |  |
| 6 | Completion + event triggers | DSL (`compiler.rs:351-354`), `dependents` keyed `(idx,gen)`, cycle rejection | chained DAGs with concurrent upstream runs + Limits enforcement |  |
| 7 | Snapshot A | staging, writer thread, file format, output files, restore steps 1–4 | snapshot→restore byte equality (before Unknown rewrite); fault injection at each writer step; fuzz semantic pass with checksum bypassed never panics; memcpy bench |  |
| 8 | Durable acks + restore hold + catch-up | ack gating, HELD/RestoreHold, adoption, `CronMissed` | `kill -9 serve` mid-run then restart: no re-execution (marker files); post-snapshot dispatch adopted; grace → Lost → retry; 2 h outage → one `CronMissed`; Applied fallback when snapshots fail |  |
| 9 | (only if measured) dirty-chunk copy + delta files | | worst stall per boundary ≤0.5 ms at 1M |  |
| 10 | Phase B sidecar | memfd, seqlock, sidecar binary | cross-process stress with per-row redundant stamps; kill scheduler mid-tick → `s−1` image or last durable file |  |

Doc/TODO hygiene rides along: TODO.md stale items fixed in 0a; `design/03` drift and the broken `examples/README.md` link fixed in 0a; `design/06` scheduler section rewritten in phase 2; `design/08` gains pipeline-level completion semantics and event idempotency as open questions in phase 6.

### Risks and deferred items

- At 1M pipelines, cold DagDefs (~15 GB) need the resident-Plan + LRU step before they fit.
- Durable acks keep each finished operator alive up to one snapshot interval, consuming executor slots (≈1 s normally, ≈6 s at 1M until delta files).
- `PR_SET_PDEATHSIG` fires when the forking *thread* exits; the operator must spawn the script from a long-lived thread.
- Large anonymous reservations can fail under strict overcommit; reservations come from configured maxima.
- Phase B is x86_64-only until writer stores become relaxed atomics.
- Manual triggers accepted after the last snapshot are lost on crash (bounded by cadence; documented).
- **Deferred:** warm standby, TCP listener + remote executors, multi-file script bundles, `FailurePolicy::Continue`, pipeline-level completion semantics, structural (dag_id-independent) Plan dedupe.

## Verification (whole plan)

- Every phase: `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo build --all-targets`, `cargo test --all-targets` — on each commit individually, by checking it out into a scratch `git worktree` and running all four there.
- Deterministic scheduler tests run on `TestClock` + `MockExecutor` + `quiesce()`, no sleeps.
- Performance claims are `#[ignore]` release benchmarks run on the owner's box (decision latency at 10k/1M, herd drain, memcpy cost, session burst), with `cmd_latency`/`timer_lag`/`snapshot_*` metrics exported from phase 2 on.
- End-to-end after phase 8: `tinydag serve examples/` with a cron pipeline; `kill -9` mid-run; restart; the run resumes and completes without re-executing finished tasks (marker files), and a missed window produces one `CronMissed`.
