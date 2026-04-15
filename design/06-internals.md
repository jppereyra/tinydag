### IR Structure

The IR captures graph structure and execution contracts, not task logic. Task
logic is the user's responsibility.

Fields:

- Node ID
- Task reference: a tagged union where the operator type determines valid fields
  - `python`: callable reference, inputs, outputs
  - `bash`: command string
  - `sql`: connection reference, query
  - `s3`: source, destination
  - `http`: url, method, headers, body
  - `kubernetes`: image, command, resources
- Inputs / outputs (names + types)
- Execution metadata (retries, timeout, etc.)
- Trigger definition
- Owner (team / user)
- Inter-pipeline dependency declarations

Triggers are a first-class field in the IR. Every pipeline declares how it
is activated: `cron`, `event`, `manual`, or `pipeline_completion`.

With `pipeline_completion` a pipeline wakes dependents when it succeeds and
notifies them when it fails.

The trigger type is carried in every telemetry event, so it is always clear
whether a run was scheduled, manually triggered, or woken by an upstream
pipeline.

The IR is serialized as **Protobuf**. A version hash is baked into every IR so that
when a DAG definition changes, it is a new version. Runs in flight belong to the
version they started on.

### Execution Model

tinydag is a dispatch and telemetry system, not a compute system.
It owns the graph, the ordering, the state, and the dispatch. It does not run
tasks, it dispatches them to an execution backend and receives telemetry back.

- v1: local executor runs tasks as subprocesses on the same machine, for
development and small deployments
- v2+: remote backends (Kubernetes jobs, Lambda functions, any system that can
accept a dispatch payload and emit telemetry)
- tinydag is backend-agnostic, i.e. the IR carries enough metadata to generate
the dispatch payload for any target

### Runner

The runner owns the within-run execution loop for a single DAG run. It is
stateless across runs and has no knowledge of triggers, other pipelines, or
run history. Given a frozen, validated `DagDef` and an executor, it:

1. Computes the dispatch order
2. Tracks per-task state: pending, dispatched, succeeded, failed
3. Dispatches tasks in parallel as their dependencies are satisfied
4. Collects task outputs and threads them as inputs to downstream tasks
5. Returns a `RunOutcome` when the run completes or a `RunError` on failure

`run()` is an async function. Each task dispatch is a `tokio::task::spawn`
that awaits `executor.dispatch()`. The tokio runtime multiplexes all in-flight
tasks across its thread pool so no OS thread is blocked waiting for a task to
complete. This model should scale from a handful of tasks on a development
machine to millions of concurrent tasks dispatched to remote backends.

The scheduler creates runners; runners do not know about schedulers.

**Future direction: explicit dispatch queue**

The current model uses tokio's task scheduler as an implicit queue. A future
design may introduce an explicit bounded work queue between the scheduler and
the executor, giving system operators direct observability into dispatch queue
depth and enabling priority lanes for certain task types. This is a v2+ concern
and does not change the runner's external interface.

### Scheduler

The scheduler operates at the pipeline level (and across pipelines) and decides
when to start a run. It has no knowledge of what is inside a DAG.

Scheduler
├── tokio::task::spawn(run(pipeline_A_run_1))  ← runner instance 1
├── tokio::task::spawn(run(pipeline_B_run_1))  ← runner instance 2
└── tokio::task::spawn(run(pipeline_A_run_2))  ← runner instance 3

Each runner runs as an independent tokio task on the shared runtime and each
runner yields the thread while waiting for task completions.

**v0.1: manual trigger only.** The v0.1 scheduler is intentionally minimal.
It maintains a registry of compiled DAG definitions and accepts manual trigger
requests. When triggered, it spawns a runner task and tracks the run to
completion. No persistence, no cron, no inter-pipeline dependencies.

**v2+: full scheduler.** The production scheduler adds cron triggers, event
triggers, pipeline completion triggers, run history, concurrency limits, and
error recovery mechanisms. The current design thinking is to run
very large systems from a single box with a warm standby for failover, rather
than adding complexity by sharding.

**Heartbeat traffic saturation.** At scale, heartbeat traffic could saturate a
single control server. More thinking has to go into this, but not for now. From
the operator's perspective nothing should change, however: it connects to a
single `TINYDAG_CONTROL_ENDPOINT` and the proxy routing should be transparent.


### Security Model

Delegated to the execution system for v1. tinydag trusts that what it dispatches
runs in an isolated environment. This is an explicit, documented boundary and
operators are responsible for execution isolation.
