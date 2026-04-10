# tinydag — TODO

## IR

- [ ] Migrate IR from serde/JSON to Protobuf via `prost` + `prost-build`
- [ ] Define `.proto` schema for all IR types (`DagDef`, `Node`, `Edge`, `Port`, `TaskRef`, `RetryPolicy`)
- [x] Add `version_hash` to `DagDef` — baked in at definition time, new version on any change
- [x] Add `trigger` as first-class IR field (`cron | event | manual | pipeline_completion`)
- [x] Add `team` and `user` as first-class IR fields (multi-tenancy from day one)
- [x] Add `pipeline_id` as first-class IR field (distinct from `dag_id`)
- [ ] Add inter-pipeline dependency declarations to IR (v1: store, don't enforce)
- [ ] Implement IR diffing (compare two versions of a `DagDef`, surface structural changes)
- [ ] Implement IR versioning / audit trail (persist versions, query by hash)

## Validation (compile-time)

- [x] Duplicate node ID detection
- [x] Unknown node reference in edges
- [x] Cycle detection (Kahn's algorithm)
- [x] Topological sort
- [ ] Contract checking — output type of node A matches declared input type of node B
- [x] Missing task reference checks — callable/script/container exists and is reachable (structural: non-empty fields, valid dotted identifier for Python callables; import-level check is Python-side)
- [ ] Parameter validation — required params present and correct type
- [x] Disconnected node detection (nodes with no edges in a multi-node DAG)
- [ ] Python DSL scope violation checks — imports, side effects, system calls (Starlark-style)

## Pre-execution validation (v1)

- [ ] Pre-execution validation pass interface — runs after compile, before any dispatch
- [ ] Precondition declaration in IR — users declare explicitly, tinydag does not infer
- [ ] `preconditions=none()` explicit opt-out in IR — distinguishes intentional absence from oversight
- [ ] Compile-time warning when pipeline has neither preconditions declared nor `none()`
- [ ] Resolver callable reference validated at compile time (same rules as task callables)
- [ ] Built-in precondition: partition exists check
- [ ] Built-in precondition: SLA window check
- [ ] If any precondition fails: abort pipeline, emit failure event immediately
- [ ] Hook interface for custom preconditions

## Scheduler

- [ ] Core scheduler loop — operates on frozen, validated IR only
- [ ] Trigger: `manual`
- [ ] Trigger: `cron`
- [ ] Trigger: `event`
- [ ] Trigger: `pipeline_completion` (wakes dependents on success, notifies on failure)
- [ ] Skip semantics on nodes (static solution for conditional branching)
- [ ] Parameter injection at submission time (static solution for parameterized pipelines)
- [ ] Pre-execution DAG expansion step (static solution for fan-out over unknown inputs)
- [ ] Fan-out / fan-in scheduling (parallel branches, convergence)

## Execution / Dispatch

- [ ] Define dispatch interface (trait) — backend-agnostic
- [ ] Dispatch payload generation from IR node
- [ ] Kubernetes jobs backend
- [ ] AWS Lambda backend
- [ ] Local subprocess backend (for development / testing)
- [ ] Telemetry intake interface — tasks report status, progress, output back to tinydag
- [ ] State machine per task: `pending → dispatched → running → succeeded | failed | timed_out`
- [ ] Timeout enforcement
- [ ] Retry logic (max attempts + backoff from IR, no dynamic retry logic)
- [ ] Partial execution state snapshot on failure (machine-readable, what succeeded / failed / waiting)
- [ ] Resume from failure point (v1: user-initiated)

## Observability

- [ ] OpenTelemetry integration — traces and events
- [ ] Consistent event metadata on every emission: pipeline ID, DAG version, team/user, task ID, trigger type
- [ ] Span per task dispatch
- [ ] Span per DAG run
- [ ] Failure event with full state snapshot
- [ ] Structured logging (no ad-hoc println)

## Python DSL

- [ ] `@task` decorator — resolves to IR node at parse time, never at execution time
- [ ] `inputs` / `outputs` decorator arguments → `Port` list in IR
- [ ] `@dag` decorator — assembles `DagDef`
- [ ] Dependency declaration syntax (`>>` operator or explicit `depends_on`)
- [ ] Trigger declaration in DSL
- [ ] Precondition declaration in DSL: string reference list or `none()` explicit opt-out
- [ ] Resolver declaration in DSL: `resolver="module.fn"` on `DAG`, `late("key")` on inputs
- [ ] Sandboxed parser (Starlark-style restrictions: no imports, no side effects, no system calls)
- [ ] DSL emits IR as Protobuf (or JSON for debugging)
- [ ] `tinydag.testing` module: `compile()` function + DAG assertion helpers (`depends_on`, `task`, `schedule`, etc.) — v1 core, not optional
- [ ] Python package: `tinydag` on PyPI

## Airflow Migration Tooling

- [ ] Airflow DAG file parser — extract graph topology and task references
- [ ] Structural import: Airflow DAG → tinydag IR
- [ ] Flag what needs manual attention (operators that have no tinydag equivalent)
- [ ] CLI command: `tinydag migrate airflow <dag_file.py>`
- [ ] Migration report: what was imported automatically vs. what needs manual work

## Testing

- [ ] Property-based tests for DAG topology (`proptest` or `quickcheck`) — generate random valid DAGs, verify ordering invariants
- [ ] Fault injection: panicking tasks
- [ ] Fault injection: stalling tasks / timeout enforcement
- [ ] Fault injection: partial graph completion + resume
- [ ] Realistic ML pipeline shape tests (fan-out preprocessing, convergent training, conditional branches) using `tokio::time::sleep` + jitter
- [ ] Benchmark: 100-node DAG cold-start latency vs. Prefect / Luigi
- [ ] Benchmark: memory overhead
- [ ] Benchmark: throughput (tasks/sec dispatched)
- [ ] End-to-end test with local subprocess backend

## Multi-tenancy

- [ ] Enforce `team` / `user` fields present on all IR submissions
- [ ] Tenant isolation in dispatch (v2)
- [ ] Per-tenant observability filtering (filtering problem, not schema problem)

## Inter-pipeline Dependencies (v2)

- [ ] Registry / catalog of inter-pipeline dependencies
- [ ] `pipeline_completion` trigger wires pipelines together
- [ ] Immediate downstream notification on upstream failure or skip
- [ ] Data availability check hook (upstream partition has landed)

## CLI

- [ ] `tinydag compile <pipeline.star>` — parse Starlark DSL, validate, emit IR (Protobuf or JSON)
- [ ] `tinydag register <pipeline.star>` — compile + register IR with scheduler (CI/CD gate)
- [ ] `tinydag validate <dag.json|dag.pb>` — run compile-time validation, print errors
- [ ] `tinydag run <dag.json|dag.pb>` — submit for execution
- [ ] `tinydag status <run_id>` — show run state
- [ ] `tinydag diff <dag_v1> <dag_v2>` — IR diff
- [ ] `tinydag migrate airflow <dag_file.py>` — Airflow structural import

## Backfills (v2)

- [ ] Backfill as a first-class artifact: a named variant of a pipeline compiled through the same pipeline (parse → validate → IR → execute)
- [ ] IR carries date range + pointer to originating pipeline version
- [ ] User-specified pipeline modifications for a backfill: skip tasks, swap implementations, change parameters
- [ ] Data collision policy declared in the variant (what happens to existing data)
- [ ] Downstream pipelines notified on backfill run completion, same as any other run

## Infrastructure / Project

- [ ] Migrate IR to Protobuf (unblocks everything else)
- [ ] Set up CI (build + test on every push)
- [ ] Set up benchmarking CI job (track regressions)
- [ ] Crate published to crates.io as `tinydag`
- [ ] Python package published to PyPI as `tinydag` (or `tinydag-rs` if name is taken)
- [ ] README: lead with "Airflow exit ramp" positioning
- [ ] README: "why embed a DAG executor vs. call out to one?" framing
- [ ] Dogfooding: use tinydag to orchestrate something in the actual toolchain
