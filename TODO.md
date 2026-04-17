# tinydag — TODO

## Checklist to v0.1

### gRPC control protocol

- [x] Define `.proto` for `OperatorControl` service:
      `ReportStarted`, `ReportHeartbeat`, `ReportSucceeded`, `ReportFailed`
- [x] Implement gRPC server in tinydag
- [x] Pass gRPC endpoint to operator via environment variable
- [x] Update `tinydag-op-bash` to use gRPC protocol
- [x] Implement heartbeat timeout detection in the runner

### CLI

~~- [x] `tinydag run <pipeline.star>`: compiles, validates and runs the pipeline~~
- [x] `tinydag compile <pipeline.star>`: Starlark parser, validate, emit DAG JSON
- [x] `tinydag add <pipeline.star> [--run-now]`: compiles, validates and registers the pipeline; --run-now triggers an immediate run


### Example pipeline

- [x] Write a realistic two or three task example pipeline in `examples/`
      (extract, transform, load shape using BashOperator and PythonOperator).
- [ ] Include a `README` in `examples/` with step-by-step instructions to run it

### Polish

- [ ] Structured error output in CLI: errors should be human-readable and
      machine-parseable
- [ ] `tinydag add` exit codes: 0 success, 1 compile/validation error, 2 task failed,
      3 internal error

---

## Stuff that will bite us later or is just plain ugly

- [ ] `NodeId`, `dag_id`, `pipeline_id`, `run_id`, `version_hash`
      are all plain `String`. You can pass a `run_id` where a `dag_id` is expected and the
      compiler won't catch it.
- [ ] `compute_version_hash` hashes node IDs, edges,
      and params but not the `TaskRef` content (cmd strings, script paths).
- [ ] Any stray `print()` from a script or imported library corrupts the output JSON.
      What if we write outputs `tinydag_outputs.json` in the work dir as we do with `tinydag_inputs.json`.
- [ ] `dispatch`, `run`, and `validate` all call `global::meter(...).f64_histogram(...)`
      on every invocation. OTel deduplicates internally but we're still doing string allocations and map lookups on every task.
- [ ] `compile_source` takes `base_dir: Option<&Path>` which lets callers silently skip
      path resolution by passing `None`.

## Nits

- [ ] The `run_operator` closure returns `Result<String, (FailedErrorType, String, i32)>`
      which appears in a lot of places. Create the `OperatorFailure { error_type, message, exit_code }` struct.
- [ ] Bundle `run_id`, `dag_id`,`pipeline_id`, `dag_version`, `team`, `user`, `trigger_type`
      into a `RunContext` struct.
- [ ] **`unsafe` blocks missing `// SAFETY:` annotations**
- [ ] Add `#[must_use]` to `compute_version_hash`, `validate`, and `compile`.
- [ ] Add `clap` before the CLI grows more commands.
- [ ] `run_operator` does too many things.
- [ ] `validation.rs` and `runner.rs` both independently build a graph.
- [ ] Replace `SYNTAX_CHECK_SEQ` `AtomicU64` counters in tests with `tempfile::NamedTempFile`.

---

## Beyond v0.1

### DAG Schema

- [ ] Add inter-pipeline dependency declarations to the DAG definition
- [ ] Implement DAG diffing (compare two versions of a `DagDef`, surface structural changes)
- [ ] Implement DAG versioning / audit trail (persist versions, query by hash)
- [ ] Skip semantics on nodes
**TBD**

- [ ] Resolver: pipeline-level function called once at dispatch time, before any task runs,
      to resolve late-bound inputs (static solution for fan-out over unknown inputs)

### External Systems API (TBD)

- [ ] Implement a way for external systems to create and submit DAGs to tinydag
- [ ] Migrate DAG from serde/JSON to Protobuf. Define `.proto` schema for all DAG types

### Compile-time Validation

- [x] Duplicate node ID detection
- [x] Unknown node reference in edges
- [x] Cycle detection (Kahn's algorithm)
- [x] Topological sort
- [x] Missing task reference checks (script/cmd exists and is syntactically valid)
- [x] Disconnected node detection (nodes with no edges in a multi-node DAG)
- [ ] Contract checking (output type of node A matches declared input type of node B)
- [ ] Parameter validation — required params present and correct type
- [ ] Compile-time warning when pipeline has neither preconditions declared nor `none()`
- [ ] Operator parameter schema validation against declared schemas at compile time

### Pre-execution Validation

- [ ] Pre-execution validation pass interface
- [ ] Hook interface for custom preconditions

### Scheduler

- [ ] Core scheduler loop
- [ ] Trigger: `cron`
- [ ] Trigger: `event`
- [ ] Trigger: `pipeline_completion` (wakes dependents on success, notifies on failure)
- [ ] Parameter injection at submission time
- [ ] Fan-out / fan-in scheduling (parallel branches, convergence)

### Execution / Dispatch

- [ ] Kubernetes jobs backend
- [ ] AWS Lambda backend
- [ ] Retry logic (max attempts + backoff from the DAG, no dynamic retry logic)
- [ ] Partial execution state snapshot on failure
- [ ] Resume from failure point (v1: user-initiated)
- [ ] Add `ReportSkipped` RPC to `OperatorControl` service when skip semantics land
- [ ] Structured path for passing large artifacts (model
      checkpoints, datasets) between tasks without relying on env vars or stdout

### CLI

- [ ] `tinydag status <run_id>` — show run state (needs scheduler)
- [ ] `tinydag diff <dag_v1> <dag_v2>` — DAG diff