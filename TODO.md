# tinydag — TODO

## v0.1.1

### Tech Debt

#### Stuff that will bite us later or is just plain ugly

- [ ] `NodeId`, `dag_id`, `pipeline_id`, `run_id`, `version_hash`
      are all plain `String`. You can pass a `run_id` where a `dag_id` is expected and the
      compiler won't catch it.
- [ ] `compute_version_hash` hashes node IDs, edges,
      and params but not the `TaskRef` content (cmd strings, script paths).
- [ ] `dispatch`, `run`, and `validate` all call `global::meter(...).f64_histogram(...)`
      on every invocation. OTel deduplicates internally but we're still doing string allocations and map lookups on every task.
- [ ] `compile_source` takes `base_dir: Option<&Path>` which lets callers silently skip
      path resolution by passing `None`.

#### Nits

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
- [ ] Structured error output in CLI: errors should be human-readable and
      machine-parseable
- [ ] `tinydag add` exit codes: 0 success, 1 compile/validation error, 2 task failed,
      3 internal error
---

## v0.2

### Scheduler

- [ ] Core scheduler loop
- [ ] Trigger: `cron`
- [ ] Trigger: `event`
- [ ] Trigger: `pipeline_completion` (wakes dependents on success, notifies on failure)
- [ ] Parameter injection at submission time
- [ ] Fan-out / fan-in scheduling (parallel branches, convergence)
- [ ] Inject standard execution context into `tinydag_params.json` for every run:
      `_run_id`, `_run_date`, `_run_datetime`, `_run_date_nodash`, `_run_year`,
      `_run_month`, `_run_day`, `_scheduled_at`, `_trigger_type`, `_dag_id`,
      `_pipeline_id`, `_dag_version`.

### CLI
- [ ] `tinydag status <run_id>` -- show run state
- [ ] `tinydag diff <dag_v1> <dag_v2>` -- DAG diff


### Compile-time Validation

- [ ] Contract checking (output type of node A matches declared input type of node B)
- [ ] Parameter validation — required params present and correct type
- [ ] Compile-time warning when pipeline has neither preconditions declared nor `none()`
- [ ] Operator parameter schema validation against declared schemas at compile time

### Pre-execution Validation

- [ ] Pre-execution validation pass interface
- [ ] Hook interface for custom preconditions

---

## v0.3

### Operator Library

Ship as a separate `tinydag-operators` package so community operators don't require
touching core.

- [ ] `SqlOperator` -- connects to a DB, runs a query, emits row count as output
- [ ] `HttpOperator` -- HTTP request, emits response body as output
- [ ] `S3Operator` -- upload / download / check existence
- [ ] `DbtOperator` -- runs dbt commands
- [ ] `SparkOperator` -- submits a Spark job and waits for completion
- [ ] `NotifyOperator` -- Slack / PagerDuty / email
- [ ] `PartitionSensorOperator` -- checks partition availability (no polling).

### Distribution

- [ ] `brew install tinydag`
- [ ] `.deb`, `.rpm`,  AUR package
- [ ] `cargo install tinydag`

### DAG Schema

- [ ] Add inter-pipeline dependency declarations to the DAG definition
- [ ] Implement DAG diffing (compare two versions of a `DagDef`, surface structural changes)
- [ ] Implement DAG versioning / audit trail (persist versions, query by hash)
- [ ] Skip semantics on nodes


- [ ] Resolver **TBD** -- pipeline-level function called once at dispatch time, before any task runs,
      to resolve late-bound inputs

### Execution / Dispatch

- [ ] Kubernetes jobs backend
- [ ] AWS Lambda backend
- [ ] Retry logic (max attempts + backoff from the DAG, no dynamic retry logic)
- [ ] Partial execution state snapshot on failure
- [ ] Resume from failure point (v1: user-initiated)
- [ ] Add `ReportSkipped` RPC to `OperatorControl` service when skip semantics land
- [ ] Structured path for passing large artifacts (model
      checkpoints, datasets) between tasks without relying on env vars or stdout

---

## v0.4+

### External Systems API (TBD)

- [ ] Implement a way for external systems to create and submit DAGs to tinydag
- [ ] Migrate DAG from serde/JSON to Protobuf. Define `.proto` schema for all DAG types