# tinydag — TODO

## Checklist to v0.1

### gRPC control protocol

- [ ] Define `.proto` for `OperatorControl` service:
      `ReportStarted`, `ReportHeartbeat`, `ReportSucceeded`, `ReportFailed`
- [ ] Implement gRPC server in tinydag
- [ ] Pass gRPC endpoint to operator via environment variable
- [ ] Update `tinydag-op-bash` to use gRPC protocol
- [ ] Implement heartbeat timeout detection in the runner

### CLI

- [x] `tinydag run <dag.json>`: reads JSON IR, validates, and executes with
      the local executor. Prints run ID and task count on success.
- [ ] `tinydag compile <pipeline.star>`: Starlark parser, validate, emit JSON IR

### Example pipeline

- [ ] Write a realistic two or three task example pipeline in `examples/`
      (extract, transform, load shape using BashOperator and PythonOperator).
      This replaces sample.json.
- [ ] Include a `README` in `examples/` with step-by-step instructions to run it

### Polish

- [ ] Structured error output in CLI: errors should be human-readable and
      machine-parseable (JSON flag)
- [ ] `tinydag run` exit codes: 0 success, 1 validation error, 2 task failed,
      3 internal error

## Beyond v0.1

### IR

- [ ] Add inter-pipeline dependency declarations to IR (v1: store, don't enforce)
- [ ] Implement IR diffing (compare two versions of a `DagDef`, surface structural changes)
- [ ] Implement IR versioning / audit trail (persist versions, query by hash)

**TBD**

- [ ] Migrate IR from serde/JSON to Protobuf via `prost` + `prost-build`
- [ ] Define `.proto` schema for all IR types (`DagDef`, `Node`, `Edge`, `Port`, `TaskRef`, `RetryPolicy`)

### Compile-time Validation

- [x] Duplicate node ID detection
- [x] Unknown node reference in edges
- [x] Cycle detection (Kahn's algorithm)
- [x] Topological sort
- [x] Missing task reference checks (callable/script/container exists and is reachable)
- [x] Disconnected node detection (nodes with no edges in a multi-node DAG)
- [ ] Contract checking (output type of node A matches declared input type of node B)
- [ ] Parameter validation — required params present and correct type
- [ ] Python DSL scope violation checks — imports, side effects, system calls (Starlark-style)
- [ ] Compile-time warning when pipeline has neither preconditions declared nor `none()`
- [ ] Operator parameter schema validation: validate operator invocations
      against declared schemas at compile time. Schema format TBD.

### Pre-execution validation

- [ ] Pre-execution validation pass interface
- [ ] Resolver callable reference validated at compile time (same rules as task callables)
- [ ] Hook interface for custom preconditions

### Scheduler

- [ ] Core scheduler loop — operates on frozen, validated IR only
- [ ] Trigger: `manual`
- [ ] Trigger: `cron`
- [ ] Trigger: `event`
- [ ] Trigger: `pipeline_completion` (wakes dependents on success, notifies on failure)
- [ ] Skip semantics on nodes (static solution for conditional branching)
- [ ] Parameter injection at submission time (static solution for parameterized pipelines)

**TBD** 

- [ ] Resolver: pipeline-level Python function called once at dispatch time,
      before any task runs, to resolve late-bound inputs (static solution for
      fan-out over unknown inputs)
- [ ] Fan-out / fan-in scheduling (parallel branches, convergence)

### Execution / Dispatch

- [x] Define dispatch interface (trait)
- [x] Dispatch payload generation from IR node
- [ ] Kubernetes jobs backend
- [ ] AWS Lambda backend
- [x] Local subprocess backend (for development / testing)
- [x] State machine per task: `pending → dispatched → running → succeeded | failed | timed_out`
- [x] Timeout enforcement
- [ ] Retry logic (max attempts + backoff from IR, no dynamic retry logic)
- [ ] Partial execution state snapshot on failure (machine-readable, what succeeded / failed / waiting)
- [ ] Resume from failure point (v1: user-initiated)

### CLI

- [ ] `tinydag run <dag.json>` — full implementation with scheduler integration
- [ ] `tinydag status <run_id>` — show run state (needs scheduler)
- [ ] `tinydag diff <dag_v1> <dag_v2>` — IR diff
- [ ] `tinydag register <pipeline.star>`: compile + register with scheduler
