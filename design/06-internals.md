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

### Scheduler

tinydag ships with a high-performance embedded scheduler that can be swapped out.
If you have existing scheduling infrastructure, you can ignore it and trigger
tinydag externally via the API. The scheduler is not required, but it is fast
and has no external dependencies.

**Design philosophy:** most pipeline schedulers are slow because they treat
scheduling as a database problem. tinydag's scheduler runs fully in memory,
snapshotting to disk for resiliency. The working set of an active scheduler
comprises runs in flight, pending tasks, trigger timers and is small enough to
fit comfortably in memory even for large deployments. This means scheduling
decisions happen in microseconds, and the latency between "task A completes" and
"task B is dispatched" is essentially zero.

The goal is to run very large systems from a single box, with a warm standby for
failover instead of sharding and adding complexity. A well-designed in-memory
system can go much further than most teams expect before sharding becomes
necessary.



### Security Model

Delegated to the execution system for v1. tinydag trusts that what it dispatches
runs in an isolated environment. This is an explicit, documented boundary and
operators are responsible for execution isolation.
