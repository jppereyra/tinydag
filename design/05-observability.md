# Observability

## Control layer: tinydag needs to know

tinydag needs the minimum information required to manage a pipeline run; this
is communicated directly between the operator and tinydag via the operator
protocol:

- Task started
- Task succeeded (with exit code)
- Task failed (with exit code and structured error)
- Task heartbeat (proof of life at a regular interval)

tinydag uses this information to advance the pipeline, handle failures,
and detect stuck tasks. It does not interpret anything beyond these
four signals.

The heartbeat is what allows tinydag to distinguish between a task that is
running slowly and a task whose process died silently. A missed heartbeat
triggers a timeout event.

## Data layer: humans (or their agents) need to know

Everything else flows through OpenTelemetry to whatever backend the user
or operator of the tinydag deployment has configured.
tinydag does not intermediate this layer.

Examples of what belongs here:

- How long a task has been running
- Whether it is consuming unusual resources
- Whether it is slow compared to previous runs
- Progress events emitted by the task itself
- Logs, traces, and custom metrics from the operator

A task that is taking unusually long but is still sending heartbeats is
"running" as far as tinydag is concerned. Whether that is normal or a problem
is a question for the human or agent looking at the observability backend.

---

## What tinydag emits

tinydag itself emits structured OTel events for pipeline-level and
task-level state transitions. These flow to the same backend as operator
telemetry. Every event carries:

- Run ID
- Pipeline ID
- DAG version
- Team / user
- Task ID
- Trigger type

Task-level telemetry is the primary unit. Pipeline-level status is a derived
view, not the source of truth.

Every meaningful state transition is emitted as a structured event:

- Task dispatched
- Task started (with execution context)
- Task progress (optional hook)
- Task succeeded (with outputs, duration)
- Task failed (with structured error: machine-readable type, message, context)
- Task skipped (with reason)
- Task timed out
- Task retried (with attempt number and backoff)

---

## Multi-tenancy

Team and user are first-class fields in the IR and in every telemetry event.
Retrofitting tenancy is painful.

---

## No built-in UI

tinydag ships no UI. Users bring their own observability tooling.

---

## The operator SDK

Operator authors should not need to know which layer their telemetry belongs
to. The operator SDK abstracts this: calling `ctx.emit_event(...)` routes
control-layer signals to tinydag and data-layer signals to OTel, depending
on the event type and the execution environment.

The operator protocol uses gRPC. tinydag exposes a control service that
operators connect to via an endpoint passed as an environment variable. The
proto definition is small -- four RPCs:

```protobuf
service OperatorControl {
  rpc ReportStarted(StartedEvent) returns (Ack);
  rpc ReportHeartbeat(HeartbeatEvent) returns (Ack);
  rpc ReportSucceeded(SucceededEvent) returns (Ack);
  rpc ReportFailed(FailedEvent) returns (Ack);
}
```
Operators are responsible for their own validation. Before doing any real
work, an operator runs its own checks (connectivity, permissions, data
availability for external inputs). If validation fails, the operator calls
`ReportFailed` with a structured reason and exits. tinydag sees a fast
structured failure and stops dependent tasks.

This means the operator execution lifecycle is:
Dispatch → operator starts → self-validate → ReportStarted
→ execute → ReportSucceeded or ReportFailed

If self-validation fails, `ReportStarted` is never called. This is still
dramatically faster than discovering the problem mid-pipeline: for example a
`PrestoOperator` that can't reach its cluster or finds a missing external
partition fails in the first seconds of execution with a clear structured
error, before any dependent tasks are dispatched.

Inputs produced by preceding tasks in the same pipeline are not validated by
the operator as the DAG structure already encodes that dependency. tinydag will
not dispatch the operator until its predecessors have succeeded.

Each message carries at minimum a run ID and task ID. `FailedEvent` carries
a structured error. `SucceededEvent` carries task outputs. `HeartbeatEvent`
carries a timestamp.

This proto definition is the contract every operator in every language must
implement. It will be treated as a formal RFC before it is finalized.

In v1 with the local executor, the gRPC server runs on a local port or unix
socket. In v2 with remote execution backends, the endpoint is a reachable
service -- on Kubernetes, a pod-accessible service; the operator code does
not change between environments.

> **Note:** In environments where the operator cannot reach the tinydag gRPC
> endpoint (air-gapped networks, locked-down VPCs, isolated GPU clusters),
> the execution backend adapter serves as the control layer instead. Each
> backend knows how to extract job status from its own completion signals:
> pod exit codes on Kubernetes, response payloads on Lambda, etc. This is a
> planned design direction but is not yet specified.