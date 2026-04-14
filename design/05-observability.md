### Observability

**OpenTelemetry out of the box. No built-in UI.** Users bring their own observability
tooling (Grafana, Honeycomb, Datadog, etc.).

Task-level telemetry is the primary unit. Pipeline-level status is a derived view,
not the source of truth.

Every meaningful state transition is emitted as a structured event:

- Task dispatched
- Task started (with execution context)
- Task progress (optional hook)
- Task succeeded (with outputs, duration)
- Task failed (with structured error: machine-readable type, message, and context)
- Task skipped (with reason)
- Task timed out
- Task retried (with attempt number and backoff)

Every event carries: pipeline ID, DAG version, run ID, team/user, task ID,
trigger type.

### Multi-tenancy

Team and user are first-class fields in the IR and in every telemetry event.
Retrofitting tenancy is painful.