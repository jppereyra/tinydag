## v1 Scope

### In

- Starlark pipeline DSL with `PythonOperator`
- Plain Python task modules with no tinydag imports
- Parser that compiles to Protobuf IR
- Structural validation at compile time (cycles, missing references, broken dependencies)
- Callable validation at compile time (strict mode by default)
- Explicit precondition functions required on every pipeline
- Pipeline-level resolver for late-bound inputs
- CI/CD as the deployment gate (`tinydag compile`, `tinydag register`)
- Local executor: runs tasks as subprocesses on the same machine, no remote dispatch
- Cron and manual triggers only
- Task-level telemetry via OpenTelemetry: structured events, structured errors,
  all metadata fields: run ID, pipeline ID, DAG version, team/user, task ID,
  trigger type
- `tinydag.testing` module shipped as part of core
- Single tenant

### Part of the design but out for v1

- Remote execution backends (k8s, Lambda)
- Event and pipeline_completion triggers
- Contract validation at edges (inputs/outputs)
- Pre-execution data availability checks beyond precondition functions
- Inter-pipeline dependencies
- Multi-tenancy enforcement
- Airflow migration tooling
- Retries
- Partial execution and resume
- Backfills
- Container-based and manifest-based callable validation modes
- `tinydag-preconditions` package

## Testing Strategy

> **Note:** This section is a placeholder and will be expanded later.

**Realistic workloads:** try to simulate ML pipeline shapes including fan-out
preprocessing, convergent training steps, and conditional branches.

**Fault injection:** test panicking tasks, stalling tasks, dependency cycles,
and partial graph completion.

**Property-based testing:** use `proptest` or `quickcheck` to generate random
valid DAG topologies and verify ordering invariants.

**Benchmarking:** compare against other DAG platforms on a 100-node DAG across
three dimensions: cold-start latency, memory overhead, and throughput.

Other ideas? TBD

## Contributing

This project is in early design. The best way to contribute right now is to open
an issue with feedback on the design decisions above. As the project matures,
major changes will go through a formal RFC process.