## Positioning

There is no tool that is: embedded-first, Rust-native, dependency-light,
and designed to run inside a larger system rather than be the system.
tinydag fills that gap.

### Landscape

| Tool | Notes |
|---|---|
| Airflow | The dominant pipeline platform. Heavy, complex, dynamic-DAG-first. The baseline most users are coming from. |
| Dagster | Full platform, asset-centric, good local dev story. Too heavy. |
| Prefect | More Pythonic than Airflow, dynamic DAGs, hybrid execution. Still heavy. |
| Hatchet | General-purpose task orchestration platform. Go-based. Closest in spirit but diverging toward AI agent workloads. |
| Luigi | Older, minimal. Closest in philosophy. |
| Dagger.io | CI/CD pipelines as code, different domain. |

## Airflow Interoperability

Airflow is the most common pipeline system users may be migrating from.
Full Airflow compatibility is out of scope as it would require reimplementing
the Airflow runtime to support.

What is in scope is structural compatibility: parse an Airflow DAG file, extract
graph topology and task references, and import into tinydag IR.
This gives users a starting point for migration rather than a blank page.