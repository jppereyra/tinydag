# tinydag

A tiny DAG orchestrator written in Rust.

Runs *inside* a larger system rather than *being* the system. Owns the graph, the ordering, the state, and the dispatch, but not the compute. Most pipeline tools (Airflow, Dagster, Prefect) assume they are the center of your infrastructure. tinydag does not.

---

## How it works

```
Python DSL -> Parser -> DagDef -> Orchestrator (Rust)
                              ^
External systems  ------------|
```

You define pipelines in Python. tinydag compiles them to a DAG definition and the orchestrator never sees raw Python. External systems can submit a DAG definition directly, bypassing the DSL entirely.

```python
# pipeline.star
pipeline = DAG("my_pipeline", schedule="0 * * * *")

with pipeline:
    raw = PythonOperator(
        task_id="extract",
        python_callable="mymodule.extract",
        inputs=[],
        outputs=["raw_data"]
    )
```

---

## Design principles

**Fail fast at every layer.** The primary value of pipeline compilation is moving runtime failures to compile time.

- *Compile time:* cycles, broken dependencies, contract mismatches, missing task references
- *Pre-execution:* data availability, SLA windows, user-declared preconditions
- *Early execution:* logic errors and external failures that are unavoidable but are surfaced early and clearly

**Static DAGs over dynamic ones.** Almost every dynamic DAG is convertible to a static one. Parameterized pipelines, fan-out, and conditional branching all have static solutions. For the rest, there is an explicit `unsafe` mode.

**Dispatch and telemetry, not compute.** tinydag dispatches tasks to execution backends (local subprocess, Kubernetes, Lambda) and receives telemetry back. It does not run your code.

**OpenTelemetry out of the box, no built-in UI.** Every event carries pipeline ID, DAG version, team/user, task ID, and trigger type. Multi-tenant observability is included from day one.

---

## Status

Early design phase. No running code yet. See [DESIGN.md](DESIGN.md) for full design rationale and open questions.

## Contributing

The project is in early design. Code contributions are not yet open, but
design feedback is very welcome. The best way to contribute right now is to open an issue with thoughts on the design decisions in [DESIGN.md](DESIGN.md).

As the project matures, major changes will go through a formal RFC process.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
