# Open Design Questions

This document captures decisions that are known to be unresolved, deferred, or in
tension with each other. The goal is to make the tradeoffs explicit rather than
leaving them implicit in code comments or TODOs.

---

## 1. Open vs Closed Operator Set

**Resolved for now:** `TaskRef` is a closed enum living in `src/operators/mod.rs`.
Adding an operator means adding a variant to the enum, a file in `src/operators/`,
and one line to `all_operator_globals()`. The engine (`compiler.rs`, `dag.rs`,
`executor.rs`) never imports concrete operator types directly.

The operator code is cleanly isolated in `src/operators/` and designed to be
extracted into a separate crate if it becomes necessary.

---

## 2. Output Protocol: Stdout vs File

**Resolved:** Outputs are written to `tinydag_outputs.json` in the work directory,
symmetric with `tinydag_inputs.json`. Stdout is a pure logging channel.



---

## 3. Strategic Direction

**Direction A: Embedded ML/AI infrastructure layer**
Priority: artifact passing for large files, fan-out for hyperparameter sweeps,
GPU-aware scheduling, Kubernetes execution, checkpoint-aware retries.
Target user: ML platform teams embedding orchestration into training infrastructure.

**Direction B: Lightweight DAG orchestration for small teams**
Priority: cron scheduling, run history and persistence, a minimal web UI,
resume-from-failure, a richer DSL.
Target user: data engineering teams that find Airflow too heavy.

**Direction C: Agent orchestration**
LLM-powered agents increasingly execute multi-step plans with dependencies, error
handling, and observability. This is structurally a DAG problem. tinydag's
embedded-first positioning may map well to agent runtimes. What would need to
change:
- Dynamic fan-out: the number of branches isn't known at compile time (e.g. one branch
  per retrieved document)
- Streaming outputs: agents need partial results, not just final outputs
- Sub-second dispatch latency: agent steps are often milliseconds, not minutes
- Human-in-the-loop semantics: pause a run, wait for human approval, resume

**Current state:** While some of the features are obvious needs (cron scheduling, etc.)
there is no explicit commitment to a direction at this time. I want it all, though.
Need-think-more-hard.

---

## 4. Data-Passing Model for Large Artifacts

**Current model:** Outputs are flat JSON values passed as environment variables
(`TINYDAG_INPUT_<KEY>`). This works for scalar values and small strings. It breaks
for large artifacts: model checkpoints, datasets, feature matrices, embeddings.

**The gap:** There is no first-class concept of an artifact in tinydag. Tasks that
produce large files must coordinate through implicit conventions (write to a known
path, pass the path as an output).

**Options:**
- Formalize the convention: add an `artifact_dir` field to the dispatch payload, a
  reserved scratch space per DAG run that all tasks can read and write
- Introduce an artifact registry abstraction with pluggable backends (local filesystem,
  S3, GCS)
- Keep it out of scope

This is a hard requirement for ML pipelines and not necessarily needed for simple ETL.
