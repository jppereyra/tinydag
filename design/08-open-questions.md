# Open Design Questions

This document captures decisions that are known to be unresolved, deferred, or in
tension with each other. The goal is to make the tradeoffs explicit rather than
leaving them implicit in code comments or TODOs.

---

## 1. Open vs Closed Operator Set

`TaskRef` is a closed enum. Adding an operator requires touching `TaskRef`,
the `Operator` trait impl, and the `executor` registry.

This is correct for a schema-versioned, library-embedded DAG engine but it
breaks down if community-authored operators need to be loaded at runtime without
recompiling tinydag.

The alternative is using`Box<dyn Operator> + typetag::serde` for open dispatch.
Which costs us heap allocation per node, loss of `Clone` and `PartialEq` on
`TaskRef`, need for `dyn_clone`.

I think we'll need to review this before we add the next operator, tbh.

---

## 2. Output Protocol: Stdout vs File

**Current decision:** Operator scripts emit `{"outputs": {"key": value}}` on stdout.
The operator binary captures this and reports it to the control server.

**The problem:** Any stray `print()` from the script or any library it imports corrupts
the JSON. This is a constant source of user friction in practice. The failure mode is
an opaque parse error with no indication of what the stray output was.

**Alternative:** Outputs written to `tinydag_outputs.json` in the work directory,
symmetric with `tinydag_inputs.json`. Stdout becomes a pure logging channel.

**Tradeoff:** The file-based approach is more robust but requires user scripts to
explicitly know about tinydag's file conventions. Stdout is more natural for quick
one-liners.


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
