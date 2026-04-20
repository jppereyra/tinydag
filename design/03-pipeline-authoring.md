### Task Definition Language

**Python is the user-facing DSL.**

The architecture is a compilation pipeline:

```
Python DSL -> Parser -> DagDef -> Orchestrator
                                  ^
External systems  ----------------|
```

The Python layer is a configuration language, not an execution environment.
Users define pipelines in Python; tinydag parses that into a DAG definition
and the orchestrator never sees raw Python.
This means users cannot interact with or affect the orchestrator from their
pipeline code.

**Reference:** Starlark (used by Bazel) is a restricted Python dialect designed
for exactly this use case. It's deterministic, no side effects, no imports,
sandboxed. Worth studying as a reference for what restrictions to impose on
the parser.

External systems can submit a DAG definition directly, bypassing the Python DSL entirely.

### Task Module Validation

At compile time, tinydag attempts to import every callable reference declared in
the DAG definition. A malformed module, broken import, or missing function is caught here.

**Validation modes:**

- **Strict (v1 default):** imports callable in local Python environment.
  Compilation fails if any callable cannot be imported. Appropriate for the
  local executor and development environments.
- **Container:** imports callable inside the execution container image
  locally. Same strictness as strict mode but validates against the actual
  execution environment. Requires Docker at compile time. Slower and more
  expensive but catches environment mismatches.
- **Manifest-based (v2, maybe):** the execution environment publishes a manifest
  of available callables; tinydag validates against that instead of importing
  directly. Decouples validation from the local environment entirely.
- **Warn:** skips validation and emits a warning. Escape hatch only.

### Task Interface

Pipeline structure, task wiring, and metadata are declared in the Starlark
file. Task logic lives in plain Python modules with no tinydag imports.

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

    cleaned = PythonOperator(
        task_id="clean",
        python_callable="mymodule.clean",
        inputs=["raw_data"],
        outputs=["clean_data"]
    )

    raw >> cleaned
```

```python
# mymodule.py - plain Python, no tinydag imports
import numpy as np

def extract():
    ...

def clean(raw_data):
    return np.array(raw_data).mean()
```

The Starlark file holds only string references to callables. tinydag resolves
those references at compile time but never executes them. Execution happens
in the task runtime, which is plain Python with no restrictions. This means
task code can be tested with plain pytest, run locally without tinydag
installed, and migrated away from tinydag without touching task modules.

**Open question: dependency declaration syntax**

For small pipelines, declaring dependencies inline with `depends_on` is
readable. For larger pipelines, a dedicated dependency block separating
structure from operator definitions scales better:

```python
dependencies = [
    extract >> clean,
    clean >> [enrich, validate],
    enrich >> aggregate,
    validate >> aggregate,
]
```

The right syntax for tinydag is an open question. It should be readable at
scale and make the topology of the pipeline visible without having to read
every operator definition. Feedback welcome.

### Static vs. Dynamic DAGs

> **Note:** The designs in this section are very early and speculative. The core
> principle is that static DAGs are always preferred (with clearly documented
> escape hatches), but the specific mechanisms (late-binding, resolvers, unsafe mode)
> are not yet finalized and will evolve with user input.

*Dynamic DAGs: here be dragons.*

The core principle is: almost every dynamic DAG is convertible to a static one, with some work. 
tinydag identifies common dynamic DAG scenarios and provides static solutions upfront.
For cases with no static solution, they are either declared out of scope or handled
via an explicit `unsafe` mode (TBD).

| Use Case | Static Solution |
|---|---|
| Parameterized pipelines (e.g. date partitions) | Static DAG + parameter injection at submission time |
| Fan-out over unknown inputs (e.g. files in a dir) | Pre-execution expansion via pipeline-level resolver; DAG structure is static, inputs are late-bound and resolved once before execution starts |
| Conditional branching | Static DAG with skip semantics on nodes |
| External system submitting pipelines | API that accepts a DAG definition, validates, then freezes it for execution |
| Retry with modified subgraph | New DAG submission, not mutation of running DAG |
| Recursive / iterate-until-convergence | Out of scope or `unsafe` mode |

#### Late-binding Inputs

For cases where inputs cannot be known at compile time (e.g. a directory of
files, a partition list, an API response), tinydag supports late-binding via
a pipeline-level resolver.

The resolver is a plain Python function declared on the DAG and called once
by tinydag at dispatch time, before any task runs. It returns a dictionary
of resolved values that late-bound inputs are keyed into:

```python
# pipeline.star
pipeline = DAG(
    "my_pipeline",
    schedule="0 * * * *",
    resolver="resolvers.resolve"
)

with pipeline:
    process = PythonOperator(
        task_id="process_file",
        python_callable="mymodule.process",
        inputs=late("files"),
        outputs=["result"]
    )
```

```python
# resolvers.py - plain Python, no restrictions
import boto3

def resolve(ctx):
    s3 = boto3.client("s3")
    objects = s3.list_objects(Bucket="my-bucket", Prefix="input/")
    return {
        "files": [obj["Key"] for obj in objects["Contents"]]
    }
```

tinydag calls the resolver once, validates that all declared `late()` keys
are present in the output, and proceeds to execution with fully resolved
inputs. If the resolver fails, the pipeline never starts and the error is
clearly attributable to the resolution phase.

The resolver follows the same compile-time validation rules as task modules:
the callable reference is validated at compile time, not at runtime.

**Known limitations:**

- Single point of failure with no granularity: if the resolver fails, the
  entire pipeline is blocked regardless of which input caused it
- Resolver complexity grows with pipeline complexity as more late-bound
  inputs are added
- All-or-nothing: if one input cannot be resolved, tasks that don't depend
  on it are still blocked
- No parallelism: resolution is sequential regardless of how many inputs
  need resolving
- Testing requires mocking all external systems the resolver touches

These tradeoffs are acceptable for v1.

### Compile time checks

- Structural errors: cycles, disconnected nodes, missing dependencies
- Contract mismatches: output type of node A does not match declared input of node B
- Missing task references: callable, script, or container does not exist
- Parameter errors: required params missing or wrong type
- Scope violations: Python DSL doing things it should not (imports, side effects, system calls)

Input/output contract declarations are a core requirement for any operator.
Without them, only structural errors can be caught at compile time.

### Pre-execution

A validation pass runs after compilation but before any task starts, checking
preconditions: e.g. partition exists, upstream table has landed, SLA window is open.
If any precondition fails, the pipeline never starts and dependents are notified.
Users declare preconditions explicitly and tinydag does not infer them.
Inference would require tinydag to understand the semantics of your data
stores and inputs, which is domain knowledge that belongs to the user, 
or another tool.

Preconditions are plain Python functions that return a boolean. tinydag
provides the interface and users implement them:

```python
# preconditions.py - plain Python, no tinydag imports
def my_table_ready(ctx):
    return check_partition_exists("my_table", ctx.execution_date)
```

```python
# pipeline.star
pipeline = DAG(
    "my_pipeline",
    schedule="0 * * * *",
    preconditions=["preconditions.my_table_ready"]
)

# Explicitly none: a conscious decision, not an oversight
pipeline = DAG(
    "my_pipeline",
    schedule="0 * * * *",
    preconditions=none()
)
```

A pipeline with neither preconditions declared nor `preconditions=none()`
produces a compile-time warning.

### Early execution

Some errors are unavoidable at compile time and can only be caught during
execution: runtime data errors, bad values, schema drift, external system
failures, and logic errors inside task code. tinydag does not check these
directly. They surface through task-level telemetry as structured errors,
giving the user enough context to understand what failed and where.

### Testing

tinydag has two distinct levels of testing, both first-class and supported
out of the box via a `tinydag.testing` module.

**Task-level tests**: plain pytest, no tinydag involved. Because task
modules have zero tinydag imports, they test like any other Python code:

```python
# test_mymodule.py
from mymodule import clean

def test_clean():
    assert clean([1, 2, 3]) == 2.0
```

**Pipeline-level tests**: tinydag compiles the Starlark file and exposes
the result for assertion. Structure, contracts, and callable references are
all validatable:

```python
# test_pipeline.py
from tinydag.testing import compile

def test_pipeline_structure():
    dag = compile("pipeline.star")
    
    # Topology
    assert dag.depends_on("clean", "extract")
    assert not dag.depends_on("extract", "clean")  # not the other way around
    assert len(dag.tasks) == 2  # no accidental extra tasks
    
    # Task references point where we expect
    assert dag.task("extract").callable == "mymodule.extract"
    assert dag.task("clean").callable == "mymodule.clean"
    
    # Scheduling intent
    assert dag.schedule == "0 * * * *"
    assert dag.task("extract").timeout == 300


```

Pipeline-level tests are a capability but not a requirement. The compiler does
the heavy lifting: contracts, references, and structure are validated on
every `compile()` call. The testing module is for teams that need to go
further:

- **Large pipelines:** 20, 30, 50+ tasks where topology is hard to reason
  about by reading the Starlark file alone
- **Frequently changing pipelines:** catches regressions when someone
  modifies a pipeline they didn't originally write
- **Critical pipelines:** where a miswired dependency causes downstream
  data corruption or missed SLAs; the cost of a bug justifies the overhead
- **Team environments:** tests serve as documentation of intent as much as
  correctness checks when multiple people touch the same pipeline definitions

## 5. Macros

TL;DR: tinydag does not support macros.

We define macros as arbitrary Python functions called inside templates rendering
at task execution time, on the worker, right before `execute()` runs.
They usually look like `{{ my_macro(ds, table) }}` in operator params.

**We have seen them cause problems:**

- They run on workers at execution time which means workers need network access
  to every external system any macro might call. A macro that queries the Hive
  metastore means every worker needs metastore access.
- They run at different times for different tasks in the same run. If 20 tasks
  use `{{ max_partition('events') }}`, that's 20 separate metastore calls at
  20 different moments. If a new partition lands mid-run, different tasks can
  see different values. **The pipeline produces silently incorrect data with no
  error, no failure, nothing in the logs.**
- The scheduler sees a template string, not a function call. It cannot enforce
  ordering, cannot retry the macro call independently, cannot surface it in
  telemetry.
- The solution to the previous points usually involve adding complxity to the
  macro (caching, more network calls, etc.) so they grow.
  Teams start with `{{ format_date(ds) }}` and end up with macros
  that make API calls, query databases, and implement business logic, all
  running inside the template renderer with no observability.
- Error messages tend to be bad.

**So, in tinydag:**

Anything a macroo would compute belongs in a task that emits it as an output.
The canonical example:

```python
# Instead of: cmd = "process --partition={{ max_partition('events') }}"

max_partition = python_operator("max-partition",
    script  = "tasks/max_partition.py",
    outputs = ["partition"],
)

process = bash_operator("process",
    cmd        = "process --partition=$(jq -r .partition tinydag_inputs.json)",
    inputs     = ["partition"],
    depends_on = max_partition,
)
```

`max_partition.py` calls the metastore once, emits the value as an output.
Every downstream task receives exactly that value. The partition is frozen for
the duration of the run.

What you gain over the macro approach:
- The metastore call is a node in the graph with start time, duration,
  success/failure status, and retry policy
- A metastore failure is a clear task failure, not a cryptic template error
- The partition value is an auditable output visible in telemetry
- `inputs = ["partition"]` makes the dependency explicit and compile-time checkable
- `tasks/max_partition.py` is testable in isolation with plain pytest

**For execution context** (run date, run ID, etc.) the scheduler injects a
rich set of standard variables into `tinydag_params.json` automatically. No
template syntax needed.

**The broader principle:** Every custom macro is a piece of business
logic that escaped into the wrong layer.

## Backfills

Backfills are a v2 feature. v1 supports scheduled and manual runs only.

A backfill is not "run this pipeline again for old dates", it is "run a
variant of this pipeline for a date range." That variant is a first-class
artifact, not a runtime flag. It goes through the same compilation pipeline
as any other DAG (parse → validate → DAG definition → execute). The DAG definition carries the date
range and a pointer to the original pipeline version it was derived from.

Users can specify pipeline modifications for a backfill: skipping tasks,
swapping implementations, changing parameters. The variant declaration is
also where the user specifies what happens to data that already exists.

Downstream pipelines are notified when a backfill run completes, same as
any other run.