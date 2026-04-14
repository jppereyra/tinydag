### Operator Catalog

tinydag ships with a catalog of native operators implemented in Rust.
For common operations, users should not need Python at all. Native operators
are declared directly in the Starlark file:

```python
with pipeline:
    load = SQLOperator(
        task_id="load",
        conn="my_postgres",
        query="INSERT INTO target SELECT * FROM staging",
    )

    move = S3Operator(
        task_id="move",
        src="s3://bucket/input/",
        dst="s3://bucket/processed/",
    )
```

`PythonOperator` remains the escape hatch for custom business logic that the
native catalog does not cover. The goal is for the 80% case to require no
Python in the execution path at all.

**v1 catalog:**

- `PythonOperator`: runs a callable from a plain Python module
- `BashOperator`: runs a shell command

**Planned (v2+):**

- `SQLOperator`: executes a query against a database connection
- `S3Operator`: file operations against S3-compatible storage
- `HTTPOperator`: makes an HTTP request
- `KubernetesOperator`: submits a job to a Kubernetes cluster

The catalog is intentionally small. Operators that cover cloud-specific or
niche use cases belong in a community package rather than core. The boundary
is: if a significant majority of tinydag users would need it, it belongs in
core. Otherwise it belongs in the ecosystem.

### Operator Interface

tinydag defines a language-agnostic operator interface. Operators are
standalone executables that communicate with tinydag via a gRPC control
protocol and with the observability backend via OpenTelemetry. The
implementation language is irrelevant; anything that can speak gRPC and
emit OTel events can be a tinydag operator.

The `PythonOperator` and `BashOperator` in core are tinydag's own
implementations of this contract. Community operators follow the same model.

**Rust convenience layer:**

tinydag will provide a `#[tinydag::operator]` macro that handles the gRPC
control protocol and OTel instrumentation automatically. Users write
idiomatic Rust; the macro handles the rest.

> **Note:** The operator SDK is not yet designed.

## Operator parameter schemas

Operators declare their own parameter schemas. At compile time, tinydag
validates every operator invocation against its schema:

- Required parameters are present
- Parameter names match the schema exactly (catches typos like `parittion_date`)
- Parameter types match declared types

This does not catch typos in parameter *values* -- a wrong partition name
like `my_table/dt=2024-01-32` cannot be validated at compile time for
dynamic pipelines where values are templated or late-bound. That class of
error is caught at operator self-validation time, when the operator can
check against a live catalog or schema.

Parameter schemas are declared by the operator author and versioned
alongside the operator binary. When an operator is registered with tinydag,
its schema is stored and used for compile-time validation of every pipeline
that references it.

> **Open question:** the format for operator parameter schemas is not yet
> defined. Candidates include a protobuf descriptor, a JSON Schema document,
> or a custom tinydag schema format.