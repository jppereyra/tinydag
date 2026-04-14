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

tinydag defines a language-agnostic operator interface over a stable C ABI.
Any language that can expose a C ABI can implement a tinydag operator: Rust,
C, C++, Go, or anything else. The implementation language is irrelevant to
tinydag.

The interface specifies:

- How inputs are received
- How outputs are returned
- How errors are reported
- How telemetry is emitted

Operators are compiled as C-compatible dynamic libraries and loaded by
tinydag at startup. The `PythonOperator` and `BashOperator` in core are
tinydag's own implementations of this same contract.

This means the operator ecosystem is naturally extensible by the community
without tinydag needing special knowledge of any language beyond the C ABI
contract. Operator authors also get full control over their own telemetry
and validation within the bounds of the interface.

**Rust convenience layer:**

tinydag will provide a `#[tinydag::operator]` macro that generates the C ABI
boundary automatically. Users write idiomatic Rust; the macro handles the
rest. This is a convenience, not a special case -- a C++ header or a Go cgo
wrapper serve the same purpose.

> **Note:** The operator interface ABI is not yet designed. This is a
> significant piece of work and will be treated as a formal RFC when the
> time comes.