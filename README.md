# tinydag

[![CI](https://github.com/jppereyra/tinydag/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jppereyra/tinydag/actions/workflows/ci.yml)

A tiny DAG orchestrator written in Rust.

Pipelines are defined in Starlark, a sandboxed dialect of Python with no imports and no side effects. That constraint is what makes compile-time validation possible: cycles, broken dependencies, missing scripts, and script syntax errors are caught at submission time, not at 3am when the pipeline quietly fails in production. Tasks run as separate processes and report back over gRPC, so operators can be written in any language. Every run emits OpenTelemetry spans and metrics out of the box.

Lastly, tinydag does not assume it is the center of your infrastructure.

---

## Quick start

Download the latest release for your platform from the [releases page](https://github.com/jppereyra/tinydag/releases):

| Platform | Archive |
|---|---|
| Linux x86_64 | `tinydag-x86_64-linux.tar.gz` |
| macOS Apple Silicon | `tinydag-aarch64-macos.tar.gz` |
| macOS Intel | `tinydag-x86_64-macos.tar.gz` |

Each archive contains three binaries: `tinydag` (the CLI), `tinydag-op-bash`, and `tinydag-op-python`. All three need to be on your PATH.

Then run the example from this repo:

```sh
git clone https://github.com/jppereyra/tinydag
cd tinydag
tinydag add examples/diamond-etl.star --run-now
```

See [examples/README.md](examples/README.md) for a full walkthrough of the example pipeline.

### Building from source

If you'd rather build from source, or if you're contributing to tinydag:

```sh
# Prerequisites: Rust toolchain (https://rustup.rs) and protoc
# macOS:         brew install protobuf
# Ubuntu/Debian: sudo apt-get install protobuf-compiler

cargo build --release
export PATH="$PWD/target/release:$PATH"
```

---

## Writing a pipeline

Pipelines are defined in `.star` files using a small DSL built on Starlark. A pipeline file has three parts: a config header, operator definitions, and a `build()` call that assembles the graph.

```python
# pipeline.star
cfg = config(
    name        = "my-pipeline",
    pipeline_id = "my-team-pipelines",
    team        = "data-eng",
    user        = "alice",
)

extract = bash_operator("extract",
    cmd = "printf '{\"outputs\":{\"rows\":1000}}' > tinydag_outputs.json",
)

transform = bash_operator("transform",
    cmd        = "echo transforming >&2",
    depends_on = extract,
)

load = bash_operator("load",
    cmd        = "echo loading >&2",
    depends_on = transform,
)

build(cfg, load)
```

Operator functions return values that you should pass to `depends_on` directly. `build()` takes the terminal node (or a list of terminal nodes) and walks the dependency graph from there. You can use the full Starlark language to build pipelines programmatically:

```python
cfg = config(name="regional-pipeline", team="data-eng")

def regional(region):
    extract = bash_operator(f"extract-{region}", cmd = f"extract.sh {region}")
    load    = python_operator(f"load-{region}",   script = "tasks/load.py",
                              depends_on = extract)
    return load

loads = [regional(r) for r in ["us", "eu", "apac"]]
done  = bash_operator("done", cmd = "echo all regions loaded", depends_on = loads)

build(cfg, done)
```

Tasks write outputs to `tinydag_outputs.json` in their work directory: `{"outputs": {"key": value}}`. Stdout is a free logging channel. Upstream outputs are available in `tinydag_inputs.json` before your script runs. DAG params are in `tinydag_params.json`.


---

## CLI

```
tinydag compile <pipeline.star> [--output <path>]
```
Compiles a Starlark pipeline, runs validation, and writes the DAG artifact to disk as JSON. Default output path: `<pipeline>.dag.json`. This is useful for debugging purposes.

```
tinydag add <pipeline.star> [--run-now]
```
Compiles, validates, and registers the pipeline with the scheduler.
With `--run-now`, an immediate run is triggered and the result is printed on completion.

---

## How it works

```
pipeline.star  →  compiler  →  DagDef  →  scheduler  →  runner  →  executor  →  operator binary
```

The compiler is the only path to a `DagDef`. On trigger, the runner walks the graph dispatching tasks as their dependencies complete and running independent branches in parallel.

Operator binaries are separate processes. The executor spawns them, writes the dispatch payload to stdin, and receives lifecycle events (started, heartbeat, succeeded, failed) over gRPC. Any binary that implements the protocol can be a valid operator.

---

## Status

v0.1 The core pipeline lifecycle works end to end: compile, validate, schedule, dispatch, observe. See [TODO.md](TODO.md) and [Roadmap](design/07-roadmap.md) for what's next.

What works:
- Starlark DSL compiler with full structural validation
- Bash and Python operators with pipeline compile-time syntax checking
- Local subprocess execution backend
- Parallel task dispatch
- gRPC operator control protocol with heartbeat-based stuck-task detection
- OpenTelemetry trace and metric emission
- `tinydag compile` and `tinydag add [--run-now]` CLI

What's missing: cron scheduling, run history, retry logic, Kubernetes/Lambda execution backends, resume from failure, a web UI, and probably much more.

---

## Design

See [DESIGN.md](DESIGN.md) for the full design rationale: concepts, pipeline authoring model, operator protocol, observability, internals, and roadmap.

---

## Contributing

tinydag is early-stage and the design is still in flux. The best contributions right now are trying it on real pipelines and opening issues when things break or feel wrong.
If you want to contribute code make sure you read the relevant section in [DESIGN.md](DESIGN.md) and open an issue first to discuss the change (especially for anything touching the compiler, the operator protocol, or the DAG schema).
For smaller things (docs, test coverage, bug fixes) pull requests are welcome without prior discussion.

---

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
