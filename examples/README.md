# Examples

## diamond-etl

A four-node ETL pipeline demonstrating fan-out, parallel execution, fan-in,
and both operator types.

```
extract → transform-a (Python) ─┐
        ↘                        → load
          transform-b (bash)   ─┘
```

`extract` runs first and produces a `rows` output. `transform-a` and
`transform-b` run in parallel once it completes. `load` waits for both.

---

### Run it

You should have the tinydag binaries on your PATH. See the
[root README](../README.md#quick-start) if you haven't done that yet.

```sh
tinydag add examples/diamond-etl.star --run-now
```

Expected output:
```
run 3f2a1c8d-... succeeded (4 tasks)
```

Task logs go to stderr and will appear in your terminal alongside the run output.

### Compile only

```sh
tinydag compile examples/diamond-etl.star
```

Writes `examples/diamond-etl.dag.json` which is a validated, hashed DAG artifact.
Open it to see what the compiler produced.

### Register without running

```sh
tinydag add examples/diamond-etl.star
```

Compiles and registers without triggering a run. In a long-running process you
would trigger runs programmatically or (in a future version) via a cron
schedule or event.

---

### What the pipeline does

- **extract** — simulates I/O with a short sleep, then writes
  `{"outputs": {"rows": 42}}` to `tinydag_outputs.json`
- **transform-a** — a Python script (`tasks/transform_a.py`) that reads
  `rows` from `tinydag_inputs.json`, doubles it, logs a message to stdout,
  and writes `{"outputs": {"doubled": 84}}` to `tinydag_outputs.json`
- **transform-b** — a bash task that runs in parallel with transform-a;
  logs a message to stderr and produces no outputs
- **load** — dumps `tinydag_inputs.json` to stderr so you can see `doubled`
  flowing in from transform-a

Tasks exchange data through JSON files in the work directory. Upstream outputs
land in `tinydag_inputs.json` before your script runs; write your results to
`tinydag_outputs.json` when it finishes. Both files use the same shape:
`{"key": value}`. Stdout and stderr are free logging channels that go straight
to the terminal.
