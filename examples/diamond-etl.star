# Diamond ETL — demonstrates fan-out, parallel execution, and fan-in.
#
# Topology:
#   extract → transform-a (Python) ─┐
#           ↘                        → load
#             transform-b (bash)   ─┘
#
# Run with:  tinydag add examples/diamond-etl.star --run-now
# Compile:   tinydag compile examples/diamond-etl.star

cfg = config(
    name        = "etl-sample",
    pipeline_id = "sample-pipeline",
    team        = "data-eng",
    user        = "alice",
)

# Simulates I/O with a short sleep, emits {"rows": 42} for downstream tasks.
extract = bash_operator("extract",
    cmd = "echo extracting >&2 && sleep 0.1 && printf '{\"outputs\":{\"rows\":42}}' > tinydag_outputs.json",
)

# Reads `rows` from tinydag_inputs.json, doubles it, writes {"doubled": 84}.
transform_a = python_operator("transform-a",
    script     = "tasks/transform_a.py",
    inputs     = ["rows"],
    outputs    = ["doubled"],
    depends_on = extract,
)

# Runs in parallel with transform-a. Logs to stderr, produces no outputs.
transform_b = bash_operator("transform-b",
    cmd        = "echo transforming-b >&2",
    depends_on = extract,
)

# Waits for both transforms. Dumps tinydag_inputs.json to stderr so you can
# see `doubled` flowing in from transform-a.
load = bash_operator("load",
    cmd        = "echo loading >&2 && cat tinydag_inputs.json >&2",
    depends_on = [transform_a, transform_b],
)

build(cfg, load)
