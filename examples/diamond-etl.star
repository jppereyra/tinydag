dag("etl-sample",
    pipeline_id = "sample-pipeline",
    team        = "data-eng",
    user        = "alice",
)

extract = bash_operator("extract",
    cmd = "echo extracting >&2 && sleep 0.1 && printf '{\"outputs\":{\"rows\":42}}' > tinydag_outputs.json",
)

transform_a = python_operator("transform-a",
    script     = "tasks/transform_a.py",
    inputs     = ["rows"],
    outputs    = ["doubled"],
    depends_on = extract,
)

transform_b = bash_operator("transform-b",
    cmd = "echo transforming-b >&2",
    depends_on = extract,
)

bash_operator("load",
    cmd = "echo loading >&2 && cat tinydag_inputs.json >&2",
    depends_on = [transform_a, transform_b],
)
