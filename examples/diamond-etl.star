dag("etl-sample",
    pipeline_id = "sample-pipeline",
    team        = "data-eng",
    user        = "alice",
)

extract = bash_operator("extract",
    cmd = "echo extracting >&2 && sleep 0.1 && printf '{\"outputs\":{\"rows\":42}}'",
)
transform_a = bash_operator("transform-a",
    cmd = "echo transforming-a >&2 && printf '{\"outputs\":{}}'",
    depends_on = extract,
)
transform_b = bash_operator("transform-b",
    cmd = "echo transforming-b >&2 && printf '{\"outputs\":{}}'",
    depends_on = extract,
)
bash_operator("load",
    cmd = "echo loading >&2 && printf '{\"outputs\":{}}'",
    depends_on = [transform_a, transform_b],
)
