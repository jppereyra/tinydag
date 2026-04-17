import json

with open("tinydag_inputs.json") as f:
    inputs = json.load(f)

rows = int(inputs["rows"])
print(f"transform-a: doubling {rows} rows", flush=True)

with open("tinydag_outputs.json", "w") as f:
    json.dump({"outputs": {"doubled": rows * 2}}, f)
