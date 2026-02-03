from pathlib import Path
import pandas as pd
import json

root= Path("../data/assembly")

rows = []
for f in root.glob("**/sequences-assembly_stats/*.json"):
    # print()
    with open(f) as j:
        d = json.load(j)
        d["method"] = f.parents[1].name
        d["sample"] = f.stem

    e = {k:v for k, v in d.items() if not isinstance(v, dict)}
    for sub, name in [
        ("_raw_mapping", "minimap2"),
        ("_raw_seqkit", "seqkit"),
    ]:
        for k, v in d[sub].items():
            kk = f"{name}_{k}".replace(" ", "_")
            e[kk] = v
    rows.append(e)
df = pd.DataFrame(rows)
df = df.sort_values(["method", "sample"])
df.to_csv("./stats.asm.csv", index=False)

rows = []
for f in root.glob("**/sequences-read_qc_stats/*.json"):
    # print()
    with open(f) as j:
        d = json.load(j)
        d["method"] = f.parents[1].name
        d["sample"] = f.stem

    e = {k:v for k, v in d.items() if not isinstance(v, dict)}
    for sub, name in [
        ("_raw", "raw"),
    ]:
        for k, v in d[sub].items():
            kk = f"{name}_{k}".replace(" ", "_")
            e[kk] = v
    rows.append(e)
df = pd.DataFrame(rows)
df = df.sort_values(["method", "sample"])
df.to_csv("./stats.reads.csv", index=False)

