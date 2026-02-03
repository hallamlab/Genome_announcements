from pathlib import Path
import os
import json
import pandas as pd
from hashlib import md5
from metasmith.python_api import DataInstanceLibrary

root= Path("../data")
res_path = root/"assembly/hifi_meta"
# res_path = root/"assembly/flye_raw"
# res_path = root/"taxonomy/flye_raw/metabuli"
# res_path = root/"taxonomy/flye_raw/checkm.ana"
# res_path = root/"assembly/hifi_100x"
# res_path = root/"taxonomy/hifi_100x/metabuli"
# res_path = root/"taxonomy/hifi_100x/checkm.ana"
# res_path = root/"assembly/hifi_meta"
# res_path = root/"taxonomy/hifi_meta/metabuli"
# res_path = root/"taxonomy/hifi_meta/checkm"
# res_path = root/"assembly/check_flye_raw"
# res_path = root/"taxonomy/check_flye_raw"
# res_path = root/"taxonomy/hifi_meta/gtdbtk.ana"

def _get_hash(p):
    _hash = md5(str(p).encode()).hexdigest()
    _hash = int(_hash[:15], 16) # 15 is important as it allows us to disregard the sign of a long and match with java
    return _hash

dfg = pd.read_csv(res_path/"_manifests/given.csv")
print(dfg.shape)
# given2k = {Path(r["path"]).name:(r["instance_key"], _get_hash(r["path"])) for _, r in dfg.iterrows()}
given2k = {Path(r["path"]).name:(r["instance_key"], r["instance_index"]) for _, r in dfg.iterrows()}
given2k = {k:v for k, v in given2k.items() if k.endswith("gz") or k.endswith("fna")}
# given2k = {k:v for k, v in given2k.items() if k.endswith("fna")}
# print(json.dumps(given2k, indent=4))
k2name = {v[1]: k.split(".")[0] for k, v in given2k.items()}
# print(json.dumps(k2name, indent=4))

lineages = []
for j in res_path.glob("_manifests/*.json"):
    with open(j) as f:
        lineages+=json.load(f)
p2lin = {d["path"]:d for d in lineages}

reslib = DataInstanceLibrary.Load(res_path)
to_rename = {}
for p, lin in p2lin.items():
    lin = lin["lineage"]
    pk = "fWlq91HI"
    # pk = "1DldQWJB"
    assert pk in lin, p 
    k = lin[pk][0]
    if k not in k2name:
        print(k)
    name = k2name[k]
    p = Path(p)
    new_path = p.parent/f"{name}{p.suffix}"
    to_rename[p] = new_path

_seen = set()
for a, b in to_rename.items():
    if b in reslib: continue
    print(f"{a} -> {b}")
    assert b not in _seen
    _seen.add(b)


# # check twice, cut once
#     reslib.Rename(a, b, _save=False)
# reslib.Save() # important!
# print("done!")
