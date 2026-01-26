from __future__ import annotations
from pathlib import Path
import json
from dataclasses import dataclass
import pandas as pd
import os

from local.constants import WORKSPACE_ROOT
from local.utils import regex
from local.caching import load

# setup kegg orthology
# download from https://www.kegg.jp/kegg-bin/download_htext?htext=ko00001.keg&format=json
# view at https://www.kegg.jp/brite/ko00001

with open(WORKSPACE_ROOT.joinpath("data/references/brite.json")) as j:
    brite_raw = json.load(j)

def gene_name_ok(gene, ko):
    if "." in gene: return False # E4.1.1.32
    if gene == ko: return False
    return True

children_map = {}
parent_map = {}
meta_map = {}
def traverse(node, parent):
    raw_meta = node["name"]
    ko = raw_meta.split(" ")[0]
    _m = raw_meta[len(ko):].strip()

    if ko[0] == "K":
        if ";" in _m:
            genes, _m = _m.split(";", 1)
            _m = _m.strip()
            genes = [x for g in [x.split("-") for x in genes.split(",")] for x in g if gene_name_ok(x, ko)]
        else:
            genes = []
        ec = list(regex(r"\[EC:[\d\.\s-]+\]", _m))
        if len(ec) > 0:
            _m = _m[:-len(ec[0])].strip()
            ec = ec[0][4:-1].split(" ")

        meta_map[ko] = ("leaf", _m, (genes, ec))
    elif parent is not None:
        meta = list(regex(r"\[.+\]", _m))
        if len(meta) > 0:
            _m = _m[:-len(meta[0])].strip()
            meta = {k:v for k, v in [x.split(":") for x in meta[0][1:-1].split(" ")]}
        else:
            meta = {}
        meta_map[ko] = ("node", _m, meta)
    else:
        meta_map[ko] = ("root", "root", None)

    for ch in node.get("children", []):
        traverse(ch, ko)
    if parent is None: return
    children_map[parent] = children_map.get(parent, set())|{ko}
    parent_map[ko] = parent_map.get(ko, set())|{parent}
traverse(brite_raw, None)

def klineage(ko):
    if ko not in parent_map:
        return [[ko]]
    else:
        all_lineages = []
        for path in parent_map[ko]:
            all_lineages += [[ko]+lin for lin in klineage(path)]
        return all_lineages

depth_map: dict[str, set[int]] = {}
def get_depths(ko):
    if ko not in parent_map:
        depth_map[ko] = {0}
    elif ko not in depth_map:
        parent_depths = {d for g in [get_depths(p) for p in parent_map[ko]] for d in g}
        depth_map[ko] = {d+1 for d in parent_depths}
    return depth_map[ko]

def parents_at(ko, depth, whitelist=None):
    def _parents_at(_ko):
        if whitelist is not None and _ko not in whitelist: return set()
        depths = get_depths(_ko)
        to_return = set()
        if any(d > depth for d in depths):
            for p in parent_map[_ko]:
                to_return |= _parents_at(p)
        elif any(d == depth for d in depths):
            to_return.add(_ko)
        return to_return
    depths = get_depths(ko)
    results = _parents_at(ko)
    if any(d < depth for d in depths):
        results.add(ko)
    return results

def get_list(ko, whitelist=False):
    ref = set()
    def _register(_ko, force=True):
        parents = parent_map.get(_ko, set())
        if force or whitelist or all(p in ref for p in parents):
            ref.add(_ko)
        for ch in children_map.get(_ko, set()):
            _register(ch, force=False)
    _register(ko)
    return ref

REFS = {}
for k in "09100, 09120, 09130, 09140, 09180".split(", "):
    REFS[k] = get_list(k)
    print(k, meta_map[k][1], len(REFS[k]))

def kegg_aggregate(ko):
    depths = {
        "09100": 2, # Metabolism
        "09120": 2, # Genetic Information Processing
        "09130": 2, # Environmental Information Processing
        "09140": 2, # Cellular Processes
        "09180": 3, # Brite Hierarchies
    }
    lins = klineage(ko)
    aggregated = set()
    for l in lins:
        group_k = l[-2] if len(l) > 1 else l[0]
        if group_k not in depths: continue
        d = depths[group_k]
        cat = l[-d-1]
        aggregated.add((cat, group_k))
    return aggregated

print(f"{len(meta_map)} nodes in kofam meta_map")

# _fos_kegg = load("fos_kegg", alt_workspace="../annotations")
# def inspect_category(cat, clones=None):
#     kos = {}
#     members = {}
#     for clone, (orfs, clone_cats) in _fos_kegg.items():
#         if clones is not None and int(clone) not in clones: continue 
#         if cat not in clone_cats: continue
#         for orf, ko, cats in orfs:
#             if cat in cats:
#                 kos[ko] = kos.get(ko, 0)+1
#                 members[ko] = members.get(ko, []) + [(clone, orf)]
#     kos = sorted(kos.items(), key=lambda x: x[1], reverse=True)
#     return kos, members

# def print_category(cat, clones=None):
#     kos, members = inspect_category(cat, clones)
#     print(f"{sum(count for _, count in kos)} {cat}:{meta_map[cat][1]}")
#     for ko, count in kos:
#         print("  ", count, ko, meta_map[ko][1], members[ko])
#     return kos, members

@dataclass
class Hit:
    k: str
    ko: str
    threshold: float|None
    score: float
    evalue: float
    description: str

    def FracScore(self):
        if self.threshold is None:
            return None
        else:
            return self.score / self.threshold
    
    def IsSignificant(self):
        s = self.FracScore()
        return s is not None and s >= 1
    
    def BetterThan(self, other: Hit):
        s, o = self.FracScore(), other.FracScore()
        if s is None or o is None:
            return self.score > other.score
        else:
            return s > o

def parse_kofam_results(kofam_results: Path):
    """accepts single file or folder of multiple files"""
    def parse_hit(l: str):
        row = [x for x in l[:-1].split(" ")[1:] if x != ""] # just ignore the star, can just look at ratio ourselves
        desc = " ".join(row[5:])
        k, ko, thres, score, evalue = row[:5]
        thres = float(thres) if thres != "-" else None
        return k, Hit(k, ko, thres, float(score), float(evalue), desc)
    
    if kofam_results.is_dir():
        files = [f for f in kofam_results.iterdir() if f.is_file() and f.name.endswith(".out")]
        files = sorted(files, key=lambda x: x.stem)
    else:
        files = [kofam_results]
    best_hits = {}
    for i, f in enumerate(files):
        print(f"{i+1} of {len(files)} | {f.name}", end="\r")

        with open(kofam_results.joinpath(f), "r") as fh:
            for l in fh:
                if l.startswith("#"): continue
                k, hit = parse_hit(l)
                if k not in best_hits or hit.BetterThan(best_hits[k]):
                    best_hits[k] = hit
    print()
    _rows = []
    for hit in best_hits.values():
        _rows.append([hit.k, hit.ko, hit.threshold, hit.score, hit.evalue, hit.description])
    df = pd.DataFrame(_rows, columns=["orf", "ko", "hmm_threshold", "score", "evalue", "description"])
    return df
    # df.to_csv(metag_best_hits, sep="\t", index=False)
