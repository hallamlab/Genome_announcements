from __future__ import annotations
import os, sys
import json
from pathlib import Path
from dataclasses import dataclass, field

from local.constants import WORKSPACE_ROOT
from local.biocyc_facade import Pgdb
from local.biocyc_facade.utils import parseDat

def LoadMetacyc(ver: str = "26.0"):
    REF_DIR = WORKSPACE_ROOT.joinpath("data/hierarchies")
    VER = ver
    if not REF_DIR.exists(): os.makedirs(REF_DIR, exist_ok=True)
    REF = REF_DIR.joinpath("meta")
    if not REF.exists():
        print("metacyc needs a licence and will need to be acquired manually")
        print("https://metacyc.org/download.shtml")
        print("ensure the metacyc reference exists here:")
        print(REF)

    def _get(fpath: Path, bl: set[str] = set(), root: str|None = None):
        entries = parseDat(fpath, "UNIQUE-ID", {}, all_fields=True)

        tree = {}
        for k, v in entries.items():
            parents = v.get("TYPES", [])
            if len(parents) == 0: continue
            for p in parents:
                tree[p] = tree.get(p, [])+[k]

        if root is not None:
            rxn_tree = {}
            def _build_tree(node: str):
                if node not in tree: return
                rxn_tree[node] = rxn_tree.get(node, [])+[c for c in tree[node] if c not in bl]
                for c in rxn_tree[node]:
                    _build_tree(c)
            _build_tree(root)
            tree = rxn_tree
        return entries, tree

    pgdb_path = REF_DIR.joinpath("metacyc.pgdb")
    if pgdb_path.exists():
        meta = Pgdb(pgdb_path)
    else:
        meta = Pgdb.ImportFromBiocyc(pgdb_path, REF.joinpath(f"{VER}/data"))
    ver_str = meta.GetInfo().get("VERSION", "ERROR!")
    print(f"metacyc ver: {ver_str}")

    ROOT = "Generalized-Reactions"
    h_entries, h_tree = _get(WORKSPACE_ROOT.joinpath(f"data/hierarchies/meta/{VER}/data/classes.dat"), {"Reactions", "Super-Pathways"}, ROOT)
    w_entries, w_tree = _get(WORKSPACE_ROOT.joinpath(f"data/hierarchies/meta/{VER}/data/pathways.dat"), {"Super-Pathways"})
    entries = h_entries|w_entries
    tree = h_tree
    for k, v in w_tree.items():
        tree[k] = tree.get(k, [])+v

    ontology: dict[str, Metacyc] = {}
    mroot: MetacycNode|None = None
    todo: list[tuple[str, int, MetacycNode|None]] = [(ROOT, 0, None)]
    while len(todo) > 0:
        k, depth, parent = todo.pop(0)
        if k not in ontology:
            data = entries[k]
            # desc = data.get("COMMON-NAME", data.get("COMMENT", k))
            desc = data.get("COMMON-NAME", k.replace("-", "").lower())
            if isinstance(desc, list):
                assert len(desc) == 1, desc
                desc = desc[0]
            model = Metacyc(k, desc)
            ontology[k] = model
        else:
            model = ontology[k]
        node = MetacycNode(model, depth, parent)
        model.nodes.append(node)
        if mroot is None: mroot = node
        if parent is not None:
            parent.children.append(node)

        for ch in tree.get(k, []):
            todo.append((ch, depth+1, node))

    assert mroot is not None
    return ontology, mroot, meta

@dataclass
class Metacyc:
    id: str
    desc: str
    nodes: list[MetacycNode] = field(default_factory=lambda: list())

    def _get_dict(self):
        BL = {"nodes"}
        return {k:v for k, v in self.__dict__.items() if k not in BL and (not isinstance(v, list) or len(v)>0)}

    def __repr__(self) -> str:
        return json.dumps(self._get_dict(), indent=4)
    
@dataclass
class MetacycNode:
    data: Metacyc
    depth: int
    parent: MetacycNode|None
    children: list[MetacycNode] = field(default_factory=lambda: list())

    def __repr__(self) -> str:
        return json.dumps(self.data._get_dict()|dict(depth=self.depth), indent=4)


# M_ONTOLOGY, MROOT, MDB = LoadMetacyc()