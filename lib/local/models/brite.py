from __future__ import annotations
import os
from dataclasses import dataclass, field
from hashlib import md5
import json
import re
import requests

from local.constants import WORKSPACE_ROOT


def LoadBrite():
    REF_DIR = WORKSPACE_ROOT.joinpath("data/references")
    if not REF_DIR.exists(): os.makedirs(REF_DIR, exist_ok=True)
    REF = REF_DIR.joinpath("brite.json")
    if not REF.exists():
        print("first time? This'll take a sec...")
        res = requests.get("https://www.kegg.jp/kegg-bin/download_htext?htext=KO&format=json")
        assert res.status_code == 200, "download failed"
        with open(REF, "w") as j:
            json.dump(res.json(), j, indent=4)
        print(f"caching raw data to {REF}")

    with open(REF) as j:
        brite = json.load(j) 

    entries: dict[str, Brite] = {}
    count = 0
    def _parse(raw: dict, depth=0, parent=None) -> BriteNode:
        nonlocal count
        count += 1

        # isleaf = "children" in node
        info = raw["name"]

        ko = info.split(" ")[0].upper()
        desc = info[len(ko):].strip()
        ko = {
            "KO00001": "00001"
        }.get(ko, ko)
        ko = "M"+ko if ko[0] != "K" else ko

        all_misc = re.findall(r"\[.+\]", desc)
        if all_misc is None: misc = []

        for x in all_misc:
            desc = desc.replace(x, "").strip()
        all_misc = [x[1:-1] for x in all_misc]
        EC = "EC:"
        misc = [x for x in all_misc if not x.startswith(EC)]
        ecs = [x[len(EC):] for x in all_misc if x.startswith(EC)]

        if "; " in desc:
            genes = desc.split("; ")[0]
            desc = desc[len(genes):]
            genes = genes.split(", ")
        else:
            genes = []

        names = desc.split(" / ")
        main = names[0]
        alts = names[1:] if len(names)>1 else []

        model = Brite(
            depth = depth,
            id = ko,
            desc = main,
            alts = alts,
            misc = misc,
            ec = ecs,
        )
        if ko in entries:
            assert entries[ko].GetHash() == model.GetHash()
            model = entries[ko]
        else:
            entries[ko] = model

        node = BriteNode(
            brite = model,
            parent = parent,
        )
        children = [_parse(n, depth+1, node) for n in raw.get("children", [])]
        for ch in children:
            ch.parent = node
        node.children = children
        model.nodes += [node]
        return node

    pbrite = _parse(brite)
    print(f"{len(entries)} unique entries, {count} nodes")
    return pbrite, entries

@dataclass
class Brite:
    depth: int
    id: str
    desc: str = ""
    genes: list[str] = field(default_factory=lambda: list())
    alts: list[str] = field(default_factory=lambda: list())
    misc: list[str] = field(default_factory=lambda: list())
    ec: list[str] = field(default_factory=lambda: list())
    nodes: list[BriteNode] = field(default_factory=lambda: list())
    _hash: str|None = None

    def _get_dict(self):
        BL = set("nodes, _hash".split(", "))
        return {k:v for k, v in self.__dict__.items() if k not in BL and (not isinstance(v, list) or len(v)>0)}
        
    def GetHash(self):
        if self._hash is None:
            self._hash = md5(json.dumps(self._get_dict()).encode("latin1")).hexdigest()
        return self._hash
    
    def __repr__(self) -> str:
        return json.dumps(self._get_dict(), indent=4)

@dataclass
class BriteNode:
    brite: Brite
    parent: BriteNode|None = None
    children: list[BriteNode] = field(default_factory=lambda: list())

    def __repr__(self) -> str:
        return json.dumps(dict(
            id = self.brite.id,
            depth = self.brite.depth,
            is_root = self.parent is None,
            children = len(self.children),
        ), indent=4)
