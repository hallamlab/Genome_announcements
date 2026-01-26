from __future__ import annotations
import os
from dataclasses import dataclass, field
from hashlib import md5
import json
import xmltodict
import requests

from local.constants import WORKSPACE_ROOT
from local.caching import load, save, save_exists


def _parse_raw(raw: dict):
    print = lambda x=None: None

    axioms = raw["rdf:RDF"]["owl:Axiom"]
    classes = raw["rdf:RDF"]["owl:Class"]
    edge_types = raw["rdf:RDF"]["owl:ObjectProperty"]
    # len(axioms), len(classes), len(edge_types)

    relation_types = {}
    for item in edge_types:
        relation_types[item["oboInOwl:hasDbXref"]] = item["rdfs:label"]

    IS_A = "is a"
    ontology = {}
    edges = {} # key -> value; specific -> general
    for i, item in enumerate(classes):
        if "owl:deprecated" in item: continue

        myid = item["oboInOwl:id"]
        refs = item.get("rdfs:subClassOf", [])
        if isinstance(refs, dict): refs = [refs]
        for ref in refs:
            if "@rdf:resource" in ref:
                parent = _url2id(ref["@rdf:resource"])
                edges[myid] = edges.get(myid, [])+[(IS_A, parent)]
            else:
                rel = ref["owl:Restriction"]
                rtype = _url2id(rel["owl:onProperty"]["@rdf:resource"])
                rtype = relation_types[rtype]
                parent = _url2id(rel["owl:someValuesFrom"]["@rdf:resource"])
                edges[myid] = edges.get(myid, [])+[(rtype, parent)]

        model = GeneOntology(
            id = myid,
            label = item["rdfs:label"],
            desc = [v for k, v in item.items() if "obo1" in k][0],
        )
        assert myid not in ontology
        ontology[myid] = model

    num_edges = sum(len(e) for e in edges.values())
    print(f"{len(ontology)} terms, {num_edges} edges")


    trees = {v:dict() for v in relation_types.values()}|{IS_A: dict()}
    for child, parents in edges.items():
        for rtype, parent in parents:
            tree = trees[rtype]
            tree[parent] = tree.get(parent, [])+[child]

    roots = {}
    for rtype, tree in trees.items():
        all_children = set()
        for children in tree.values():
            for ch in children:
                all_children.add(ch)
        _r = []
        for go in tree:
            if go not in all_children:
                _r.append(go)
        roots[rtype] = _r

    print()
    for rtype, rs in roots.items():
        print(f"{rtype}: {len(rs)} roots")
    split_edge_count = 0
    for r, tree in trees.items():
        split_edge_count += sum(len(v) for v in tree.values())
    assert num_edges == split_edge_count

    def _set_depth(tree, roots):
        _seen = set()
        todo: list[tuple[GeneOntology, int]] = [(ontology[go], 0) for go in roots]
        print(len(todo))
        while len(todo) > 0:
            node, depth = todo.pop(0)
            if node.id in _seen: continue
            _seen.add(node.id)
            node.depth = depth
            for child in tree.get(node.id, []):
                todo.append((ontology[child], depth+1))

    _set_depth(trees[IS_A], roots[IS_A])
    return ontology, trees, roots[IS_A]

def LoadGo() -> tuple[dict[str, GeneOntology], dict[str, dict[str, str]], list[str]]:
    REF_DIR = WORKSPACE_ROOT.joinpath("data/hierarchies")
    if not REF_DIR.exists(): os.makedirs(REF_DIR, exist_ok=True)
    REF = REF_DIR.joinpath("gene_ontology.owl")
    if not REF.exists():
        print("first time? This'll take a sec...")
        res = requests.get("http://purl.obolibrary.org/obo/go.owl")
        assert res.status_code == 200, "download failed"
        with open(REF, "w") as j:
            j.write(res.text)
        print(f"caching raw data to {REF}")

    SNAME = "go_ref"
    if save_exists(SNAME):
        ontology, trees, roots = load(SNAME)
    else:
        print("parsing...")
        with open(REF) as f:
            raw = xmltodict.parse(f.buffer)
        ontology, trees, roots = _parse_raw(raw)
        save(SNAME, (ontology, trees, roots))
    return ontology, trees, roots

@dataclass
class GeneOntology:
    id: str
    label: str
    desc: str
    depth: int = -1

    def _get_dict(self):
        BL = set()
        return {k:v for k, v in self.__dict__.items() if k not in BL and (not isinstance(v, list) or len(v)>0)}

    def __repr__(self) -> str:
        return json.dumps(self._get_dict(), indent=4)

def _url2id(url):
    return url.split("/")[-1].replace("_", ":")
