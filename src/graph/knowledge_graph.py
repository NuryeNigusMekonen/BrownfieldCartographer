from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

from src.models.schemas import EdgeType


class KnowledgeGraph:
    def __init__(self) -> None:
        self.graph = nx.DiGraph()

    def add_node(self, node_id: str, node_type: str, **attrs: Any) -> None:
        self.graph.add_node(node_id, node_type=node_type, **attrs)

    def add_edge(
        self,
        source: str,
        target: str,
        edge_type: EdgeType,
        weight: float = 1.0,
        **metadata: Any,
    ) -> None:
        self.graph.add_edge(
            source, target, edge_type=edge_type.value, weight=weight, **metadata
        )

    def module_import_graph(self) -> nx.DiGraph:
        subgraph = nx.DiGraph()
        for node_id, attrs in self.graph.nodes(data=True):
            if attrs.get("node_type") == "module":
                subgraph.add_node(node_id, **attrs)
        for source, target, attrs in self.graph.edges(data=True):
            if attrs.get("edge_type") != EdgeType.IMPORTS.value:
                continue
            source_type = self.graph.nodes.get(source, {}).get("node_type")
            target_type = self.graph.nodes.get(target, {}).get("node_type")
            if source_type == "module" and target_type == "module":
                subgraph.add_edge(source, target, **attrs)
        return subgraph

    def pagerank(self, module_import_only: bool = False) -> dict[str, float]:
        graph = self.module_import_graph() if module_import_only else self.graph
        if not graph.nodes:
            return {}
        return nx.pagerank(graph)

    def strongly_connected_components(self, module_import_only: bool = False) -> list[list[str]]:
        graph = self.module_import_graph() if module_import_only else self.graph
        scc = nx.strongly_connected_components(graph)
        return [sorted(list(comp)) for comp in scc if len(comp) > 1]

    def downstream(self, node_id: str) -> list[str]:
        if node_id not in self.graph:
            return []
        return sorted(nx.descendants(self.graph, node_id))

    def upstream(self, node_id: str) -> list[str]:
        if node_id not in self.graph:
            return []
        rev = self.graph.reverse(copy=False)
        return sorted(nx.descendants(rev, node_id))

    def serialize(self, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data = nx.node_link_data(self.graph, edges="edges")
        output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, input_path: Path) -> "KnowledgeGraph":
        kg = cls()
        data = json.loads(input_path.read_text(encoding="utf-8"))
        edge_key = "edges" if "edges" in data else "links"
        kg.graph = nx.node_link_graph(data, directed=True, edges=edge_key)
        return kg
