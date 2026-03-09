from __future__ import annotations

from src.graph.knowledge_graph import KnowledgeGraph


class DataLineageGraph(KnowledgeGraph):
    def blast_radius(self, node_id: str) -> list[str]:
        return self.downstream(node_id)

    def find_sources(self) -> list[str]:
        return sorted([node for node in self.graph.nodes if self.graph.in_degree(node) == 0])

    def find_sinks(self) -> list[str]:
        return sorted([node for node in self.graph.nodes if self.graph.out_degree(node) == 0])
