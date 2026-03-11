from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

from src.models.schemas import (
    CallsEdge,
    ConfiguresEdge,
    ConsumesEdge,
    DatasetNode,
    EdgeType,
    FunctionNode,
    GraphEdge,
    ImportsEdge,
    ModuleNode,
    NodeType,
    ProducesEdge,
    TransformationNode,
)


class KnowledgeGraph:
    _NODE_MODEL_BY_TYPE = {
        NodeType.module: ModuleNode,
        NodeType.dataset: DatasetNode,
        NodeType.function: FunctionNode,
        NodeType.transformation: TransformationNode,
    }
    _EDGE_MODEL_BY_TYPE = {
        EdgeType.IMPORTS: ImportsEdge,
        EdgeType.PRODUCES: ProducesEdge,
        EdgeType.CONSUMES: ConsumesEdge,
        EdgeType.CALLS: CallsEdge,
        EdgeType.CONFIGURES: ConfiguresEdge,
    }

    def __init__(self, graph: nx.DiGraph | None = None) -> None:
        self.graph = graph or nx.DiGraph()

    def add_module_node(self, module: ModuleNode) -> str:
        node_id = module.path
        self.add_node(node_id, NodeType.module, **module.model_dump(mode="json"))
        return node_id

    def add_dataset_node(self, node_id: str, dataset: DatasetNode) -> str:
        self.add_node(node_id, NodeType.dataset, **dataset.model_dump(mode="json"))
        return node_id

    def add_function_node(self, function: FunctionNode) -> str:
        node_id = function.qualified_name
        self.add_node(node_id, NodeType.function, **function.model_dump(mode="json"))
        return node_id

    def add_transformation_node(self, node_id: str, transformation: TransformationNode) -> str:
        self.add_node(node_id, NodeType.transformation, **transformation.model_dump(mode="json"))
        return node_id

    def add_node(self, node_id: str, node_type: str | NodeType, **attrs: Any) -> None:
        normalized_id = str(node_id).strip()
        if not normalized_id:
            raise ValueError("node_id is required.")
        normalized_type = self._normalize_node_type(node_type)
        validated_attrs = self._validate_node_attrs(normalized_id, normalized_type, attrs)
        self.graph.add_node(normalized_id, node_type=normalized_type, **validated_attrs)

    def add_imports_edge(self, source: str, target: str, weight: float = 1.0, **metadata: Any) -> None:
        self.add_edge(source, target, EdgeType.IMPORTS, weight=weight, **metadata)

    def add_produces_edge(self, source: str, target: str, weight: float = 1.0, **metadata: Any) -> None:
        self.add_edge(source, target, EdgeType.PRODUCES, weight=weight, **metadata)

    def add_consumes_edge(self, source: str, target: str, weight: float = 1.0, **metadata: Any) -> None:
        self.add_edge(source, target, EdgeType.CONSUMES, weight=weight, **metadata)

    def add_calls_edge(self, source: str, target: str, weight: float = 1.0, **metadata: Any) -> None:
        self.add_edge(source, target, EdgeType.CALLS, weight=weight, **metadata)

    def add_configures_edge(self, source: str, target: str, weight: float = 1.0, **metadata: Any) -> None:
        self.add_edge(source, target, EdgeType.CONFIGURES, weight=weight, **metadata)

    def add_typed_edge(self, edge: GraphEdge) -> None:
        self.add_edge(
            edge.source,
            edge.target,
            edge.edge_type,
            weight=edge.weight,
            **edge.metadata,
        )

    def add_edge(
        self,
        source: str,
        target: str,
        edge_type: EdgeType | str,
        weight: float = 1.0,
        **metadata: Any,
    ) -> None:
        source_id = str(source).strip()
        target_id = str(target).strip()
        if not source_id or not target_id:
            raise ValueError("Edge source and target are required.")

        normalized_type, parsed_type = self._normalize_edge_type(edge_type)
        if parsed_type is None:
            self.graph.add_edge(
                source_id,
                target_id,
                edge_type=normalized_type,
                weight=self._coerce_weight(weight),
                **metadata,
            )
            return

        edge_model = self._EDGE_MODEL_BY_TYPE[parsed_type](
            source=source_id,
            target=target_id,
            weight=weight,
            metadata=metadata,
        )
        payload = edge_model.model_dump(mode="json")
        self.graph.add_edge(
            source_id,
            target_id,
            edge_type=payload["edge_type"],
            weight=payload["weight"],
            **payload["metadata"],
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

    def to_dict(self) -> dict[str, Any]:
        validated = self._validated_copy()
        data = nx.node_link_data(validated.graph, edges="edges")
        data["graph_schema_version"] = 1
        return data

    def serialize(self, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KnowledgeGraph":
        kg = cls()
        edge_key = "edges" if "edges" in data else "links"
        graph = nx.node_link_graph(data, directed=True, edges=edge_key)
        kg.graph.graph.update(dict(graph.graph))

        for node_id, attrs in graph.nodes(data=True):
            payload = dict(attrs)
            raw_type = payload.pop("node_type", payload.pop("type", ""))
            node_type = str(raw_type).strip().lower()
            if not node_type:
                inferred = cls._infer_node_type(str(node_id), payload)
                node_type = inferred.value if inferred else ""
            if node_type:
                kg.add_node(str(node_id), node_type, **payload)
            else:
                kg.graph.add_node(str(node_id), **payload)

        for source, target, attrs in graph.edges(data=True):
            payload = dict(attrs)
            raw_edge_type = payload.pop("edge_type", "")
            weight = payload.pop("weight", 1.0)
            edge_type = str(raw_edge_type).strip().upper()
            if edge_type:
                kg.add_edge(str(source), str(target), edge_type, weight=weight, **payload)
            else:
                kg.graph.add_edge(str(source), str(target), weight=cls._coerce_weight(weight), **payload)
        return kg

    @classmethod
    def load(cls, input_path: Path) -> "KnowledgeGraph":
        data = json.loads(input_path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    @staticmethod
    def _normalize_node_type(node_type: str | NodeType) -> str:
        if isinstance(node_type, NodeType):
            return node_type.value
        return str(node_type).strip().lower()

    @classmethod
    def _parse_node_type(cls, node_type: str) -> NodeType | None:
        try:
            return NodeType(node_type)
        except ValueError:
            return None

    @classmethod
    def _validate_node_attrs(cls, node_id: str, node_type: str, attrs: dict[str, Any]) -> dict[str, Any]:
        payload = dict(attrs)
        parsed_type = cls._parse_node_type(node_type)
        if parsed_type is None:
            return payload

        if parsed_type == NodeType.module:
            payload.setdefault("path", node_id)
            payload.setdefault("language", "unknown")
        elif parsed_type == NodeType.dataset:
            payload.setdefault("name", node_id.replace("dataset::", "", 1))
        elif parsed_type == NodeType.function:
            payload.setdefault("qualified_name", node_id)
            if "::" in node_id:
                payload.setdefault("parent_module", node_id.split("::", 1)[0])
        elif parsed_type == NodeType.transformation:
            payload.setdefault("source_datasets", [])
            payload.setdefault("target_datasets", [])
            payload.setdefault("transformation_type", "unknown")
            payload.setdefault("source_file", "")

        model = cls._NODE_MODEL_BY_TYPE[parsed_type](**payload)
        return model.model_dump(mode="json")

    @staticmethod
    def _normalize_edge_type(edge_type: EdgeType | str) -> tuple[str, EdgeType | None]:
        if isinstance(edge_type, EdgeType):
            return edge_type.value, edge_type
        normalized = str(edge_type).strip().upper()
        try:
            return normalized, EdgeType(normalized)
        except ValueError:
            return normalized, None

    @staticmethod
    def _coerce_weight(weight: Any) -> float:
        try:
            parsed = float(weight)
        except (TypeError, ValueError):
            return 1.0
        return max(0.0, parsed)

    @classmethod
    def _infer_node_type(cls, node_id: str, attrs: dict[str, Any]) -> NodeType | None:
        explicit = str(attrs.get("type") or "").strip().lower()
        if explicit:
            try:
                return NodeType(explicit)
            except ValueError:
                return None
        if node_id.startswith("dataset::"):
            return NodeType.dataset
        if node_id.startswith("transform::"):
            return NodeType.transformation
        if node_id.startswith("config::") or node_id.startswith("pipeline::"):
            return None
        if "::" in node_id:
            return NodeType.function
        return None

    def _validated_copy(self) -> "KnowledgeGraph":
        validated = KnowledgeGraph()
        validated.graph.graph.update(dict(self.graph.graph))
        for node_id, attrs in self.graph.nodes(data=True):
            payload = dict(attrs)
            raw_type = payload.pop("node_type", payload.pop("type", ""))
            node_type = str(raw_type).strip().lower()
            if not node_type:
                inferred = self._infer_node_type(str(node_id), payload)
                node_type = inferred.value if inferred else ""
            if node_type:
                validated.add_node(str(node_id), node_type, **payload)
            else:
                validated.graph.add_node(str(node_id), **payload)

        for source, target, attrs in self.graph.edges(data=True):
            payload = dict(attrs)
            raw_edge_type = payload.pop("edge_type", "")
            weight = payload.pop("weight", 1.0)
            edge_type = str(raw_edge_type).strip().upper()
            if edge_type:
                validated.add_edge(str(source), str(target), edge_type, weight=weight, **payload)
            else:
                validated.graph.add_edge(
                    str(source),
                    str(target),
                    weight=self._coerce_weight(weight),
                    **payload,
                )
        return validated
