from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any

from src.analyzers.dag_config_parser import DAGConfigAnalyzer
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.graph.data_lineage_graph import DataLineageGraph
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import EdgeType, TraceEvent


class HydrologistAgent:
    def __init__(self, lineage_graph: KnowledgeGraph | None = None) -> None:
        self.sql = SQLLineageAnalyzer()
        self.cfg = DAGConfigAnalyzer()
        self.pyflow = PythonDataFlowAnalyzer()
        self.lineage_graph: KnowledgeGraph | None = lineage_graph

    def run(
        self,
        repo_path: Path,
        lineage_graph: KnowledgeGraph | None = None,
        include_files: set[str] | None = None,
    ) -> tuple[DataLineageGraph, list[TraceEvent]]:
        graph = DataLineageGraph()
        if lineage_graph is not None:
            graph.graph.update(lineage_graph.graph)
        trace: list[TraceEvent] = []
        unresolved_dynamic_refs = 0

        sql_files = [
            p
            for p in repo_path.rglob("*.sql")
            if self._include(repo_path, p, include_files) and not self._ignored(p)
        ]
        yaml_files = [
            p for p in (list(repo_path.rglob("*.yml")) + list(repo_path.rglob("*.yaml")))
            if self._include(repo_path, p, include_files) and not self._ignored(p)
        ]
        py_files = [
            p
            for p in repo_path.rglob("*.py")
            if self._include(repo_path, p, include_files) and not self._ignored(p)
        ]
        ipynb_files = [
            p
            for p in repo_path.rglob("*.ipynb")
            if self._include(repo_path, p, include_files) and not self._ignored(p)
        ]

        for file in sql_files:
            for dep in self.sql.extract_from_file(file, repo_path):
                t_id = f"transform::{dep.source_file}::{abs(hash(dep.statement))}"
                graph.add_node(
                    t_id,
                    "transformation",
                    source_file=dep.source_file,
                    transformation_type="sql",
                    sql_query_if_applicable=dep.statement,
                    line_range=dep.line_range,
                )
                for src in dep.source_tables:
                    dsrc = f"dataset::{src}"
                    graph.add_node(dsrc, "dataset", name=src, storage_type="table")
                    graph.add_edge(
                        dsrc,
                        t_id,
                        EdgeType.CONSUMES,
                        source_file=dep.source_file,
                        line_range=dep.line_range,
                        analysis_method="sqlglot",
                    )
                for tgt in dep.target_tables:
                    dtgt = f"dataset::{tgt}"
                    graph.add_node(dtgt, "dataset", name=tgt, storage_type="table")
                    graph.add_edge(
                        t_id,
                        dtgt,
                        EdgeType.PRODUCES,
                        source_file=dep.source_file,
                        line_range=dep.line_range,
                        analysis_method="sqlglot",
                    )

        for file in yaml_files:
            edges = self.cfg.parse(file, repo_path)
            for e in edges:
                src = f"config::{e.source}"
                tgt = f"config::{e.target}"
                graph.add_node(src, "config", name=e.source, source_file=e.source_file)
                graph.add_node(tgt, "config", name=e.target, source_file=e.source_file)
                graph.add_edge(
                    src, tgt, EdgeType.CONFIGURES, source_file=e.source_file, analysis_method="yaml"
                )

        for file in py_files:
            for e in self.cfg.parse_airflow_python(file, repo_path):
                src = f"pipeline::{e.source}"
                tgt = f"pipeline::{e.target}"
                graph.add_node(src, "pipeline", name=e.source, source_file=e.source_file)
                graph.add_node(tgt, "pipeline", name=e.target, source_file=e.source_file)
                graph.add_edge(
                    src, tgt, EdgeType.CONFIGURES, source_file=e.source_file, analysis_method="python_ast"
                )
            py_events = self.pyflow.extract_from_file(file, repo_path)
            if py_events:
                transform_id = f"transform::{py_events[0].source_file}"
                graph.add_node(
                    transform_id,
                    "transformation",
                    source_file=py_events[0].source_file,
                    transformation_type="python",
                )
            for event in py_events:
                d_id, dataset_name = self._dataset_node_id(
                    event.dataset,
                    source_file=event.source_file,
                    line_range=event.line_range,
                    unresolved=event.unresolved,
                )
                graph.add_node(
                    d_id,
                    "dataset",
                    name=dataset_name,
                    storage_type=event.storage_type,
                    unresolved_dynamic_reference=event.unresolved,
                )
                edge_type = EdgeType.CONSUMES if event.flow_type == "CONSUMES" else EdgeType.PRODUCES
                if edge_type == EdgeType.CONSUMES:
                    graph.add_edge(
                        d_id,
                        transform_id,
                        edge_type,
                        source_file=event.source_file,
                        line_range=event.line_range,
                        analysis_method=event.analysis_method,
                        unresolved_dynamic_reference=event.unresolved,
                    )
                else:
                    graph.add_edge(
                        transform_id,
                        d_id,
                        edge_type,
                        source_file=event.source_file,
                        line_range=event.line_range,
                        analysis_method=event.analysis_method,
                        unresolved_dynamic_reference=event.unresolved,
                    )
                if event.unresolved:
                    unresolved_dynamic_refs += 1
        for file in ipynb_files:
            self._extract_notebook_io(file, repo_path, graph)

        trace.append(
            TraceEvent(
                agent="hydrologist",
                action="lineage_graph_built",
                evidence={
                    "nodes": graph.graph.number_of_nodes(),
                    "edges": graph.graph.number_of_edges(),
                    "sql_files": len(sql_files),
                    "yaml_files": len(yaml_files),
                    "python_files": len(py_files),
                    "notebooks": len(ipynb_files),
                    "dynamic_references_unresolved": unresolved_dynamic_refs,
                },
                confidence="medium",
            )
        )
        self.lineage_graph = graph
        return graph, trace

    def attach_graph(self, lineage_graph: KnowledgeGraph) -> None:
        self.lineage_graph = lineage_graph

    def get_upstream(self, dataset_name: str) -> list[dict[str, Any]]:
        graph = self._require_graph()
        target = self._resolve_dataset_node(graph, dataset_name)
        if not target:
            return []
        return self._traverse_dependencies(graph, target, direction="upstream")

    def get_downstream(self, dataset_name: str) -> list[dict[str, Any]]:
        graph = self._require_graph()
        target = self._resolve_dataset_node(graph, dataset_name)
        if not target:
            return []
        return self._traverse_dependencies(graph, target, direction="downstream")

    def what_feeds_table(self, dataset_name: str) -> dict[str, Any]:
        graph = self._require_graph()
        target = self._resolve_dataset_node(graph, dataset_name) or self._normalize_dataset_name(dataset_name)
        direct = self._direct_neighbors(graph, target, direction="upstream") if target in graph.graph else []
        full = self.get_upstream(dataset_name) if target in graph.graph else []
        return {
            "target": target,
            "direct_upstream": direct,
            "full_upstream": full,
            "evidence": self._collect_evidence(full),
        }

    def what_depends_on_output(self, dataset_name: str) -> dict[str, Any]:
        graph = self._require_graph()
        target = self._resolve_dataset_node(graph, dataset_name) or self._normalize_dataset_name(dataset_name)
        direct = self._direct_neighbors(graph, target, direction="downstream") if target in graph.graph else []
        full = self.get_downstream(dataset_name) if target in graph.graph else []
        return {
            "target": target,
            "direct_downstream": direct,
            "full_downstream": full,
            "evidence": self._collect_evidence(full),
        }

    def blast_radius(
        self,
        dataset_name_or_graph: str | KnowledgeGraph,
        graph_or_node: str | KnowledgeGraph | None = None,
    ) -> dict[str, Any] | list[str]:
        # Backward compatibility:
        # - blast_radius(graph, node_id) -> list[str]
        # New behavior:
        # - blast_radius(dataset_name) -> structured dict
        if isinstance(dataset_name_or_graph, KnowledgeGraph):
            graph_obj = dataset_name_or_graph
            node_id = str(graph_or_node or "")
            if not node_id:
                return []
            if isinstance(graph_obj, DataLineageGraph):
                return graph_obj.blast_radius(node_id)
            return graph_obj.downstream(node_id)

        if isinstance(graph_or_node, KnowledgeGraph):
            node_id = str(dataset_name_or_graph)
            graph_obj = graph_or_node
            if isinstance(graph_obj, DataLineageGraph):
                return graph_obj.blast_radius(node_id)
            return graph_obj.downstream(node_id)

        dataset_name = str(dataset_name_or_graph)
        graph_obj = self._require_graph()
        target = self._resolve_dataset_node(graph_obj, dataset_name) or self._normalize_dataset_name(dataset_name)
        impacted = self.get_downstream(dataset_name) if target in graph_obj.graph else []
        return {
            "target": target,
            "impacted_nodes": impacted,
            "impact_count": len(impacted),
            "evidence": self._collect_evidence(impacted),
        }

    def _extract_notebook_io(self, file_path: Path, repo_path: Path, graph: DataLineageGraph) -> None:
        rel = str(file_path.relative_to(repo_path))
        try:
            notebook = json.loads(file_path.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            return
        cells = notebook.get("cells", [])
        text = "\n".join(
            "".join(cell.get("source", [])) for cell in cells if isinstance(cell, dict) and cell.get("cell_type") == "code"
        )
        if not text.strip():
            return
        transform_id = f"transform::{rel}"
        graph.add_node(transform_id, "transformation", source_file=rel, transformation_type="notebook")
        for token in ["read_csv(", "read_parquet(", "read_sql(", "to_csv(", "to_parquet(", "write("]:
            idx = text.find(token)
            if idx < 0:
                continue
            line = text[:idx].count("\n") + 1
            dataset_id, dataset_name = self._dataset_node_id(
                PythonDataFlowAnalyzer.DYNAMIC_REFERENCE,
                source_file=rel,
                line_range=(line, line),
                unresolved=True,
            )
            edge_type = EdgeType.CONSUMES if token.startswith("read_") else EdgeType.PRODUCES
            graph.add_node(
                dataset_id,
                "dataset",
                name=dataset_name,
                storage_type="file",
                unresolved_dynamic_reference=True,
            )
            if edge_type == EdgeType.CONSUMES:
                graph.add_edge(
                    dataset_id,
                    transform_id,
                    edge_type,
                    source_file=rel,
                    line_range=(line, line),
                    analysis_method="notebook_text_scan",
                    unresolved_dynamic_reference=True,
                )
            else:
                graph.add_edge(
                    transform_id,
                    dataset_id,
                    edge_type,
                    source_file=rel,
                    line_range=(line, line),
                    analysis_method="notebook_text_scan",
                    unresolved_dynamic_reference=True,
                )

    def find_sources(self, graph: KnowledgeGraph) -> list[str]:
        if isinstance(graph, DataLineageGraph):
            return graph.find_sources()
        return sorted([n for n in graph.graph.nodes if graph.graph.in_degree(n) == 0])

    def find_sinks(self, graph: KnowledgeGraph) -> list[str]:
        if isinstance(graph, DataLineageGraph):
            return graph.find_sinks()
        return sorted([n for n in graph.graph.nodes if graph.graph.out_degree(n) == 0])

    def _include(self, repo_path: Path, path: Path, include_files: set[str] | None) -> bool:
        if include_files is None:
            return True
        return str(path.relative_to(repo_path)) in include_files

    def _ignored(self, path: Path) -> bool:
        ignored = {".git", "__pycache__", ".venv", "venv", "node_modules", ".cartography", "dist", "build"}
        return any(part in ignored for part in path.parts)

    def _dataset_node_id(
        self,
        dataset: str,
        *,
        source_file: str,
        line_range: tuple[int, int],
        unresolved: bool,
    ) -> tuple[str, str]:
        if not unresolved:
            return f"dataset::{dataset}", dataset
        start, end = line_range
        contextual = f"{dataset} @ {source_file}:{start}-{end}"
        node_id = f"dataset::{dataset}::{source_file}:{start}-{end}"
        return node_id, contextual

    def _require_graph(self) -> KnowledgeGraph:
        if self.lineage_graph is None:
            raise ValueError("Hydrologist lineage graph is not attached.")
        return self.lineage_graph

    def _normalize_dataset_name(self, dataset_name: str) -> str:
        text = dataset_name.strip()
        if not text:
            return "dataset::"
        if text.startswith("dataset::"):
            return text
        return f"dataset::{text}"

    def _resolve_dataset_node(self, graph: KnowledgeGraph, dataset_name: str) -> str | None:
        normalized = self._normalize_dataset_name(dataset_name)
        if normalized in graph.graph:
            return normalized

        search = dataset_name.strip().lower()
        if not search:
            return None
        normalized_lower = normalized.lower()

        exact_prefix_matches: list[str] = []
        loose_matches: list[str] = []
        for node_id, attrs in graph.graph.nodes(data=True):
            if attrs.get("node_type") != "dataset":
                continue
            node_lower = str(node_id).lower()
            name_lower = str(attrs.get("name", "")).lower()
            if node_lower == normalized_lower or name_lower == search:
                return str(node_id)
            if node_lower.startswith(f"{normalized_lower}::"):
                exact_prefix_matches.append(str(node_id))
                continue
            if search in node_lower or search in name_lower:
                loose_matches.append(str(node_id))

        if exact_prefix_matches:
            return sorted(exact_prefix_matches)[0]
        if loose_matches:
            return sorted(loose_matches)[0]
        return None

    def _traverse_dependencies(
        self,
        graph: KnowledgeGraph,
        start_node: str,
        direction: str,
    ) -> list[dict[str, Any]]:
        if start_node not in graph.graph:
            return []

        visited: set[str] = {start_node}
        queue: deque[tuple[str, int]] = deque([(start_node, 0)])
        results: list[dict[str, Any]] = []

        while queue:
            current, depth = queue.popleft()
            neighbors = (
                list(graph.graph.predecessors(current))
                if direction == "upstream"
                else list(graph.graph.successors(current))
            )
            for neighbor in sorted(neighbors):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                next_depth = depth + 1
                entry = self._node_entry_with_evidence(
                    graph=graph,
                    origin=current,
                    neighbor=neighbor,
                    direction=direction,
                    depth=next_depth,
                )
                results.append(entry)
                queue.append((neighbor, next_depth))

        results.sort(key=lambda item: (int(item.get("depth", 0)), str(item.get("node", ""))))
        return results

    def _direct_neighbors(self, graph: KnowledgeGraph, start_node: str, direction: str) -> list[dict[str, Any]]:
        if start_node not in graph.graph:
            return []
        neighbors = (
            sorted(graph.graph.predecessors(start_node))
            if direction == "upstream"
            else sorted(graph.graph.successors(start_node))
        )
        return [
            self._node_entry_with_evidence(
                graph=graph,
                origin=start_node,
                neighbor=neighbor,
                direction=direction,
                depth=1,
            )
            for neighbor in neighbors
        ]

    def _node_entry_with_evidence(
        self,
        graph: KnowledgeGraph,
        origin: str,
        neighbor: str,
        direction: str,
        depth: int,
    ) -> dict[str, Any]:
        node_attrs = dict(graph.graph.nodes.get(neighbor, {}))
        if direction == "upstream":
            edge_attrs = dict(graph.graph.get_edge_data(neighbor, origin, default={}))
        else:
            edge_attrs = dict(graph.graph.get_edge_data(origin, neighbor, default={}))

        line_range = edge_attrs.get("line_range") or node_attrs.get("line_range") or [0, 0]
        if isinstance(line_range, tuple):
            line_range = list(line_range)

        return {
            "node": neighbor,
            "node_type": str(node_attrs.get("node_type", "unknown")),
            "source_file": str(edge_attrs.get("source_file") or node_attrs.get("source_file") or ""),
            "line_range": line_range,
            "analysis_method": str(edge_attrs.get("analysis_method") or node_attrs.get("analysis_method") or ""),
            "depth": depth,
            "transformation_type": str(node_attrs.get("transformation_type", "")),
        }

    def _collect_evidence(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[str, str, str, tuple[int, int], str]] = set()
        evidence: list[dict[str, Any]] = []
        for item in entries:
            line = item.get("line_range") or [0, 0]
            if isinstance(line, tuple):
                line = list(line)
            if not isinstance(line, list) or len(line) != 2:
                line = [0, 0]
            key = (
                str(item.get("node", "")),
                str(item.get("source_file", "")),
                str(item.get("analysis_method", "")),
                (int(line[0]), int(line[1])),
                str(item.get("transformation_type", "")),
            )
            if key in seen:
                continue
            seen.add(key)
            evidence.append(
                {
                    "node": key[0],
                    "source_file": key[1],
                    "analysis_method": key[2],
                    "line_range": [key[3][0], key[3][1]],
                    "transformation_type": key[4],
                }
            )
        return evidence
