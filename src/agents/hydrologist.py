from __future__ import annotations

from collections.abc import Callable
import json
import re
from collections import Counter
from collections import deque
from pathlib import Path
from typing import Any

from src.analyzers.dag_config_parser import DAGConfigAnalyzer
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.graph.data_lineage_graph import DataLineageGraph
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import DatasetNode, EdgeType, TraceEvent, TransformationNode


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
        progress_callback: Callable[[str], None] | None = None,
    ) -> tuple[DataLineageGraph, list[TraceEvent]]:
        graph = DataLineageGraph()
        if lineage_graph is not None:
            graph.graph.update(lineage_graph.graph)
        trace: list[TraceEvent] = []
        unresolved_dynamic_refs = 0
        file_errors: list[dict[str, str]] = []

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

        if progress_callback:
            progress_callback(
                "Hydrologist: analyzing "
                f"{len(sql_files)} SQL, {len(yaml_files)} YAML, {len(py_files)} Python, {len(ipynb_files)} notebooks."
            )

        for file in sql_files:
            rel = str(file.relative_to(repo_path))
            try:
                for dep in self.sql.extract_from_file(file, repo_path):
                    t_id = f"transform::{dep.source_file}::{abs(hash(dep.statement))}"
                    transformation_type = self._classify_sql_transformation(dep.statement, dep.statement_operation)
                    graph.add_transformation_node(
                        t_id,
                        TransformationNode(
                            source_datasets=dep.source_tables,
                            target_datasets=dep.target_tables,
                            source_file=dep.source_file,
                            transformation_type=transformation_type,
                            sql_query_if_applicable=dep.statement,
                            line_range=dep.line_range,
                            statement_operation=dep.statement_operation,
                            dialect=dep.dialect,
                        ),
                    )
                    for src in dep.source_tables:
                        dsrc = f"dataset::{src}"
                        graph.add_dataset_node(dsrc, DatasetNode(name=src, storage_type="table"))
                        graph.add_consumes_edge(
                            dsrc,
                            t_id,
                            source_file=dep.source_file,
                            line_range=dep.line_range,
                            analysis_method="sqlglot",
                            transformation_type=transformation_type,
                        )
                    for tgt in dep.target_tables:
                        dtgt = f"dataset::{tgt}"
                        graph.add_dataset_node(dtgt, DatasetNode(name=tgt, storage_type="table"))
                        graph.add_produces_edge(
                            t_id,
                            dtgt,
                            source_file=dep.source_file,
                            line_range=dep.line_range,
                            analysis_method="sqlglot",
                            transformation_type=transformation_type,
                        )
            except Exception as exc:
                file_errors.append({"file": rel, "phase": "sql", "error": str(exc)})
                if progress_callback:
                    progress_callback(f"Hydrologist: skipping SQL file {rel} due to error: {exc}")
                continue

        for file in yaml_files:
            rel = str(file.relative_to(repo_path))
            try:
                edges = self.cfg.parse(file, repo_path)
                for e in edges:
                    src = f"config::{e.source}"
                    tgt = f"config::{e.target}"
                    graph.add_node(src, "config", name=e.source, source_file=e.source_file)
                    graph.add_node(tgt, "config", name=e.target, source_file=e.source_file)
                    graph.add_configures_edge(
                        src,
                        tgt,
                        source_file=e.source_file,
                        line_range=e.line_range,
                        analysis_method="yaml",
                        transformation_type=e.transformation_type,
                    )
            except Exception as exc:
                file_errors.append({"file": rel, "phase": "yaml", "error": str(exc)})
                if progress_callback:
                    progress_callback(f"Hydrologist: skipping YAML file {rel} due to error: {exc}")
                continue

        for file in py_files:
            rel = str(file.relative_to(repo_path))
            try:
                for e in self.cfg.parse_airflow_python(file, repo_path):
                    src = f"pipeline::{e.source}"
                    tgt = f"pipeline::{e.target}"
                    graph.add_node(src, "pipeline", name=e.source, source_file=e.source_file)
                    graph.add_node(tgt, "pipeline", name=e.target, source_file=e.source_file)
                    graph.add_configures_edge(
                        src,
                        tgt,
                        source_file=e.source_file,
                        line_range=e.line_range,
                        analysis_method="python_ast",
                        transformation_type=e.transformation_type,
                    )
                py_events = self.pyflow.extract_from_file(file, repo_path)
                if py_events:
                    transform_id = f"transform::{py_events[0].source_file}"
                    transformation_type = self._classify_python_transformation(py_events)
                    min_line = min(event.line_range[0] for event in py_events)
                    max_line = max(event.line_range[1] for event in py_events)
                    graph.add_transformation_node(
                        transform_id,
                        TransformationNode(
                            source_file=py_events[0].source_file,
                            transformation_type=transformation_type,
                            line_range=(min_line, max_line),
                        ),
                    )
                for event in py_events:
                    d_id, dataset_name = self._dataset_node_id(
                        event.dataset,
                        source_file=event.source_file,
                        line_range=event.line_range,
                        unresolved=event.unresolved,
                    )
                    graph.add_dataset_node(
                        d_id,
                        DatasetNode(
                            name=dataset_name,
                            storage_type=event.storage_type,
                            unresolved_dynamic_reference=event.unresolved,
                        ),
                    )
                    edge_type = EdgeType.CONSUMES if event.flow_type == "CONSUMES" else EdgeType.PRODUCES
                    if edge_type == EdgeType.CONSUMES:
                        graph.add_consumes_edge(
                            d_id,
                            transform_id,
                            source_file=event.source_file,
                            line_range=event.line_range,
                            analysis_method=event.analysis_method,
                            unresolved_dynamic_reference=event.unresolved,
                            transformation_type=transformation_type,
                        )
                    else:
                        graph.add_produces_edge(
                            transform_id,
                            d_id,
                            source_file=event.source_file,
                            line_range=event.line_range,
                            analysis_method=event.analysis_method,
                            unresolved_dynamic_reference=event.unresolved,
                            transformation_type=transformation_type,
                        )
                    if event.unresolved:
                        unresolved_dynamic_refs += 1
            except Exception as exc:
                file_errors.append({"file": rel, "phase": "python", "error": str(exc)})
                if progress_callback:
                    progress_callback(f"Hydrologist: skipping Python file {rel} due to error: {exc}")
                continue
        for file in ipynb_files:
            rel = str(file.relative_to(repo_path))
            try:
                self._extract_notebook_io(file, repo_path, graph)
            except Exception as exc:
                file_errors.append({"file": rel, "phase": "notebook", "error": str(exc)})
                if progress_callback:
                    progress_callback(f"Hydrologist: skipping notebook {rel} due to error: {exc}")
                continue

        sources = graph.find_sources()
        sinks = graph.find_sinks()
        transformation_type_counts = self._transformation_type_counts(graph)
        pipeline_nodes = self._pipeline_nodes(graph)
        top_pipeline_impacts = self._top_pipeline_impact_summaries(graph, pipeline_nodes, limit=10)
        graph.graph.graph["hydrologist_insights"] = {
            "source_count": len(sources),
            "sink_count": len(sinks),
            "transformation_type_counts": transformation_type_counts,
            "top_pipeline_impacts": top_pipeline_impacts,
        }

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
                    "failed_files": len(file_errors),
                    "dynamic_references_unresolved": unresolved_dynamic_refs,
                    "source_count": len(sources),
                    "sink_count": len(sinks),
                    "transformation_type_counts": transformation_type_counts,
                    "pipeline_impact_reports": len(top_pipeline_impacts),
                },
                confidence="medium",
            )
        )
        if file_errors:
            trace.append(
                TraceEvent(
                    agent="hydrologist",
                    action="files_skipped_on_error",
                    evidence={
                        "failed_file_count": len(file_errors),
                        "failed_files": file_errors[:100],
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

    def pipeline_impact_report(self, dataset_or_node: str) -> dict[str, Any]:
        graph = self._require_graph()
        target = self._resolve_lineage_node(graph, dataset_or_node)
        if not target:
            if dataset_or_node.strip().startswith(("dataset::", "pipeline::", "config::", "transform::")):
                unknown_target = dataset_or_node.strip()
            else:
                unknown_target = self._normalize_dataset_name(dataset_or_node)
            return {
                "target": unknown_target,
                "target_node_type": "unknown",
                "direct_downstream": [],
                "impacted_nodes": [],
                "impact_count": 0,
                "summary": {
                    "max_depth": 0,
                    "node_type_counts": {},
                    "transformation_type_counts": {},
                    "impacted_datasets": [],
                    "terminal_sinks": [],
                    "source_files": [],
                    "related_pipeline_nodes": [],
                },
                "evidence": [],
            }
        return self._pipeline_impact_report_for_node(graph, target)

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
        read_tokens = {"read_csv(", "read_parquet(", "read_sql("}
        write_tokens = {"to_csv(", "to_parquet(", "write("}
        token_hits: list[tuple[str, int]] = []
        for token in sorted(read_tokens | write_tokens):
            idx = text.find(token)
            if idx < 0:
                continue
            line = text[:idx].count("\n") + 1
            token_hits.append((token, line))

        has_read = any(token in read_tokens for token, _ in token_hits)
        has_write = any(token in write_tokens for token, _ in token_hits)
        line_range = (
            min((line for _, line in token_hits), default=0),
            max((line for _, line in token_hits), default=0),
        )
        transform_id = f"transform::{rel}"
        transformation_type = self._classify_notebook_transformation(has_read=has_read, has_write=has_write)
        graph.add_transformation_node(
            transform_id,
            TransformationNode(
                source_file=rel,
                transformation_type=transformation_type,
                line_range=line_range,
            ),
        )
        for token, line in token_hits:
            dataset_id, dataset_name = self._dataset_node_id(
                PythonDataFlowAnalyzer.DYNAMIC_REFERENCE,
                source_file=rel,
                line_range=(line, line),
                unresolved=True,
            )
            edge_type = EdgeType.CONSUMES if token in read_tokens else EdgeType.PRODUCES
            storage_type = "table" if token == "read_sql(" else "file"
            graph.add_dataset_node(
                dataset_id,
                DatasetNode(
                    name=dataset_name,
                    storage_type=storage_type,
                    unresolved_dynamic_reference=True,
                ),
            )
            if edge_type == EdgeType.CONSUMES:
                graph.add_consumes_edge(
                    dataset_id,
                    transform_id,
                    source_file=rel,
                    line_range=(line, line),
                    analysis_method="notebook_text_scan",
                    unresolved_dynamic_reference=True,
                    transformation_type=transformation_type,
                )
            else:
                graph.add_produces_edge(
                    transform_id,
                    dataset_id,
                    source_file=rel,
                    line_range=(line, line),
                    analysis_method="notebook_text_scan",
                    unresolved_dynamic_reference=True,
                    transformation_type=transformation_type,
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

    def _resolve_lineage_node(self, graph: KnowledgeGraph, dataset_or_node: str) -> str | None:
        raw = dataset_or_node.strip()
        if not raw:
            return None
        if raw in graph.graph:
            return raw
        dataset_match = self._resolve_dataset_node(graph, raw)
        if dataset_match:
            return dataset_match
        lowered = raw.lower()
        exact_matches = [str(node_id) for node_id in graph.graph.nodes if str(node_id).lower() == lowered]
        if exact_matches:
            return sorted(exact_matches)[0]
        suffix_matches = [str(node_id) for node_id in graph.graph.nodes if str(node_id).lower().endswith(f"::{lowered}")]
        if suffix_matches:
            return sorted(suffix_matches)[0]
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
            "transformation_type": str(
                edge_attrs.get("transformation_type")
                or node_attrs.get("transformation_type")
                or ""
            ),
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

    def _classify_sql_transformation(self, statement: str, operation: str) -> str:
        sql = statement.strip().lower()
        if sql.startswith("merge"):
            return "sql_merge_upsert"
        if sql.startswith("insert"):
            return "sql_insert"
        if sql.startswith("update"):
            return "sql_update"
        if sql.startswith("delete"):
            return "sql_delete"
        if sql.startswith("create") and " as select " in re.sub(r"\s+", " ", sql):
            return "sql_ctas"
        if operation == "write":
            return "sql_write"
        return "sql_model_select"

    def _classify_python_transformation(self, events: list[Any]) -> str:
        if not events:
            return "python_unknown"
        consumes_table = any(e.flow_type == "CONSUMES" and e.storage_type == "table" for e in events)
        consumes_file = any(e.flow_type == "CONSUMES" and e.storage_type == "file" for e in events)
        produces_table = any(e.flow_type == "PRODUCES" and e.storage_type == "table" for e in events)
        produces_file = any(e.flow_type == "PRODUCES" and e.storage_type == "file" for e in events)

        if consumes_table and produces_table:
            return "python_table_transform"
        if consumes_file and produces_table:
            return "python_file_to_table_load"
        if consumes_table and produces_file:
            return "python_table_to_file_export"
        if consumes_file and produces_file:
            return "python_file_transform"
        if produces_table:
            return "python_table_writer"
        if produces_file:
            return "python_file_writer"
        if consumes_table:
            return "python_table_reader"
        if consumes_file:
            return "python_file_reader"
        return "python_unknown"

    def _classify_notebook_transformation(self, *, has_read: bool, has_write: bool) -> str:
        if has_read and has_write:
            return "notebook_etl"
        if has_read:
            return "notebook_ingestion"
        if has_write:
            return "notebook_export"
        return "notebook_analysis"

    def _pipeline_nodes(self, graph: KnowledgeGraph) -> list[str]:
        return sorted(
            str(node_id)
            for node_id, attrs in graph.graph.nodes(data=True)
            if attrs.get("node_type") in {"pipeline", "config"}
        )

    def _transformation_type_counts(self, graph: KnowledgeGraph) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for _, attrs in graph.graph.nodes(data=True):
            if attrs.get("node_type") != "transformation":
                continue
            transformation_type = str(attrs.get("transformation_type") or "unknown").strip() or "unknown"
            counts[transformation_type] += 1
        return dict(sorted(counts.items()))

    def _pipeline_impact_report_for_node(self, graph: KnowledgeGraph, target: str) -> dict[str, Any]:
        impacted = self._traverse_dependencies(graph, target, direction="downstream")
        summary = self._impact_summary(graph, impacted)
        return {
            "target": target,
            "target_node_type": str(graph.graph.nodes.get(target, {}).get("node_type", "unknown")),
            "direct_downstream": self._direct_neighbors(graph, target, direction="downstream"),
            "impacted_nodes": impacted,
            "impact_count": len(impacted),
            "summary": summary,
            "evidence": self._collect_evidence(impacted),
        }

    def _impact_summary(self, graph: KnowledgeGraph, impacted: list[dict[str, Any]]) -> dict[str, Any]:
        node_type_counts: Counter[str] = Counter()
        transformation_type_counts: Counter[str] = Counter()
        source_files: set[str] = set()
        impacted_datasets: list[str] = []
        terminal_sinks: list[str] = []
        max_depth = 0

        for item in impacted:
            node = str(item.get("node", ""))
            node_type = str(item.get("node_type", "unknown")) or "unknown"
            node_type_counts[node_type] += 1

            depth = max(0, int(item.get("depth", 0)))
            max_depth = max(max_depth, depth)

            source_file = str(item.get("source_file", "")).strip()
            if source_file:
                source_files.add(source_file)

            if node_type == "dataset":
                impacted_datasets.append(node)
            if node in graph.graph and graph.graph.out_degree(node) == 0:
                terminal_sinks.append(node)

            transformation_type = str(item.get("transformation_type", "")).strip()
            if transformation_type:
                transformation_type_counts[transformation_type] += 1

        return {
            "max_depth": max_depth,
            "node_type_counts": dict(sorted(node_type_counts.items())),
            "transformation_type_counts": dict(sorted(transformation_type_counts.items())),
            "impacted_datasets": sorted(set(impacted_datasets)),
            "terminal_sinks": sorted(set(terminal_sinks)),
            "source_files": sorted(source_files),
            "related_pipeline_nodes": self._related_pipeline_nodes(graph, source_files),
        }

    def _related_pipeline_nodes(self, graph: KnowledgeGraph, source_files: set[str]) -> list[str]:
        if not source_files:
            return []
        related: list[str] = []
        for node_id, attrs in graph.graph.nodes(data=True):
            if attrs.get("node_type") not in {"pipeline", "config"}:
                continue
            source_file = str(attrs.get("source_file", "")).strip()
            if source_file and source_file in source_files:
                related.append(str(node_id))
        return sorted(set(related))

    def _top_pipeline_impact_summaries(
        self,
        graph: KnowledgeGraph,
        pipeline_nodes: list[str],
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for node_id in pipeline_nodes:
            report = self._pipeline_impact_report_for_node(graph, node_id)
            summary = report["summary"]
            summaries.append(
                {
                    "target": report["target"],
                    "target_node_type": report["target_node_type"],
                    "impact_count": report["impact_count"],
                    "max_depth": int(summary.get("max_depth", 0)),
                    "node_type_counts": dict(summary.get("node_type_counts", {})),
                    "terminal_sinks": list(summary.get("terminal_sinks", []))[:5],
                    "related_pipeline_nodes": list(summary.get("related_pipeline_nodes", []))[:10],
                }
            )
        summaries.sort(
            key=lambda item: (-int(item.get("impact_count", 0)), str(item.get("target", "")))
        )
        return summaries[: max(0, limit)]
