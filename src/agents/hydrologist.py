from __future__ import annotations

import json
from pathlib import Path

from src.analyzers.dag_config_parser import DAGConfigAnalyzer
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.graph.data_lineage_graph import DataLineageGraph
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import EdgeType, TraceEvent


class HydrologistAgent:
    def __init__(self) -> None:
        self.sql = SQLLineageAnalyzer()
        self.cfg = DAGConfigAnalyzer()
        self.pyflow = PythonDataFlowAnalyzer()

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
                        t_id,
                        dsrc,
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
                d_id = f"dataset::{event.dataset}"
                graph.add_node(d_id, "dataset", name=event.dataset, storage_type=event.storage_type)
                edge_type = EdgeType.CONSUMES if event.flow_type == "CONSUMES" else EdgeType.PRODUCES
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
        return graph, trace

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
            dataset = PythonDataFlowAnalyzer.DYNAMIC_REFERENCE
            edge_type = EdgeType.CONSUMES if token.startswith("read_") else EdgeType.PRODUCES
            ds = f"dataset::{dataset}"
            graph.add_node(ds, "dataset", name=dataset, storage_type="file")
            graph.add_edge(
                transform_id,
                ds,
                edge_type,
                source_file=rel,
                line_range=(line, line),
                analysis_method="notebook_text_scan",
                unresolved_dynamic_reference=True,
            )

    def blast_radius(self, graph: KnowledgeGraph, node_id: str) -> list[str]:
        if isinstance(graph, DataLineageGraph):
            return graph.blast_radius(node_id)
        return graph.downstream(node_id)

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
