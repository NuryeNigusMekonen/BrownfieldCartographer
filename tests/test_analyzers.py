from __future__ import annotations

from pathlib import Path

from src.analyzers.dag_config_parser import DAGConfigAnalyzer
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer


def test_tree_sitter_analyzer_extracts_structure(mini_repo_copy: Path) -> None:
    analyzer = TreeSitterAnalyzer()
    module = analyzer.analyze_module(mini_repo_copy / "pipeline.py", mini_repo_copy)

    assert module.language == "python"
    assert "helpers" in module.imports
    assert "build_orders" in module.public_functions
    assert "PipelineJob(BaseJob)" in module.classes
    assert module.class_inheritance == {"PipelineJob": ["BaseJob"]}


def test_python_dataflow_analyzer_extracts_io(mini_repo_copy: Path) -> None:
    analyzer = PythonDataFlowAnalyzer()
    io_file = mini_repo_copy / "io_job.py"
    io_file.write_text(
        "\n".join(
            [
                "import pandas as pd",
                'SOURCE_PATH = "data/orders.csv"',
                'TARGET_PATH = "data/orders_clean.csv"',
                "df = pd.read_csv(SOURCE_PATH)",
                "result.write.csv(TARGET_PATH)",
            ]
        ),
        encoding="utf-8",
    )
    events = analyzer.extract_from_file(io_file, mini_repo_copy)

    observed = {(event.flow_type, event.dataset, event.storage_type, event.unresolved) for event in events}
    assert ("CONSUMES", "data/orders.csv", "file", False) in observed
    assert ("PRODUCES", "data/orders_clean.csv", "file", False) in observed


def test_sql_lineage_analyzer_extracts_cte_dependency(mini_repo_copy: Path) -> None:
    analyzer = SQLLineageAnalyzer()
    deps = analyzer.extract_from_file(mini_repo_copy / "model.sql", mini_repo_copy)

    assert len(deps) == 1
    assert deps[0].source_tables == ["orders_raw"]
    assert deps[0].target_tables == ["model"]


def test_dag_config_analyzer_extracts_yaml_and_airflow_edges(mini_repo_copy: Path) -> None:
    analyzer = DAGConfigAnalyzer()
    yaml_edges = analyzer.parse(mini_repo_copy / "schema.yml", mini_repo_copy)
    py_edges = analyzer.parse_airflow_python(mini_repo_copy / "dag.py", mini_repo_copy)

    yaml_pairs = {(edge.source, edge.target) for edge in yaml_edges}
    py_pairs = {(edge.source, edge.target) for edge in py_edges}

    assert ("orders_raw", "model") in yaml_pairs
    assert ("extract", "transform") in yaml_pairs
    assert ("extract_task", "transform_task") in py_pairs
