from __future__ import annotations

from pathlib import Path

from src.analyzers import sql_lineage as sql_lineage_module
from src.analyzers.dag_config_parser import DAGConfigAnalyzer
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer


def test_tree_sitter_analyzer_extracts_structure(mini_repo_copy: Path) -> None:
    analyzer = TreeSitterAnalyzer()
    module = analyzer.analyze_module(mini_repo_copy / "pipeline.py", mini_repo_copy)

    assert module.language == "python"
    assert "helpers" in module.imports
    assert "helpers" in module.resolved_imports
    assert "build_orders" in module.public_functions
    assert "PipelineJob(BaseJob)" in module.classes
    assert module.class_inheritance == {"PipelineJob": ["BaseJob"]}


def test_tree_sitter_analyzer_routes_and_extracts_multilanguage_structures(mini_repo_copy: Path) -> None:
    analyzer = TreeSitterAnalyzer()

    py_file = mini_repo_copy / "jobs" / "relative_decorated.py"
    py_file.parent.mkdir(parents=True, exist_ok=True)
    py_file.write_text(
        "\n".join(
            [
                "from ..helpers import transform_orders",
                "",
                "@task_decorator",
                "def load_data(source_path: str) -> str:",
                "    return transform_orders(source_path)",
                "",
                "class Worker(BaseWorker):",
                "    @classmethod",
                "    def run(cls, value):",
                "        return load_data(value)",
            ]
        ),
        encoding="utf-8",
    )
    py_analysis = analyzer.analyze_module(py_file, mini_repo_copy)
    assert py_analysis.language == "python"
    assert "..helpers.transform_orders" in py_analysis.imports
    assert "helpers.transform_orders" in py_analysis.resolved_imports
    assert "load_data" in py_analysis.public_functions
    assert "Worker(BaseWorker)" in py_analysis.classes
    assert py_analysis.class_inheritance["Worker"] == ["BaseWorker"]
    assert any(sig.startswith("load_data(") for sig in py_analysis.function_signatures.values())
    assert py_analysis.function_decorators.get("load_data") == ["task_decorator"]
    assert py_analysis.function_decorators.get("Worker.run") == ["classmethod"]

    sql_analysis = analyzer.analyze_module(mini_repo_copy / "model.sql", mini_repo_copy)
    assert sql_analysis.language == "sql"
    assert "select" in sql_analysis.sql_query_structure
    assert "cte" in sql_analysis.sql_query_structure
    assert "orders_raw" in sql_analysis.sql_table_references

    yaml_analysis = analyzer.analyze_module(mini_repo_copy / "schema.yml", mini_repo_copy)
    assert yaml_analysis.language == "yaml"
    assert "models" in yaml_analysis.public_functions
    assert any(path.startswith("models[]") for path in yaml_analysis.yaml_key_hierarchy)


def test_tree_sitter_analyzer_gracefully_handles_unparseable_files(mini_repo_copy: Path) -> None:
    analyzer = TreeSitterAnalyzer()
    broken = mini_repo_copy / "broken.py"
    broken.write_text(
        "\n".join(
            [
                "def broken(",
                "    return 1",
            ]
        ),
        encoding="utf-8",
    )
    analysis = analyzer.analyze_module(broken, mini_repo_copy)
    assert analysis.language == "python"
    assert analysis.parse_issues
    assert analysis.function_signatures == {}
    assert analysis.skipped or "syntax errors detected; returning partial extraction" in analysis.parse_issues


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
    assert deps[0].statement_operation == "read"
    assert deps[0].dialect
    assert deps[0].line_range[0] >= 1
    assert deps[0].line_range[1] >= deps[0].line_range[0]


def test_python_dataflow_analyzer_handles_keyword_args_and_dynamic_refs(mini_repo_copy: Path) -> None:
    analyzer = PythonDataFlowAnalyzer()
    io_file = mini_repo_copy / "keyword_io_job.py"
    io_file.write_text(
        "\n".join(
            [
                "import pandas as pd",
                "from sqlalchemy import text",
                'query = "select * from orders_raw"',
                "table_name = 'silver_orders'",
                "session.execute(statement=text(query))",
                "spark.read.table(name=table_name)",
                "df.write.saveAsTable(table_name)",
                'suffix = "orders.csv"',
                'pd.read_csv(filepath_or_buffer=f"data/{suffix}")',
            ]
        ),
        encoding="utf-8",
    )
    events = analyzer.extract_from_file(io_file, mini_repo_copy)
    observed = {(event.flow_type, event.dataset, event.storage_type, event.unresolved) for event in events}

    assert ("CONSUMES", "orders_raw", "table", False) in observed
    assert ("CONSUMES", "silver_orders", "table", False) in observed
    assert ("PRODUCES", "silver_orders", "table", False) in observed
    assert ("CONSUMES", PythonDataFlowAnalyzer.DYNAMIC_REFERENCE, "file", True) in observed


def test_sql_lineage_analyzer_ignores_unused_ctes_and_falls_back_for_dbt_refs(mini_repo_copy: Path) -> None:
    analyzer = SQLLineageAnalyzer()
    chained_sql = mini_repo_copy / "chained_model.sql"
    chained_sql.write_text(
        "\n".join(
            [
                "with unused_cte as (",
                "  select * from ignored_table",
                "),",
                "orders as (",
                "  select * from orders_raw",
                "),",
                "joined as (",
                "  select o.* from orders o join dim_customers c on o.customer_id = c.customer_id",
                ")",
                "select * from joined",
            ]
        ),
        encoding="utf-8",
    )
    deps = analyzer.extract_from_file(chained_sql, mini_repo_copy)
    assert len(deps) == 1
    assert sorted(deps[0].source_tables) == ["dim_customers", "orders_raw"]
    assert "ignored_table" not in deps[0].source_tables
    assert deps[0].target_tables == ["chained_model"]

    dbt_sql = mini_repo_copy / "dbt_ref_model.sql"
    dbt_sql.write_text(
        "select * from {{ ref('analytics_pkg', 'orders_model') }} join {{ source('raw', 'customers') }}",
        encoding="utf-8",
    )
    dbt_deps = analyzer.extract_from_file(dbt_sql, mini_repo_copy)
    assert len(dbt_deps) == 1
    assert sorted(dbt_deps[0].source_tables) == ["analytics_pkg.orders_model", "raw.customers"]
    assert dbt_deps[0].target_tables == ["dbt_ref_model"]
    assert dbt_deps[0].statement_operation == "read"
    assert dbt_deps[0].dialect == "dbt_template"


def test_sql_lineage_analyzer_supports_multi_dialect_fallback(monkeypatch, mini_repo_copy: Path) -> None:
    analyzer = SQLLineageAnalyzer()
    sql_file = mini_repo_copy / "dialect_fallback.sql"
    sql_file.write_text("select * from orders_raw", encoding="utf-8")
    parse_calls: list[str] = []

    def fake_parse(sql_text: str, read: str):
        parse_calls.append(read)
        if read in {"postgres", "bigquery"}:
            raise ValueError(f"dialect failed: {read}")
        return [sql_lineage_module.exp.select("*").from_("orders_raw")]

    monkeypatch.setattr(sql_lineage_module.sqlglot, "parse", fake_parse)
    deps = analyzer.extract_from_file(sql_file, mini_repo_copy)

    assert len(analyzer.dialects) >= 3
    assert parse_calls[:3] == ["postgres", "bigquery", "snowflake"]
    assert len(deps) == 1
    assert deps[0].source_tables == ["orders_raw"]
    assert deps[0].target_tables == ["dialect_fallback"]
    assert deps[0].dialect == "snowflake"


def test_sql_lineage_analyzer_extracts_nested_cte_subquery_sources(mini_repo_copy: Path) -> None:
    analyzer = SQLLineageAnalyzer()
    sql_file = mini_repo_copy / "complex_nested_model.sql"
    sql_file.write_text(
        "\n".join(
            [
                "with base_orders as (",
                "  select * from raw.orders",
                "),",
                "with_subquery as (",
                "  select * from (",
                "    with customer_seed as (",
                "      select * from raw.customers",
                "    )",
                "    select customer_id from customer_seed",
                "  ) c",
                "  join base_orders o on c.customer_id = o.customer_id",
                ")",
                "select * from with_subquery",
            ]
        ),
        encoding="utf-8",
    )

    deps = analyzer.extract_from_file(sql_file, mini_repo_copy)
    assert len(deps) == 1
    assert sorted(deps[0].source_tables) == ["raw.customers", "raw.orders"]
    assert deps[0].target_tables == ["complex_nested_model"]
    assert deps[0].line_range[0] >= 1
    assert deps[0].line_range[1] >= deps[0].line_range[0]


def test_sql_lineage_analyzer_marks_write_operations(mini_repo_copy: Path) -> None:
    analyzer = SQLLineageAnalyzer()
    sql_file = mini_repo_copy / "write_operation.sql"
    sql_file.write_text(
        "\n".join(
            [
                "insert into mart.orders_enriched",
                "select * from staging.orders_raw",
            ]
        ),
        encoding="utf-8",
    )

    deps = analyzer.extract_from_file(sql_file, mini_repo_copy)
    assert len(deps) == 1
    assert deps[0].statement_operation == "write"
    assert deps[0].source_tables == ["staging.orders_raw"]
    assert deps[0].target_tables == ["mart.orders_enriched"]


def test_sql_lineage_analyzer_logs_and_skips_unparseable_sql(caplog, mini_repo_copy: Path) -> None:
    analyzer = SQLLineageAnalyzer()
    sql_file = mini_repo_copy / "broken.sql"
    sql_file.write_text("select from ??? totally_bad_sql", encoding="utf-8")

    with caplog.at_level("WARNING", logger="src.analyzers.sql_lineage"):
        deps = analyzer.extract_from_file(sql_file, mini_repo_copy)

    assert deps == []
    assert any("Skipping unparseable SQL file" in record.message for record in caplog.records)


def test_dag_config_analyzer_extracts_yaml_and_airflow_edges(mini_repo_copy: Path) -> None:
    analyzer = DAGConfigAnalyzer()
    yaml_edges = analyzer.parse(mini_repo_copy / "schema.yml", mini_repo_copy)
    py_edges = analyzer.parse_airflow_python(mini_repo_copy / "dag.py", mini_repo_copy)

    yaml_pairs = {(edge.source, edge.target) for edge in yaml_edges}
    py_pairs = {(edge.source, edge.target) for edge in py_edges}

    assert ("orders_raw", "model") in yaml_pairs
    assert ("extract", "transform") in yaml_pairs
    assert ("extract_task", "transform_task") in py_pairs


def test_dag_config_analyzer_handles_airflow_list_dependencies(mini_repo_copy: Path) -> None:
    analyzer = DAGConfigAnalyzer()
    dag_file = mini_repo_copy / "dag_list.py"
    dag_file.write_text(
        "\n".join(
            [
                "[extract_a, extract_b] >> transform_task",
                "load_task << [transform_task, validate_task]",
                "group.set_downstream([publish_task, archive_task])",
            ]
        ),
        encoding="utf-8",
    )
    edges = analyzer.parse_airflow_python(dag_file, mini_repo_copy)
    pairs = {(edge.source, edge.target) for edge in edges}

    assert ("extract_a", "transform_task") in pairs
    assert ("extract_b", "transform_task") in pairs
    assert ("transform_task", "load_task") in pairs
    assert ("validate_task", "load_task") in pairs
    assert ("group", "publish_task") in pairs
    assert ("group", "archive_task") in pairs
