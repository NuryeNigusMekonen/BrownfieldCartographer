from __future__ import annotations

from pathlib import Path

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import DatasetNode, FunctionNode, ModuleNode, TransformationNode


def test_typed_nodes_edges_round_trip_json(tmp_path: Path) -> None:
    graph = KnowledgeGraph()

    module = ModuleNode(
        path="src/app.py",
        language="python",
        purpose_statement="Entrypoint for the job runner.",
        domain_cluster="runtime",
        change_velocity_30d=4,
        is_dead_code_candidate=False,
    )
    function = FunctionNode(
        qualified_name="src/app.py::run",
        parent_module="src/app.py",
        signature="def run() -> None",
    )
    source_dataset = DatasetNode(name="warehouse.raw_orders", storage_type="table")
    target_dataset = DatasetNode(name="warehouse.curated_orders", storage_type="table")
    transformation = TransformationNode(
        source_datasets=["warehouse.raw_orders"],
        target_datasets=["warehouse.curated_orders"],
        transformation_type="sql",
        source_file="models/orders.sql",
        line_range=(3, 24),
    )

    graph.add_module_node(module)
    graph.add_function_node(function)
    graph.add_dataset_node("dataset::warehouse.raw_orders", source_dataset)
    graph.add_dataset_node("dataset::warehouse.curated_orders", target_dataset)
    graph.add_transformation_node("transform::models/orders.sql", transformation)

    graph.add_configures_edge(module.path, function.qualified_name, analysis_method="python_ast")
    graph.add_consumes_edge(
        "dataset::warehouse.raw_orders",
        "transform::models/orders.sql",
        source_file="models/orders.sql",
    )
    graph.add_produces_edge(
        "transform::models/orders.sql",
        "dataset::warehouse.curated_orders",
        source_file="models/orders.sql",
    )

    output = tmp_path / "knowledge_graph.json"
    graph.serialize(output)
    loaded = KnowledgeGraph.load(output)

    assert loaded.graph.nodes["src/app.py"]["node_type"] == "module"
    assert loaded.graph.nodes["src/app.py"]["change_velocity_30d"] == 4
    assert loaded.graph.nodes["dataset::warehouse.raw_orders"]["node_type"] == "dataset"
    assert loaded.graph.nodes["transform::models/orders.sql"]["node_type"] == "transformation"
    assert loaded.graph.edges["dataset::warehouse.raw_orders", "transform::models/orders.sql"]["edge_type"] == "CONSUMES"
    assert loaded.graph.edges["transform::models/orders.sql", "dataset::warehouse.curated_orders"]["edge_type"] == "PRODUCES"


def test_generic_add_node_applies_schema_defaults_and_validators() -> None:
    graph = KnowledgeGraph()

    graph.add_node(
        "src/core.py",
        "module",
        language="python",
        change_velocity_30d=-8,
        comment_ratio=4.2,
    )
    attrs = graph.graph.nodes["src/core.py"]

    assert attrs["path"] == "src/core.py"
    assert attrs["change_velocity_30d"] == 0
    assert attrs["comment_ratio"] == 1.0


def test_from_dict_supports_legacy_links_and_schema_alignment() -> None:
    payload = {
        "directed": True,
        "multigraph": False,
        "graph": {},
        "nodes": [
            {"id": "dataset::raw.users", "name": "raw.users", "storage_type": "table"},
            {"id": "transform::jobs/build_users.py", "source_file": "jobs/build_users.py", "transformation_type": "python"},
        ],
        "links": [
            {"source": "dataset::raw.users", "target": "transform::jobs/build_users.py", "edge_type": "CONSUMES"}
        ],
    }

    graph = KnowledgeGraph.from_dict(payload)

    assert graph.graph.nodes["dataset::raw.users"]["node_type"] == "dataset"
    assert graph.graph.nodes["transform::jobs/build_users.py"]["node_type"] == "transformation"
    assert graph.graph.edges["dataset::raw.users", "transform::jobs/build_users.py"]["edge_type"] == "CONSUMES"


def test_graph_level_metadata_round_trip_json(tmp_path: Path) -> None:
    graph = KnowledgeGraph()
    graph.graph.graph["hydrologist_insights"] = {
        "source_count": 2,
        "sink_count": 3,
        "top_pipeline_impacts": [{"target": "pipeline::extract", "impact_count": 4}],
    }
    graph.add_node("dataset::raw.orders", "dataset", name="raw.orders", storage_type="table")

    output = tmp_path / "graph_with_metadata.json"
    graph.serialize(output)
    loaded = KnowledgeGraph.load(output)

    assert "hydrologist_insights" in loaded.graph.graph
    assert loaded.graph.graph["hydrologist_insights"]["sink_count"] == 3
