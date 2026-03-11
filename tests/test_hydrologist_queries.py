from __future__ import annotations

from pathlib import Path

from src.agents.hydrologist import HydrologistAgent


def test_hydrologist_upstream_downstream_and_alias_normalization(mini_repo_copy: Path) -> None:
    graph, _ = HydrologistAgent().run(mini_repo_copy)
    agent = HydrologistAgent(graph)

    upstream_short = agent.get_upstream("orders")
    upstream_full = agent.get_upstream("dataset::orders")
    downstream = agent.get_downstream("orders_raw")

    assert upstream_short == upstream_full
    assert all("depth" in item for item in upstream_short)
    assert all("node_type" in item for item in upstream_short)
    assert all("analysis_method" in item for item in upstream_short)
    assert downstream


def test_hydrologist_structured_answers(mini_repo_copy: Path) -> None:
    graph, _ = HydrologistAgent().run(mini_repo_copy)
    agent = HydrologistAgent(graph)

    feeds = agent.what_feeds_table("orders")
    depends = agent.what_depends_on_output("orders_raw")
    blast = agent.blast_radius("orders_raw")

    assert feeds["target"].startswith("dataset::")
    assert "direct_upstream" in feeds
    assert "full_upstream" in feeds
    assert "evidence" in feeds

    assert depends["target"].startswith("dataset::")
    assert "direct_downstream" in depends
    assert "full_downstream" in depends
    assert "evidence" in depends

    assert isinstance(blast, dict)
    assert blast["target"].startswith("dataset::")
    assert "impacted_nodes" in blast
    assert blast["impact_count"] == len(blast["impacted_nodes"])
    assert "evidence" in blast


def test_hydrologist_empty_structures_for_unknown_dataset(mini_repo_copy: Path) -> None:
    graph, _ = HydrologistAgent().run(mini_repo_copy)
    agent = HydrologistAgent(graph)

    feeds = agent.what_feeds_table("not_real_dataset")
    depends = agent.what_depends_on_output("not_real_dataset")
    blast = agent.blast_radius("not_real_dataset")

    assert feeds["target"] == "dataset::not_real_dataset"
    assert feeds["direct_upstream"] == []
    assert feeds["full_upstream"] == []
    assert feeds["evidence"] == []

    assert depends["target"] == "dataset::not_real_dataset"
    assert depends["direct_downstream"] == []
    assert depends["full_downstream"] == []
    assert depends["evidence"] == []

    assert isinstance(blast, dict)
    assert blast["target"] == "dataset::not_real_dataset"
    assert blast["impacted_nodes"] == []
    assert blast["impact_count"] == 0
    assert blast["evidence"] == []


def test_hydrologist_pipeline_impact_report_for_pipeline_node(mini_repo_copy: Path) -> None:
    graph, _ = HydrologistAgent().run(mini_repo_copy)
    agent = HydrologistAgent(graph)

    report = agent.pipeline_impact_report("pipeline::extract_task")
    impacted_nodes = [entry["node"] for entry in report["impacted_nodes"]]

    assert report["target"] == "pipeline::extract_task"
    assert report["target_node_type"] == "pipeline"
    assert report["impact_count"] >= 1
    assert "pipeline::transform_task" in impacted_nodes
    assert report["summary"]["node_type_counts"].get("pipeline", 0) >= 1


def test_hydrologist_assigns_domain_specific_transformation_types(mini_repo_copy: Path) -> None:
    graph, _ = HydrologistAgent().run(mini_repo_copy)
    transformation_types = [
        str(attrs.get("transformation_type", ""))
        for _, attrs in graph.graph.nodes(data=True)
        if attrs.get("node_type") == "transformation"
    ]

    assert any(t.startswith("sql_") for t in transformation_types)
    assert any(t.startswith("python_") for t in transformation_types)
