from __future__ import annotations

from pathlib import Path

from src.agents.hydrologist import HydrologistAgent
from src.agents.navigator import NavigatorAgent, NavigatorLangGraphAgent
from src.agents.semanticist import SemanticistAgent
from src.agents.surveyor import SurveyorAgent


def test_surveyor_builds_module_graph(mini_repo_copy: Path) -> None:
    graph, modules, trace = SurveyorAgent().run(mini_repo_copy)

    assert "pipeline.py" in modules
    assert graph.graph.has_edge("pipeline.py", "helpers.py")
    assert trace[-1].action == "module_graph_built"


def test_hydrologist_builds_lineage_graph(mini_repo_copy: Path) -> None:
    graph, trace = HydrologistAgent().run(mini_repo_copy)

    assert "dataset::orders_raw" in graph.graph
    assert "dataset::data/orders.csv" in graph.graph
    assert graph.find_sources()
    assert graph.find_sinks()
    assert trace[-1].action == "lineage_graph_built"


def test_semanticist_enriches_modules_without_llm(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    _, modules, _ = SurveyorAgent().run(mini_repo_copy)
    enriched, trace = SemanticistAgent().run(mini_repo_copy, modules)

    assert enriched["pipeline.py"].purpose_statement
    assert enriched["pipeline.py"].domain_cluster.startswith("domain_")
    assert trace[-1].agent == "semanticist"


def test_navigator_langgraph_returns_evidence(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    module_graph, modules, _ = SurveyorAgent().run(mini_repo_copy)
    modules, _ = SemanticistAgent().run(mini_repo_copy, modules)
    for path, module in modules.items():
        module_graph.graph.nodes[path].update(module.model_dump())
    lineage_graph, _ = HydrologistAgent().run(mini_repo_copy)

    agent = NavigatorLangGraphAgent(NavigatorAgent(module_graph, lineage_graph))
    result = agent.run("explain_module", "pipeline.py")

    assert result["module"] == "pipeline.py"
    assert result["evidence"]["source_file"] == "pipeline.py"
    assert "line_range" in result["evidence"]
    assert "analysis_method" in result["evidence"]
