from __future__ import annotations

import json
from pathlib import Path

from src.analyzers.git_history import GitFileVelocity, GitVelocitySnapshot
from src.agents.archivist import ArchivistAgent
from src.agents.hydrologist import HydrologistAgent
from src.agents.navigator import NavigatorLangGraphAgent
from src.agents.semanticist import SemanticistAgent
from src.agents.surveyor import SurveyorAgent
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import DatasetNode, DayOneAnswer, FunctionNode, ModuleNode, TraceEvent, TransformationNode


def test_surveyor_builds_module_graph(mini_repo_copy: Path) -> None:
    graph, modules, trace = SurveyorAgent().run(mini_repo_copy)

    assert "pipeline.py" in modules
    assert graph.graph.has_edge("pipeline.py", "helpers.py")
    assert modules["pipeline.py"].pagerank_score >= 0.0
    assert "surveyor_insights" in graph.graph.graph
    assert "top_pagerank_modules" in trace[-1].evidence
    assert "import_cycle_count" in trace[-1].evidence
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


def test_semanticist_purpose_prompt_ignores_docstrings(monkeypatch) -> None:
    semanticist = SemanticistAgent()
    captured: dict[str, str] = {}

    def fake_chat(model: str, messages: list[dict[str, str]], task_type: str, temperature: float = 0.1) -> str:
        captured["model"] = model
        captured["task_type"] = task_type
        captured["system"] = messages[0]["content"]
        captured["user"] = messages[1]["content"]
        return "Purpose generated from implementation."

    monkeypatch.setattr(semanticist, "_ollama_chat", fake_chat)
    module = ModuleNode(path="pipeline.py", language="python")
    file_text = '\n'.join(
        [
            '"""THIS IS DOCSTRING ONLY."""',
            "",
            "def run_pipeline():",
            "    return load_orders()",
        ]
    )

    purpose = semanticist.generate_purpose_statement(module, file_text)
    assert purpose == "Purpose generated from implementation."
    assert captured["model"] == semanticist.budget.model_fast
    assert captured["task_type"] == "bulk_summary"
    assert "Ignore docstrings" in captured["system"]
    assert "Implementation excerpt" in captured["user"]
    assert "THIS IS DOCSTRING ONLY" not in captured["user"]


def test_semanticist_doc_drift_details_are_structured(monkeypatch) -> None:
    semanticist = SemanticistAgent()
    monkeypatch.setattr(semanticist, "_ollama_chat", lambda model, messages, task_type, temperature=0.1: None)
    file_text = '\n'.join(
        [
            '"""Exposes HTTP API endpoints for customer requests."""',
            "",
            "def transform_orders(df):",
            "    return df",
        ]
    )
    purpose = "This module runs SQL pipeline transformations and writes data into warehouse tables."
    details = semanticist.detect_doc_drift_details(file_text, purpose)

    assert isinstance(details, dict)
    assert details["drift_detected"] is True
    assert details["severity"] in {"low", "medium", "high"}
    assert isinstance(details["contradictions"], list)
    assert "docstring_excerpt" in details
    assert "analysis_method" in details


def test_semanticist_run_attaches_doc_drift_metadata(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    file_path = repo / "service.py"
    file_path.write_text(
        '\n'.join(
            [
                '"""Serves REST API requests."""',
                "",
                "def model_query():",
                "    return execute_sql()",
            ]
        ),
        encoding="utf-8",
    )
    modules = {"service.py": ModuleNode(path="service.py", language="python")}
    monkeypatch.setattr(
        SemanticistAgent,
        "_generate_purpose_with_llm",
        lambda self, path, file_text: "Builds SQL models and warehouse transformations.",
    )
    monkeypatch.setattr(
        SemanticistAgent,
        "_ollama_chat",
        lambda self, model, messages, task_type, temperature=0.1: None,
    )

    enriched, trace = SemanticistAgent().run(repo, modules)
    module = enriched["service.py"]

    assert isinstance(module.doc_drift, dict)
    assert "severity" in module.doc_drift
    assert module.doc_drift["drift_detected"] is True
    assert "[Documentation Drift Suspected]" in module.purpose_statement
    assert "doc_drift_modules" in trace[-1].evidence


def test_semanticist_model_router_uses_fast_and_synth_models() -> None:
    semanticist = SemanticistAgent()
    assert semanticist._select_model_for_task("bulk_summary") == semanticist.budget.model_fast
    assert semanticist._select_model_for_task("embedding") == semanticist.budget.model_fast
    assert semanticist._select_model_for_task("synthesis") == semanticist.budget.model_synth

    semanticist.budget.spent_tokens = semanticist.token_budget_limit + 1
    assert semanticist._select_model_for_task("synthesis") == semanticist.budget.model_fast


def test_semanticist_llm_day_one_adds_citations_when_evidence_missing(monkeypatch) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()
    module = ModuleNode(path="src/pipelines/run_job.py", language="python", change_velocity_30d=3)
    module_graph.add_module_node(module)

    lineage_graph.add_dataset_node("dataset::raw.orders", DatasetNode(name="raw.orders"))
    lineage_graph.add_transformation_node("transform::m.sql::1", TransformationNode(source_file="models/m.sql"))
    lineage_graph.add_consumes_edge(
        "dataset::raw.orders",
        "transform::m.sql::1",
        source_file="models/m.sql",
        line_range=[7, 22],
        analysis_method="sqlglot",
    )

    payload = {
        "q1_primary_ingestion": {"answer": "raw.orders", "confidence": "high", "evidence": []},
        "q2_critical_outputs": {"answer": "mart.orders", "confidence": "medium", "evidence": []},
        "q3_blast_radius": {"answer": "If src/pipelines/run_job.py fails, impacts follow.", "confidence": "medium", "evidence": []},
        "q4_logic_concentration": {"answer": "src/pipelines/run_job.py", "confidence": "medium", "evidence": []},
        "q5_change_velocity": {"answer": "src/pipelines/run_job.py changed often.", "confidence": "low", "evidence": []},
    }

    monkeypatch.setattr(
        semanticist,
        "_ollama_chat",
        lambda model, messages, task_type, temperature=0.1: json.dumps(payload) if task_type == "synthesis" else None,
    )

    answers = semanticist.answer_day_one_questions(
        modules=[module],
        top_modules=[module.path],
        sources=["dataset::raw.orders"],
        sinks=[],
        downstream_map={module.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q1_evidence = answers["q1_primary_ingestion"].evidence
    q3_evidence = answers["q3_blast_radius"].evidence
    assert q1_evidence and q1_evidence[0]["source_file"] == "models/m.sql"
    assert q1_evidence[0]["line_range"] == [7, 22]
    assert q3_evidence and q3_evidence[0]["source_file"] == "src/pipelines/run_job.py"
    assert q3_evidence[0]["line_range"] == [1, 1]


def test_semanticist_day_one_filters_operational_sql_noise(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    module = ModuleNode(path="pipeline.py", language="python")
    module_graph.add_module_node(module)

    lineage_graph.add_transformation_node(
        "transform::model.sql::1",
        TransformationNode(source_file="model.sql", transformation_type="sql_model_select"),
    )
    lineage_graph.add_dataset_node("dataset::CALL load_aws_credentials()", DatasetNode(name="CALL load_aws_credentials()"))
    lineage_graph.add_consumes_edge(
        "dataset::CALL load_aws_credentials()",
        "transform::model.sql::1",
        source_file="bootstrap.py",
        line_range=[10, 10],
        analysis_method="tree_sitter_python+sqlglot",
    )
    lineage_graph.add_dataset_node("dataset::raw.orders", DatasetNode(name="raw.orders"))
    lineage_graph.add_consumes_edge(
        "dataset::raw.orders",
        "transform::model.sql::1",
        source_file="model.sql",
        line_range=[1, 1],
        analysis_method="sqlglot",
    )
    lineage_graph.add_dataset_node("dataset::mart.orders_report", DatasetNode(name="mart.orders_report"))
    lineage_graph.add_produces_edge(
        "transform::model.sql::1",
        "dataset::mart.orders_report",
        source_file="model.sql",
        line_range=[20, 20],
        analysis_method="sqlglot",
    )

    answers = semanticist.answer_day_one_questions(
        modules=[module],
        top_modules=["pipeline.py"],
        sources=["dataset::CALL load_aws_credentials()", "dataset::raw.orders"],
        sinks=["dataset::mart.orders_report"],
        downstream_map={"pipeline.py": []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    assert "dataset::raw.orders" in answers["q1_primary_ingestion"].answer
    assert "load_aws_credentials" not in answers["q1_primary_ingestion"].answer.lower()
    assert "dataset::mart.orders_report" in answers["q2_critical_outputs"].answer


def test_semanticist_day_one_flags_deprecated_zero_blast_radius(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    deprecated = ModuleNode(path="src/legacy/__init__.py", language="python", purpose_statement="deprecated shim")
    deprecated.is_deprecated_guard = True
    module_graph.add_module_node(deprecated)

    answers = semanticist.answer_day_one_questions(
        modules=[deprecated],
        top_modules=[deprecated.path],
        sources=[],
        sinks=[],
        downstream_map={deprecated.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    blast_answer = answers["q3_blast_radius"].answer.lower()
    assert "deprecated guard module" in blast_answer
    assert "0 downstream nodes" in blast_answer


def test_semanticist_day_one_uses_entrypoint_ingestion_when_lineage_is_weak(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    connector = ModuleNode(path="src/plugins/s3_connector.py", language="python", change_velocity_30d=5, complexity_score=3.0)
    orchestrator = ModuleNode(path="src/orchestration/run_pipeline.py", language="python", change_velocity_30d=4, complexity_score=4.0)
    module_graph.add_module_node(connector)
    module_graph.add_module_node(orchestrator)

    answers = semanticist.answer_day_one_questions(
        modules=[connector, orchestrator],
        top_modules=[orchestrator.path],
        sources=[],
        sinks=[],
        downstream_map={orchestrator.path: [], connector.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q1 = answers["q1_primary_ingestion"]
    assert "src/plugins/s3_connector.py" in q1.answer or "src/orchestration/run_pipeline.py" in q1.answer
    assert q1.evidence
    assert all(item.get("analysis_method") == "module_entrypoint_ingestion_heuristic" for item in q1.evidence)
    assert q1.confidence in {"medium", "low"}


def test_semanticist_day_one_blast_radius_deprioritizes_helpers(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    helper = ModuleNode(path="src/utils/helpers.py", language="python", complexity_score=2.0, change_velocity_30d=2)
    runner = ModuleNode(path="src/pipelines/run_job.py", language="python", complexity_score=3.0, change_velocity_30d=2)
    module_graph.add_module_node(helper)
    module_graph.add_module_node(runner)

    helper_fn = FunctionNode(qualified_name="src/utils/helpers.py::format_payload", parent_module=helper.path)
    runner_fn = FunctionNode(qualified_name="src/pipelines/run_job.py::run", parent_module=runner.path)
    module_graph.add_function_node(helper_fn)
    module_graph.add_function_node(runner_fn)
    module_graph.add_calls_edge(helper_fn.qualified_name, runner_fn.qualified_name, analysis_method="python_ast")

    answers = semanticist.answer_day_one_questions(
        modules=[helper, runner],
        top_modules=[helper.path, runner.path],
        sources=[],
        sinks=[],
        downstream_map={helper.path: [f"m{i}" for i in range(6)], runner.path: [f"n{i}" for i in range(5)]},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q3 = answers["q3_blast_radius"]
    assert q3.answer.startswith("If src/pipelines/run_job.py fails")
    assert q3.evidence[0]["source_file"] == "src/pipelines/run_job.py"


def test_semanticist_day_one_blast_radius_uses_module_graph_descendants_when_map_is_sparse(
    deterministic_semantics: None,
) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    runner = ModuleNode(path="src/pipelines/run_job.py", language="python", complexity_score=2.0, change_velocity_30d=1)
    dependency = ModuleNode(path="src/core/executor.py", language="python", complexity_score=1.0, change_velocity_30d=1)
    module_graph.add_module_node(runner)
    module_graph.add_module_node(dependency)
    module_graph.add_imports_edge(runner.path, dependency.path, analysis_method="python_ast")

    answers = semanticist.answer_day_one_questions(
        modules=[runner, dependency],
        top_modules=[runner.path],
        sources=[],
        sinks=[],
        downstream_map={runner.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q3 = answers["q3_blast_radius"].answer
    assert "at least 1 downstream nodes may be impacted" in q3


def test_semanticist_day_one_confidence_high_for_verified_no_sinks(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    module_graph.add_module_node(ModuleNode(path="src/orchestrate/run.py", language="python"))

    answers = semanticist.answer_day_one_questions(
        modules=[ModuleNode(path="src/orchestrate/run.py", language="python")],
        top_modules=["src/orchestrate/run.py"],
        sources=[],
        sinks=[],
        downstream_map={"src/orchestrate/run.py": []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q2 = answers["q2_critical_outputs"]
    assert q2.confidence == "high"
    assert q2.confidence_label == "high"
    assert q2.confidence_score >= 0.75
    assert q2.confidence_factors["signal_agreement"] >= 0.8
    assert q2.confidence_factors["repo_type_fit"] >= 0.6
    assert q2.confidence_components["graph_coverage_score"] >= 0.9
    assert "lineage graph coverage is complete" in q2.confidence_reason.lower()


def test_semanticist_day_one_confidence_low_for_sparse_zero_blast(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    module_a = ModuleNode(path="src/app/main.py", language="python", complexity_score=1.0)
    module_b = ModuleNode(path="src/app/runner.py", language="python", complexity_score=1.0)
    module_graph.add_module_node(module_a)
    module_graph.add_module_node(module_b)

    answers = semanticist.answer_day_one_questions(
        modules=[module_a, module_b],
        top_modules=[module_a.path],
        sources=[],
        sinks=[],
        downstream_map={module_a.path: [], module_b.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q3 = answers["q3_blast_radius"]
    assert "0 downstream nodes" in q3.answer
    assert q3.confidence == "low"
    assert q3.confidence_label == "low"
    assert q3.confidence_score < 0.45
    assert q3.confidence_factors["signal_agreement"] <= 0.3
    assert "sparse module graph" in q3.confidence_reason.lower()


def test_semanticist_day_one_confidence_high_for_logic_centrality_signals(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    orchestrator = ModuleNode(
        path="src/orchestration/run_pipeline.py",
        language="python",
        complexity_score=6.0,
        change_velocity_30d=4,
        pagerank_score=0.25,
    )
    plugin = ModuleNode(
        path="src/plugins/source_connector.py",
        language="python",
        complexity_score=5.0,
        change_velocity_30d=3,
        pagerank_score=0.18,
    )
    module_graph.add_module_node(orchestrator)
    module_graph.add_module_node(plugin)
    orchestrator_fn = FunctionNode(
        qualified_name="src/orchestration/run_pipeline.py::run",
        parent_module=orchestrator.path,
    )
    plugin_fn = FunctionNode(
        qualified_name="src/plugins/source_connector.py::execute",
        parent_module=plugin.path,
    )
    module_graph.add_function_node(orchestrator_fn)
    module_graph.add_function_node(plugin_fn)
    module_graph.add_calls_edge(orchestrator_fn.qualified_name, plugin_fn.qualified_name, analysis_method="python_ast")

    answers = semanticist.answer_day_one_questions(
        modules=[orchestrator, plugin],
        top_modules=[orchestrator.path, plugin.path],
        sources=[],
        sinks=[],
        downstream_map={orchestrator.path: [plugin.path], plugin.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q4 = answers["q4_logic_concentration"]
    assert q4.confidence == "high"
    assert q4.confidence_label == "high"
    assert q4.confidence_score >= 0.75
    assert q4.confidence_factors["heuristic_reliability"] >= 0.8
    assert q4.confidence_reason


def test_semanticist_day_one_confidence_medium_for_git_velocity_signals(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    module = ModuleNode(
        path="src/runtime/job_runner.py",
        language="python",
        complexity_score=2.0,
        change_velocity_30d=2,
        pagerank_score=0.1,
    )
    module_graph.add_module_node(module)

    answers = semanticist.answer_day_one_questions(
        modules=[module],
        top_modules=[module.path],
        sources=[],
        sinks=[],
        downstream_map={module.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q5 = answers["q5_change_velocity"]
    assert q5.confidence == "medium"
    assert q5.confidence_label == "medium"
    assert 0.45 <= q5.confidence_score < 0.75
    assert q5.confidence_factors["signal_agreement"] >= 0.6
    assert "git-frequency signals only" in q5.confidence_reason.lower()


def test_semanticist_day_one_logic_excludes_tests_and_migrations(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    test_mod = ModuleNode(path="tests/test_pipeline.py", language="python", complexity_score=10.0, change_velocity_30d=12)
    migration_mod = ModuleNode(path="migrations/versions/001_init.py", language="python", complexity_score=9.0, change_velocity_30d=8)
    logic_mod = ModuleNode(
        path="src/pipelines/build_metrics.py",
        language="python",
        complexity_score=4.0,
        change_velocity_30d=6,
        pagerank_score=0.15,
    )
    sql_mod = ModuleNode(
        path="src/models/marts/fact_revenue.sql",
        language="sql",
        complexity_score=5.0,
        change_velocity_30d=3,
        pagerank_score=0.11,
    )
    for module in [test_mod, migration_mod, logic_mod, sql_mod]:
        module_graph.add_module_node(module)

    answers = semanticist.answer_day_one_questions(
        modules=[test_mod, migration_mod, logic_mod, sql_mod],
        top_modules=[logic_mod.path, test_mod.path],
        sources=[],
        sinks=[],
        downstream_map={logic_mod.path: [sql_mod.path], test_mod.path: [], migration_mod.path: [], sql_mod.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q4 = answers["q4_logic_concentration"]
    assert "tests/test_pipeline.py" not in q4.answer
    assert "migrations/versions/001_init.py" not in q4.answer
    assert all(
        item.get("source_file") not in {"tests/test_pipeline.py", "migrations/versions/001_init.py"}
        for item in q4.evidence
    )


def test_semanticist_day_one_filters_ingestion_from_tests_and_migrations(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()
    module_graph.add_module_node(ModuleNode(path="src/meltano/cli/state.py", language="python"))

    lineage_graph.add_transformation_node(
        "transform::core.sql::1",
        TransformationNode(source_file="src/meltano/core/state_store.py", transformation_type="python_sql"),
    )
    lineage_graph.add_dataset_node("dataset::test", DatasetNode(name="test"))
    lineage_graph.add_consumes_edge(
        "dataset::test",
        "transform::core.sql::1",
        source_file="tests/meltano/core/test_sqlalchemy.py",
        line_range=[10, 20],
        analysis_method="tree_sitter_python",
    )
    lineage_graph.add_dataset_node("dataset::embed_tokens", DatasetNode(name="embed_tokens"))
    lineage_graph.add_consumes_edge(
        "dataset::embed_tokens",
        "transform::core.sql::1",
        source_file="src/meltano/migrations/versions/23ea52e6d784_add_resource_type_to_embed_token.py",
        line_range=[39, 39],
        analysis_method="tree_sitter_python",
    )
    lineage_graph.add_dataset_node("dataset::raw_events", DatasetNode(name="raw_events"))
    lineage_graph.add_consumes_edge(
        "dataset::raw_events",
        "transform::core.sql::1",
        source_file="src/meltano/core/state_store.py",
        line_range=[44, 60],
        analysis_method="tree_sitter_python+sqlglot",
    )

    answers = semanticist.answer_day_one_questions(
        modules=[ModuleNode(path="src/meltano/cli/state.py", language="python")],
        top_modules=["src/meltano/cli/state.py"],
        sources=["dataset::test", "dataset::embed_tokens", "dataset::raw_events"],
        sinks=[],
        downstream_map={"src/meltano/cli/state.py": []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q1 = answers["q1_primary_ingestion"]
    assert "dataset::raw_events" in q1.answer
    assert "dataset::test" not in q1.answer
    assert "embed_tokens" not in q1.answer.lower()
    assert all(
        "tests/" not in str(item.get("source_file", "")).lower()
        and "migrations/" not in str(item.get("source_file", "")).lower()
        for item in q1.evidence
    )


def test_semanticist_day_one_logic_deprioritizes_support_init_modules(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    support_mod = ModuleNode(
        path="src/meltano/core/utils/__init__.py",
        language="python",
        complexity_score=7.0,
        change_velocity_30d=9,
        pagerank_score=0.4,
    )
    core_logic = ModuleNode(
        path="src/meltano/core/plugin/singer/tap.py",
        language="python",
        complexity_score=5.0,
        change_velocity_30d=4,
        pagerank_score=0.2,
    )
    module_graph.add_module_node(support_mod)
    module_graph.add_module_node(core_logic)

    answers = semanticist.answer_day_one_questions(
        modules=[support_mod, core_logic],
        top_modules=[support_mod.path, core_logic.path],
        sources=[],
        sinks=[],
        downstream_map={support_mod.path: [], core_logic.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q4 = answers["q4_logic_concentration"]
    assert "src/meltano/core/plugin/singer/tap.py" in q4.answer
    assert "src/meltano/core/utils/__init__.py" not in q4.answer


def test_semanticist_day_one_velocity_prioritizes_runtime_over_docs_when_sparse(deterministic_semantics: None) -> None:
    semanticist = SemanticistAgent()
    module_graph = KnowledgeGraph()
    lineage_graph = KnowledgeGraph()

    runtime = ModuleNode(
        path="src/meltano/core/plugin_invoker.py",
        language="python",
        complexity_score=4.0,
        change_velocity_30d=1,
        pagerank_score=0.22,
    )
    docs = ModuleNode(path="docs/sidebars.js", language="javascript", complexity_score=1.0, change_velocity_30d=1)
    script = ModuleNode(path="scripts/alembic_freeze.py", language="python", complexity_score=1.0, change_velocity_30d=1)
    for module in [runtime, docs, script]:
        module_graph.add_module_node(module)

    answers = semanticist.answer_day_one_questions(
        modules=[runtime, docs, script],
        top_modules=[runtime.path],
        sources=[],
        sinks=[],
        downstream_map={runtime.path: [], docs.path: [], script.path: []},
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    q5 = answers["q5_change_velocity"]
    assert "src/meltano/core/plugin_invoker.py" in q5.answer


def test_archivist_day_one_brief_is_narrative_and_sanitized(tmp_path: Path) -> None:
    evidence = [{"analysis_method": "sqlglot", "source_file": "models/staging/orders.sql", "line_range": [1, 20]}]
    day_one = {
        "q1_primary_ingestion": DayOneAnswer(
            question_id="q1_primary_ingestion",
            answer="dataset::CALL load_aws_credentials(), dataset::raw.orders, dataset::stg.orders",
            confidence="high",
            evidence=evidence,
        )
    }

    brief_path = ArchivistAgent(tmp_path).generate_onboarding_brief(day_one)
    content = brief_path.read_text(encoding="utf-8")

    q1_section = content.split("## 1) Primary Data Ingestion Path", maxsplit=1)[1].split("## 2)", maxsplit=1)[0]
    assert "Data enters through source-aligned datasets such as raw.orders, stg.orders" in q1_section
    assert "through staging, intermediate, dimensional, and reporting models" in q1_section
    assert "Key sources:" in q1_section
    assert "- raw.orders" in q1_section
    assert "- stg.orders" in q1_section
    assert "load_aws_credentials" not in q1_section.lower()
    assert "dataset::raw.orders" not in q1_section
    assert "Confidence: high" in q1_section
    assert f"- {json.dumps(evidence)}" in q1_section
    assert q1_section.index("Key sources:") < q1_section.index("Confidence: high") < q1_section.index("Evidence:")


def test_archivist_ingestion_module_mode_uses_entrypoint_wording(tmp_path: Path) -> None:
    evidence = [
        {"analysis_method": "module_entrypoint_ingestion_heuristic", "source_file": "src/meltano/cli/elt.py", "line_range": [1, 1]},
        {"analysis_method": "module_entrypoint_ingestion_heuristic", "source_file": "src/meltano/cli/state.py", "line_range": [1, 1]},
    ]
    day_one = {
        "q1_primary_ingestion": DayOneAnswer(
            question_id="q1_primary_ingestion",
            answer="src/meltano/cli/elt.py, src/meltano/cli/state.py",
            confidence="medium",
            evidence=evidence,
        )
    }

    brief_path = ArchivistAgent(tmp_path).generate_onboarding_brief(day_one)
    content = brief_path.read_text(encoding="utf-8")
    q1_section = content.split("## 1) Primary Data Ingestion Path", maxsplit=1)[1].split("## 2)", maxsplit=1)[0]
    assert "Ingestion likely starts from entrypoint modules such as src/meltano/cli/elt.py, src/meltano/cli/state.py" in q1_section
    assert "datasets such as src/meltano/cli/elt.py" not in q1_section


def test_archivist_blast_radius_explanation_includes_count_and_module(tmp_path: Path) -> None:
    day_one = {
        "q3_blast_radius": DayOneAnswer(
            question_id="q3_blast_radius",
            answer="If src/pipelines/dagster_assets.py fails, at least 5 downstream nodes may be impacted.",
            confidence="high",
            evidence=[{"analysis_method": "module_graph_descendants", "source_file": "src/pipelines/dagster_assets.py", "line_range": [1, 1]}],
        )
    }

    brief_path = ArchivistAgent(tmp_path).generate_onboarding_brief(day_one)
    content = brief_path.read_text(encoding="utf-8")
    q3_section = content.split("## 3) Blast Radius of Critical Module Failure", maxsplit=1)[1].split("## 4)", maxsplit=1)[0]

    assert (
        "If src/pipelines/dagster_assets.py fails, at least 5 downstream modules are in the dependency path"
        in q3_section
    )
    assert "based on the module dependency graph." in q3_section


def test_archivist_blast_radius_zero_uses_uncertainty_wording(tmp_path: Path) -> None:
    day_one = {
        "q3_blast_radius": DayOneAnswer(
            question_id="q3_blast_radius",
            answer="If src/meltano/core/plugin_invoker.py fails, static import graph currently shows 0 downstream nodes.",
            confidence="medium",
            evidence=[{"analysis_method": "module_graph_descendants", "source_file": "src/meltano/core/plugin_invoker.py", "line_range": [1, 1]}],
        )
    }

    brief_path = ArchivistAgent(tmp_path).generate_onboarding_brief(day_one)
    content = brief_path.read_text(encoding="utf-8")
    q3_section = content.split("## 3) Blast Radius of Critical Module Failure", maxsplit=1)[1].split("## 4)", maxsplit=1)[0]

    assert "shows 0 downstream modules" in q3_section
    assert "operational impact may be higher" in q3_section
    assert "at least 0 downstream modules" not in q3_section


def test_archivist_python_repo_low_outputs_uses_operational_wording(tmp_path: Path) -> None:
    day_one = {
        "q2_critical_outputs": DayOneAnswer(
            question_id="q2_critical_outputs",
            answer="No obvious output dataset detected.",
            confidence="low",
            evidence=[{"analysis_method": "lineage_graph_sinks", "source_file": "src/meltano/cli/elt.py", "line_range": [1, 1]}],
        )
    }

    brief_path = ArchivistAgent(tmp_path).generate_onboarding_brief(day_one)
    content = brief_path.read_text(encoding="utf-8")
    q2_section = content.split("## 2) Critical Output Datasets/Endpoints", maxsplit=1)[1].split("## 3)", maxsplit=1)[0]

    assert "No strong terminal dataset sinks were detected" in q2_section
    assert "plugin runs, state, and job metadata" in q2_section


def test_archivist_python_profile_explanation_mentions_orchestration(tmp_path: Path) -> None:
    day_one = {
        "q4_logic_concentration": DayOneAnswer(
            question_id="q4_logic_concentration",
            answer="src/orchestration/run.py, src/plugins/source_connector.py",
            confidence="high",
            evidence=[
                {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/orchestration/run.py", "line_range": [1, 1]},
                {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/plugins/source_connector.py", "line_range": [1, 1]},
            ],
        )
    }

    brief_path = ArchivistAgent(tmp_path).generate_onboarding_brief(day_one)
    content = brief_path.read_text(encoding="utf-8")
    q4_section = content.split("## 4) Business Logic Concentration", maxsplit=1)[1].split("## 5)", maxsplit=1)[0]
    assert "orchestration behavior, connector execution, and pipeline control flow" in q4_section


def test_archivist_codebase_uses_required_high_velocity_section(tmp_path: Path) -> None:
    modules = {
        "src/runtime/run.py": ModuleNode(path="src/runtime/run.py", language="python", change_velocity_30d=3),
    }
    snapshot = GitVelocitySnapshot(
        time_window_days=90,
        history_status="complete",
        history_note="Velocity is based on full git commit history for the selected time window.",
        files=(
            GitFileVelocity(
                path="docs/README.md",
                commit_count=9,
                last_commit_timestamp="2026-03-10T10:00:00+00:00",
            ),
            GitFileVelocity(
                path="src/runtime/run.py",
                commit_count=6,
                last_commit_timestamp="2026-03-09T09:00:00+00:00",
            ),
        ),
        commit_events_scanned=20,
    )
    path = ArchivistAgent(tmp_path).generate_codebase_md(
        modules=modules,
        top_modules=["src/runtime/run.py"],
        scc=[],
        sources=[],
        sinks=[],
        git_velocity_snapshot=snapshot,
    )
    content = path.read_text(encoding="utf-8")
    assert "## High-Velocity Files" in content
    assert "analysis_method=git_log_frequency" in content
    assert "time_window_days=90" in content
    assert "docs/README.md" in content


def test_archivist_trace_rows_include_analysis_method_and_evidence_sources(tmp_path: Path) -> None:
    archivist = ArchivistAgent(tmp_path)
    events = [
        TraceEvent(
            agent="surveyor",
            action="module_graph_built",
            evidence={
                "top_high_velocity_files": [{"path": "src/runtime/run.py", "change_count": 4}],
                "failed_files": [{"file": "broken.py", "error": "parser"}],
            },
            confidence="high",
        ),
        TraceEvent(
            agent="semanticist",
            action="purpose_statements_generated",
            evidence={
                "model_usage_counts": {"llama3.2:3b": 2},
            },
            confidence="medium",
        ),
    ]

    trace_path = archivist.write_trace(events)
    rows = [
        json.loads(line)
        for line in trace_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert len(rows) == 2
    for row in rows:
        assert row["timestamp"]
        assert row["confidence"] in {"low", "medium", "high"}
        assert "analysis_method" in row["evidence"]
        assert isinstance(row["evidence"].get("evidence_sources", []), list)
    assert rows[0]["evidence"]["analysis_method"] == "static"
    assert "src/runtime/run.py" in rows[0]["evidence"]["evidence_sources"]
    assert "broken.py" in rows[0]["evidence"]["evidence_sources"]
    assert rows[1]["evidence"]["analysis_method"] == "hybrid_llm_static"


def test_archivist_brief_uses_onboarding_relevant_velocity_heading(tmp_path: Path) -> None:
    day_one = {
        "q5_change_velocity": DayOneAnswer(
            question_id="q5_change_velocity",
            answer="Onboarding-relevant high-velocity areas from git history (90d): src/runtime/run.py",
            confidence="medium",
            evidence=[
                {
                    "analysis_method": "git_log_frequency",
                    "source_file": "src/runtime/run.py",
                    "line_range": [1, 1],
                    "commit_count": 5,
                    "time_window_days": 90,
                    "last_commit_timestamp": "2026-03-09T09:00:00+00:00",
                }
            ],
        )
    }
    content = ArchivistAgent(tmp_path).generate_onboarding_brief(day_one).read_text(encoding="utf-8")
    assert "## 5) Onboarding-Relevant High-Velocity Areas" in content


def test_archivist_onboarding_brief_always_renders_five_day_one_sections(tmp_path: Path) -> None:
    content = ArchivistAgent(tmp_path).generate_onboarding_brief({}).read_text(encoding="utf-8")
    assert "## 1) Primary Data Ingestion Path" in content
    assert "## 2) Critical Output Datasets/Endpoints" in content
    assert "## 3) Blast Radius of Critical Module Failure" in content
    assert "## 4) Business Logic Concentration" in content
    assert "## 5) Onboarding-Relevant High-Velocity Areas" in content
    assert "Evidence:" in content


def test_navigator_langgraph_returns_evidence(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    module_graph, modules, _ = SurveyorAgent().run(mini_repo_copy)
    modules, _ = SemanticistAgent().run(mini_repo_copy, modules)
    for path, module in modules.items():
        module_graph.graph.nodes[path].update(module.model_dump())
    lineage_graph, _ = HydrologistAgent().run(mini_repo_copy)

    agent = NavigatorLangGraphAgent(module_graph, lineage_graph)
    state = agent.invoke("explain_module", "pipeline.py")
    result = state["result"]

    assert result["module"] == "pipeline.py"
    assert result["evidence"]["source_file"] == "pipeline.py"
    assert "line_range" in result["evidence"]
    assert "analysis_method" in result["evidence"]
    assert state["evidence"][0]["source_file"] == "pipeline.py"
    assert state["error"] is None


def test_navigator_find_implementation_uses_vector_semantic_search(
    mini_repo_copy: Path, deterministic_semantics: None
) -> None:
    module_graph, modules, _ = SurveyorAgent().run(mini_repo_copy)
    modules, _ = SemanticistAgent().run(mini_repo_copy, modules)
    for path, module in modules.items():
        module_graph.graph.nodes[path].update(module.model_dump())
    lineage_graph, _ = HydrologistAgent().run(mini_repo_copy)

    agent = NavigatorLangGraphAgent(module_graph, lineage_graph)
    state = agent.invoke("find_implementation", "pipeline")
    result = state["result"]

    assert result["match_count"] >= 1
    assert "similarity_score" in result["matches"][0]
    assert "vector_similarity_semantic_index" in result["matches"][0]["evidence"]["analysis_method"]
    assert state["error"] is None


def test_navigator_query_natural_language_routes_to_trace_lineage(
    mini_repo_copy: Path, deterministic_semantics: None
) -> None:
    module_graph, modules, _ = SurveyorAgent().run(mini_repo_copy)
    modules, _ = SemanticistAgent().run(mini_repo_copy, modules)
    for path, module in modules.items():
        module_graph.graph.nodes[path].update(module.model_dump())
    lineage_graph, _ = HydrologistAgent().run(mini_repo_copy)

    agent = NavigatorLangGraphAgent(module_graph, lineage_graph)
    state = agent.query("what feeds table orders")
    result = state["result"]

    assert state["tool"] == "trace_lineage"
    assert result["direction"] == "upstream"
    assert result["target"].startswith("dataset::")
    assert state["error"] is None


def test_navigator_query_supports_multi_step_chaining(
    mini_repo_copy: Path, deterministic_semantics: None
) -> None:
    module_graph, modules, _ = SurveyorAgent().run(mini_repo_copy)
    modules, _ = SemanticistAgent().run(mini_repo_copy, modules)
    for path, module in modules.items():
        module_graph.graph.nodes[path].update(module.model_dump())
    lineage_graph, _ = HydrologistAgent().run(mini_repo_copy)

    agent = NavigatorLangGraphAgent(module_graph, lineage_graph)
    state = agent.query("blast radius pipeline.py then explain")
    result = state["result"]

    assert state["tool"] == "blast_radius"
    assert result["chain_length"] == 2
    assert result["steps"][0]["tool"] == "blast_radius"
    assert result["steps"][1]["tool"] == "explain_module"
    assert "module" in result["final_result"]
    assert state["error"] is None


def test_navigator_nonexistent_module_returns_informative_evidence(
    mini_repo_copy: Path, deterministic_semantics: None
) -> None:
    module_graph, modules, _ = SurveyorAgent().run(mini_repo_copy)
    modules, _ = SemanticistAgent().run(mini_repo_copy, modules)
    for path, module in modules.items():
        module_graph.graph.nodes[path].update(module.model_dump())
    lineage_graph, _ = HydrologistAgent().run(mini_repo_copy)

    agent = NavigatorLangGraphAgent(module_graph, lineage_graph)
    state = agent.invoke("explain_module", "not_real_module.py")

    assert state["error"] is not None
    assert state["evidence"]
    assert "source_file" in state["evidence"][0]
    assert "line_range" in state["evidence"][0]
    assert "analysis_method" in state["evidence"][0]


def test_surveyor_detects_circular_module_dependencies(tmp_path: Path) -> None:
    repo = tmp_path / "cycle_repo"
    repo.mkdir()
    (repo / "a.py").write_text("import b\n\ndef alpha():\n    return 1\n", encoding="utf-8")
    (repo / "b.py").write_text("import a\n\ndef beta():\n    return 2\n", encoding="utf-8")

    graph, modules, trace = SurveyorAgent().run(repo)

    assert graph.graph.has_edge("a.py", "b.py")
    assert graph.graph.has_edge("b.py", "a.py")
    assert modules["a.py"].is_in_import_cycle is True
    assert modules["b.py"].import_cycle_size == 2
    assert trace[-1].evidence["import_cycle_count"] == 1
