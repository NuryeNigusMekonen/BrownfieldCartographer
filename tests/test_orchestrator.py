from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import pytest

from src.orchestrator import CartographyOrchestrator
from src.graph.knowledge_graph import KnowledgeGraph


def test_orchestrator_incremental_detects_changed_files(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    orchestrator.analyze(incremental=False)

    time.sleep(0.01)
    target = mini_repo_copy / "helpers.py"
    target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    changed = orchestrator.changed_files_since_last_run()
    assert "helpers.py" in changed


def test_orchestrator_incremental_updates_trace(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    orchestrator.analyze(incremental=False)

    time.sleep(0.01)
    target = mini_repo_copy / "pipeline.py"
    target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    orchestrator.analyze(incremental=True)
    trace_text = (mini_repo_copy / ".cartography" / "cartography_trace.jsonl").read_text(encoding="utf-8")
    assert '"action": "incremental_update"' in trace_text


def test_orchestrator_state_contains_repository_metadata(
    mini_repo_copy: Path, deterministic_semantics: None, monkeypatch
) -> None:
    monkeypatch.setattr(
        "src.orchestrator.repository_metadata",
        lambda *_: {
            "owner": "openedx",
            "repo_name": "ol-data-platform",
            "branch": "main",
            "display_name": "openedx/ol-data-platform",
            "repo_url": "https://github.com/openedx/ol-data-platform",
        },
    )

    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    orchestrator.analyze(incremental=False)

    state = json.loads((mini_repo_copy / ".cartography" / "state.json").read_text(encoding="utf-8"))
    assert state["repository"]["owner"] == "openedx"
    assert state["repository"]["repo_name"] == "ol-data-platform"
    assert state["repository"]["branch"] == "main"
    assert state["repository"]["display_name"] == "openedx/ol-data-platform"


def test_orchestrator_logs_progress(mini_repo_copy: Path, deterministic_semantics: None, capsys) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    orchestrator.analyze(incremental=False)

    output = capsys.readouterr().out
    assert "Running Surveyor agent." in output
    assert "Running Hydrologist agent." in output
    assert "Serializing artifacts to" in output


def test_orchestrator_skips_failed_surveyor_file(
    mini_repo_copy: Path, deterministic_semantics: None, monkeypatch
) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    original_analyze_module = orchestrator.surveyor.analyzer.analyze_module

    def flaky_analyze_module(path: Path, repo_root: Path):
        if path.name == "helpers.py":
            raise RuntimeError("synthetic parser failure")
        return original_analyze_module(path, repo_root)

    monkeypatch.setattr(orchestrator.surveyor.analyzer, "analyze_module", flaky_analyze_module)
    artifacts = orchestrator.analyze(incremental=False)

    module_graph = KnowledgeGraph.load(Path(artifacts["module_graph"]))
    assert "pipeline.py" in module_graph.graph
    assert "helpers.py" not in module_graph.graph

    trace_text = Path(artifacts["trace"]).read_text(encoding="utf-8")
    assert '"agent": "surveyor"' in trace_text
    assert '"action": "files_skipped_on_error"' in trace_text


def test_orchestrator_skips_failed_hydrologist_file(
    mini_repo_copy: Path, deterministic_semantics: None, monkeypatch
) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    original_extract_sql = orchestrator.hydrologist.sql.extract_from_file

    def flaky_extract_sql(path: Path, repo_root: Path):
        if path.name == "model.sql":
            raise RuntimeError("synthetic sql failure")
        return original_extract_sql(path, repo_root)

    monkeypatch.setattr(orchestrator.hydrologist.sql, "extract_from_file", flaky_extract_sql)
    artifacts = orchestrator.analyze(incremental=False)

    lineage_graph = KnowledgeGraph.load(Path(artifacts["lineage_graph"]))
    assert lineage_graph.graph.number_of_nodes() > 0

    trace_text = Path(artifacts["trace"]).read_text(encoding="utf-8")
    assert '"agent": "hydrologist"' in trace_text
    assert '"action": "files_skipped_on_error"' in trace_text


def test_orchestrator_trace_rows_include_analysis_method_and_evidence_sources(
    mini_repo_copy: Path, deterministic_semantics: None
) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    artifacts = orchestrator.analyze(incremental=False)

    rows = [
        json.loads(line)
        for line in Path(artifacts["trace"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert rows
    for row in rows:
        assert row.get("timestamp")
        assert row.get("confidence") in {"low", "medium", "high"}
        evidence = row.get("evidence", {})
        assert isinstance(evidence, dict)
        assert "analysis_method" in evidence
        assert isinstance(evidence.get("evidence_sources", []), list)


def test_orchestrator_incremental_detects_commits_via_git_log(
    mini_repo_copy: Path, deterministic_semantics: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    previous_head = "abc123"
    orchestrator.state_file.write_text(
        json.dumps({"head": previous_head, "analyzed_at_epoch": time.time() - 60}),
        encoding="utf-8",
    )

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        if cmd[:5] == ["git", "-C", str(mini_repo_copy), "log", "--name-only"]:
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="pipeline.py\nhelpers.py\n",
                stderr="",
            )
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="unsupported")

    monkeypatch.setattr("src.orchestrator.subprocess.run", fake_run)
    changed = orchestrator.changed_files_since_last_run()

    assert "pipeline.py" in changed
    assert "helpers.py" in changed
    assert any(call[:4] == ["git", "-C", str(mini_repo_copy), "log"] for call in calls)
    assert any(f"{previous_head}..HEAD" in call for call in calls)


def test_orchestrator_writes_partial_artifacts_when_semanticist_fails(
    mini_repo_copy: Path, deterministic_semantics: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    monkeypatch.setattr(orchestrator.semanticist, "run", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        orchestrator.analyze(incremental=False)

    out_dir = mini_repo_copy / ".cartography"
    assert (out_dir / "module_graph.json").exists()
    assert (out_dir / "lineage_graph.json").exists()
    assert (out_dir / "cartography_trace.jsonl").exists()

    trace_rows = [
        json.loads(line)
        for line in (out_dir / "cartography_trace.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(row.get("action") == "analysis_failed" for row in trace_rows)
