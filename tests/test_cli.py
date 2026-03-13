from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli
from src.cli import _resolve_output_dir
from src.graph.knowledge_graph import KnowledgeGraph


def test_resolve_output_dir_keeps_default_repo_relative(tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    assert _resolve_output_dir(repo_path, ".cartography") == repo_path / ".cartography"


def test_resolve_output_dir_treats_nested_relative_path_as_cwd_relative(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    assert _resolve_output_dir(repo_path, "test_repos/jaffle-shop/.cartography") == (
        tmp_path / "test_repos" / "jaffle-shop" / ".cartography"
    )


def test_analyze_accepts_github_url(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()
    fake_repo = tmp_path / "workspace" / "remote-repo"
    fake_repo.mkdir(parents=True, exist_ok=True)
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        cli,
        "resolve_repo_input",
        lambda repo_input, checkout_root=None: fake_repo,
    )

    class DummyOrchestrator:
        def __init__(self, repo_path: Path, out_dir: Path | None = None, repo_input: str | None = None) -> None:
            captured["repo_input"] = repo_input or ""
            self.out_dir = out_dir or (repo_path / ".cartography")

        def changed_files_since_last_run(self) -> list[str]:
            return []

        def analyze(self, incremental: bool = True) -> dict[str, str]:
            return {
                "module_graph": str(self.out_dir / "module_graph.json"),
                "lineage_graph": str(self.out_dir / "lineage_graph.json"),
            }

    monkeypatch.setattr(cli, "CartographyOrchestrator", DummyOrchestrator)
    github_url = "https://github.com/dbt-labs/jaffle_shop.git"
    result = runner.invoke(cli.app, ["analyze", github_url, "--no-incremental"])

    assert result.exit_code == 0
    assert captured["repo_input"] == github_url
    assert "module_graph.json" in result.stdout
    assert "lineage_graph.json" in result.stdout


def test_query_uses_langgraph_navigator_runtime(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()
    repo_path = tmp_path / "repo"
    cartography_dir = repo_path / ".cartography"
    cartography_dir.mkdir(parents=True)
    KnowledgeGraph().serialize(cartography_dir / "module_graph.json")
    KnowledgeGraph().serialize(cartography_dir / "lineage_graph.json")

    monkeypatch.setattr(cli, "resolve_repo_input", lambda repo_input, checkout_root=None: repo_path)
    captured: dict[str, object] = {}

    class DummyNavigatorLangGraphAgent:
        def __init__(self, module_graph: KnowledgeGraph, lineage_graph: KnowledgeGraph) -> None:
            captured["graphs_loaded"] = bool(module_graph is not None and lineage_graph is not None)

        def invoke(self, tool: str, arg: str, direction: str = "upstream") -> dict[str, object]:
            captured["tool"] = tool
            captured["arg"] = arg
            captured["direction"] = direction
            return {
                "tool": tool,
                "arg": arg,
                "direction": direction,
                "result": {"module": arg, "evidence": {"source_file": arg, "line_range": [1, 12], "analysis_method": "static"}},
                "error": None,
                "evidence": [{"source_file": arg, "line_range": [1, 12], "analysis_method": "static"}],
            }

    monkeypatch.setattr(cli, "NavigatorLangGraphAgent", DummyNavigatorLangGraphAgent)
    result = runner.invoke(cli.app, ["query", str(repo_path), "explain_module", "pipeline.py"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert captured["graphs_loaded"] is True
    assert captured["tool"] == "explain_module"
    assert payload["result"]["module"] == "pipeline.py"
    assert payload["evidence"][0]["source_file"] == "pipeline.py"


def test_query_alias_normalizes_to_trace_lineage_direction(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()
    repo_path = tmp_path / "repo"
    cartography_dir = repo_path / ".cartography"
    cartography_dir.mkdir(parents=True)
    KnowledgeGraph().serialize(cartography_dir / "module_graph.json")
    KnowledgeGraph().serialize(cartography_dir / "lineage_graph.json")

    monkeypatch.setattr(cli, "resolve_repo_input", lambda repo_input, checkout_root=None: repo_path)
    captured: dict[str, str] = {}

    class DummyNavigatorLangGraphAgent:
        def __init__(self, module_graph: KnowledgeGraph, lineage_graph: KnowledgeGraph) -> None:
            pass

        def invoke(self, tool: str, arg: str, direction: str = "upstream") -> dict[str, object]:
            captured["tool"] = tool
            captured["direction"] = direction
            return {
                "tool": tool,
                "arg": arg,
                "direction": direction,
                "result": {"target": arg, "direction": direction, "nodes": [], "node_count": 0, "evidence": []},
                "error": None,
                "evidence": [],
            }

    monkeypatch.setattr(cli, "NavigatorLangGraphAgent", DummyNavigatorLangGraphAgent)
    result = runner.invoke(cli.app, ["query", str(repo_path), "downstream", "orders"])

    assert result.exit_code == 0
    assert captured["tool"] == "trace_lineage"
    assert captured["direction"] == "downstream"
