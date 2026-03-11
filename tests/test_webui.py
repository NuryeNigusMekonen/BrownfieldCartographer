from __future__ import annotations

from pathlib import Path

import pytest

from backend.workspace_data import CartographyWorkspaceData
from src.cli import _resolve_cartography_dir
from tests.conftest import run_analysis


@pytest.fixture
def analyzed_mini_repo(mini_repo_copy: Path, deterministic_semantics: None) -> Path:
    return run_analysis(mini_repo_copy)


def test_workspace_summary_and_graph_payloads(analyzed_mini_repo: Path) -> None:
    workspace = CartographyWorkspaceData(analyzed_mini_repo)

    summary = workspace.summary_payload()
    module_graph = workspace.module_graph_payload()
    lineage_graph = workspace.lineage_graph_payload()

    assert summary["metrics"]["modules"] > 0
    assert "top_modules" in summary
    assert "repository" in summary
    assert "display_name" in summary["repository"]
    assert module_graph["nodes"]
    assert all("degree_centrality" in node for node in module_graph["nodes"])
    assert lineage_graph["nodes"]
    assert all("node_type" in node for node in lineage_graph["nodes"])


def test_workspace_docs_and_query_payloads(analyzed_mini_repo: Path) -> None:
    workspace = CartographyWorkspaceData(analyzed_mini_repo)

    semanticist = workspace.semantic_payload()
    archivist = workspace.archivist_payload()
    semantic = workspace.semantic_search("pipeline")
    query = workspace.run_query("explain pipeline.py")

    assert semanticist["modules"]
    assert "codebase" in archivist
    assert archivist["onboarding"]["questions"]
    assert "results" in semantic
    assert query["ok"] is True
    assert query["tool"] == "explain_module"
    assert query["arg"] == "pipeline.py"
    assert query["result"]["module"] == "pipeline.py"


def test_resolve_cartography_dir_requires_existing_output(tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with pytest.raises(Exception):
        _resolve_cartography_dir(repo_path, ".cartography")
