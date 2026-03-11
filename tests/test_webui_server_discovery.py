from __future__ import annotations

import json
from pathlib import Path

import backend.workspace_api as server


def _make_cartography(repo_dir: Path, analyzed_at_epoch: float = 1_700_000_000.0) -> None:
    cartography = repo_dir / ".cartography"
    (cartography / "semantic_index").mkdir(parents=True, exist_ok=True)
    (cartography / "module_graph.json").write_text("{}", encoding="utf-8")
    (cartography / "lineage_graph.json").write_text("{}", encoding="utf-8")
    (cartography / "semantic_index" / "module_purpose_index.jsonl").write_text("\n", encoding="utf-8")
    (cartography / "CODEBASE.md").write_text("# Codebase\n", encoding="utf-8")
    (cartography / "onboarding_brief.md").write_text("# Brief\n", encoding="utf-8")
    (cartography / "cartography_trace.jsonl").write_text("\n", encoding="utf-8")
    (cartography / "state.json").write_text(
        json.dumps(
            {
                "head": "abc123",
                "analyzed_at_epoch": analyzed_at_epoch,
                "repository": {
                    "owner": "dbt-labs",
                    "repo_name": repo_dir.name,
                    "branch": "main",
                    "display_name": f"dbt-labs/{repo_dir.name}",
                    "url": f"https://github.com/dbt-labs/{repo_dir.name}",
                },
            }
        ),
        encoding="utf-8",
    )


def test_workspace_backend_discovers_sessions_from_test_repos(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "test_repos"
    repo_dir = workspace_root / "jaffle_shop"
    repo_dir.mkdir(parents=True, exist_ok=True)
    _make_cartography(repo_dir)

    monkeypatch.setattr(server, "DEFAULT_WORKSPACE_REPO_ROOT", workspace_root)
    monkeypatch.setattr(server, "SESSION_STATE_FILE", tmp_path / "state" / "sessions.json")

    backend = server.WorkspaceBackend()
    payload = backend.sessions_payload()

    assert payload["active_repo_id"]
    assert len(payload["sessions"]) == 1
    assert payload["sessions"][0]["repo_name"] == "jaffle_shop"
    assert payload["sessions"][0]["repo_display_name"] == "dbt-labs/jaffle_shop"
    assert payload["sessions"][0]["repo_branch"] == "main"
    assert Path(payload["sessions"][0]["repo_path"]) == repo_dir.resolve()
