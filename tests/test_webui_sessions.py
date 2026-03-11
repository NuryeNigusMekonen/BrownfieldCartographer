from __future__ import annotations

import json
from pathlib import Path

from backend.sessions import WorkspaceSessionStore


def make_cartography(repo_path: Path, analyzed_at_epoch: float = 1_700_000_000.0) -> Path:
    cartography = repo_path / ".cartography"
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
                    "owner": "openedx",
                    "repo_name": repo_path.name,
                    "branch": "main",
                    "display_name": f"openedx/{repo_path.name}",
                    "url": f"https://github.com/openedx/{repo_path.name}",
                },
            }
        ),
        encoding="utf-8",
    )
    return cartography


def test_session_store_upsert_and_active(tmp_path: Path) -> None:
    state_file = tmp_path / "state" / "sessions.json"
    store = WorkspaceSessionStore(state_file)

    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()

    cartography_a = make_cartography(repo_a, analyzed_at_epoch=1_700_000_000.0)
    cartography_b = make_cartography(repo_b, analyzed_at_epoch=1_700_100_000.0)

    session_a = store.upsert_session(str(repo_a), repo_a, cartography_a)
    session_b = store.upsert_session(str(repo_b), repo_b, cartography_b)

    assert store.active_repo_id() == session_b["repo_id"]
    assert store.active_session()["repo_id"] == session_b["repo_id"]

    sessions = store.list_sessions()
    assert [item["repo_id"] for item in sessions] == [session_b["repo_id"], session_a["repo_id"]]
    assert "module_graph.json" in sessions[0]["available_artifacts"]
    assert sessions[0]["last_analysis_timestamp"].endswith("Z")


def test_register_cartography_dir(tmp_path: Path) -> None:
    state_file = tmp_path / "state" / "sessions.json"
    store = WorkspaceSessionStore(state_file)

    repo = tmp_path / "repo"
    repo.mkdir()
    cartography = make_cartography(repo)

    session = store.register_cartography_dir(cartography)

    assert session["repo_name"] == "repo"
    assert session["repo_display_name"] == "openedx/repo"
    assert session["repo_branch"] == "main"
    assert Path(session["cartography_dir"]) == cartography.resolve()


def test_register_cartography_dir_without_switching_active(tmp_path: Path) -> None:
    state_file = tmp_path / "state" / "sessions.json"
    store = WorkspaceSessionStore(state_file)

    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()

    cartography_a = make_cartography(repo_a, analyzed_at_epoch=1_700_000_000.0)
    cartography_b = make_cartography(repo_b, analyzed_at_epoch=1_700_100_000.0)

    session_a = store.register_cartography_dir(cartography_a)
    session_b = store.register_cartography_dir(cartography_b, set_active=False)

    assert store.active_repo_id() == session_a["repo_id"]
    assert session_b["repo_id"] in [entry["repo_id"] for entry in store.list_sessions()]
