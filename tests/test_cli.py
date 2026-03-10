from __future__ import annotations

from pathlib import Path

from src.cli import _resolve_output_dir


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
