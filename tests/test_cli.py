from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli
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
