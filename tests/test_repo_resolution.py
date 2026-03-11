from __future__ import annotations

from pathlib import Path

import src.repo as repo


def test_normalize_repo_name_for_github_urls() -> None:
    assert repo.normalize_repo_name("https://github.com/dbt-labs/jaffle_shop.git") == "jaffle_shop"
    assert repo.normalize_repo_name("git@github.com:meltano/meltano.git") == "meltano"


def test_extract_repo_owner_and_name_from_repo_urls() -> None:
    assert repo.extract_repo_owner_and_name("https://github.com/dbt-labs/jaffle_shop.git") == ("dbt-labs", "jaffle_shop")
    assert repo.extract_repo_owner_and_name("git@github.com:meltano/meltano.git") == ("meltano", "meltano")


def test_normalize_repo_name_for_local_paths(tmp_path: Path) -> None:
    source = tmp_path / "my pipeline@2026"
    source.mkdir()

    assert repo.normalize_repo_name(str(source)) == "my-pipeline-2026"


def test_resolve_local_repo_syncs_into_workspace(tmp_path: Path) -> None:
    source = tmp_path / "my_pipeline"
    (source / "src").mkdir(parents=True)
    (source / "src" / "main.py").write_text("print('ok')\n", encoding="utf-8")
    (source / ".cartography").mkdir(parents=True)
    (source / ".cartography" / "ignore.json").write_text("{}", encoding="utf-8")

    workspace_root = tmp_path / "workspace" / "test_repos"
    resolved = repo.resolve_repo_input(str(source), checkout_root=workspace_root)

    assert resolved == (workspace_root / "my_pipeline").resolve()
    assert (resolved / "src" / "main.py").exists()
    assert not (resolved / ".cartography" / "ignore.json").exists()

    # Existing workspace artifacts should survive local re-sync.
    (resolved / ".cartography").mkdir(parents=True, exist_ok=True)
    keep_file = resolved / ".cartography" / "keep.txt"
    keep_file.write_text("keep", encoding="utf-8")
    (source / "README.md").write_text("# Pipeline\n", encoding="utf-8")

    repo.resolve_repo_input(str(source), checkout_root=workspace_root)

    assert keep_file.exists()
    assert (resolved / "README.md").exists()


def test_resolve_github_repo_clones_into_workspace(tmp_path: Path, monkeypatch) -> None:
    url = "https://github.com/dbt-labs/jaffle_shop.git"
    workspace_root = tmp_path / "workspace" / "test_repos"
    clone_target = workspace_root / "jaffle_shop"
    calls: list[list[str]] = []

    def fake_run_git(args: list[str]) -> None:
        calls.append(list(args))
        clone_target.mkdir(parents=True, exist_ok=True)
        (clone_target / ".git").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(repo, "_run_git", fake_run_git)

    resolved = repo.resolve_repo_input(url, checkout_root=workspace_root)

    assert resolved == clone_target.resolve()
    assert calls == [["clone", "--depth", "1", url, str(clone_target)]]


def test_resolve_github_repo_fetches_existing_checkout(tmp_path: Path, monkeypatch) -> None:
    url = "https://github.com/meltano/meltano.git"
    workspace_root = tmp_path / "workspace" / "test_repos"
    repo_dir = workspace_root / "meltano"
    (repo_dir / ".git").mkdir(parents=True, exist_ok=True)
    calls: list[list[str]] = []

    monkeypatch.setattr(repo, "_git_origin_url", lambda _: url)
    monkeypatch.setattr(repo, "_run_git", lambda args: calls.append(list(args)))

    resolved = repo.resolve_repo_input(url, checkout_root=workspace_root)

    assert resolved == repo_dir.resolve()
    assert calls == [
        ["-C", str(repo_dir), "fetch", "--all"],
        ["-C", str(repo_dir), "pull", "--ff-only"],
    ]


def test_repository_metadata_uses_remote_when_input_is_local_path(tmp_path: Path, monkeypatch) -> None:
    repo_dir = tmp_path / "checkout"
    repo_dir.mkdir()

    monkeypatch.setattr(repo, "_git_origin_url", lambda _: "git@github.com:openedx/ol-data-platform.git")
    monkeypatch.setattr(repo, "git_current_branch", lambda _: "main")

    metadata = repo.repository_metadata(str(repo_dir), repo_dir)

    assert metadata["owner"] == "openedx"
    assert metadata["repo_name"] == "ol-data-platform"
    assert metadata["display_name"] == "openedx/ol-data-platform"
    assert metadata["branch"] == "main"
