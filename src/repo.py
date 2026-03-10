from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE_REPO_ROOT = PROJECT_ROOT / "test_repos"
SYNC_IGNORE = shutil.ignore_patterns(
    ".cartography",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "node_modules",
    "dist",
    "build",
)


def is_github_url(value: str) -> bool:
    return value.startswith("https://github.com/") or value.startswith("git@github.com:")


def resolve_repo_input(repo_input: str, checkout_root: Path | None = None) -> Path:
    """
    Normalize any repo input to a workspace-local checkout under:
    <project_root>/test_repos/<repo_name>
    """
    workspace_root = (checkout_root or DEFAULT_WORKSPACE_REPO_ROOT).resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)

    repo_name = normalize_repo_name(repo_input)
    repo_dir = workspace_root / repo_name

    if is_github_url(repo_input):
        return _resolve_github_repo(repo_input, repo_dir)

    return _resolve_local_repo(Path(repo_input).expanduser().resolve(), repo_dir)


def normalize_repo_name(repo_input: str) -> str:
    raw = _extract_repo_name(repo_input)
    clean = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw).strip("-._")
    return clean or "repo"


def _extract_repo_name(repo_input: str) -> str:
    text = repo_input.strip().rstrip("/")
    if is_github_url(text):
        tail = text.split("/")[-1]
        if ":" in tail:
            tail = tail.split(":")[-1]
    else:
        tail = Path(text).name
    if tail.endswith(".git"):
        tail = tail[:-4]
    return tail or "repo"


def _resolve_github_repo(repo_url: str, repo_dir: Path) -> Path:
    repo_dir.parent.mkdir(parents=True, exist_ok=True)

    if repo_dir.exists() and (repo_dir / ".git").exists():
        remote = _git_origin_url(repo_dir)
        if remote and remote != repo_url:
            shutil.rmtree(repo_dir)
            _run_git(["clone", "--depth", "1", repo_url, str(repo_dir)])
            return repo_dir
        _run_git(["-C", str(repo_dir), "fetch", "--all"])
        _run_git(["-C", str(repo_dir), "pull", "--ff-only"])
        return repo_dir

    if repo_dir.exists():
        shutil.rmtree(repo_dir)

    _run_git(["clone", "--depth", "1", repo_url, str(repo_dir)])
    return repo_dir


def _resolve_local_repo(source_repo: Path, workspace_repo: Path) -> Path:
    if not source_repo.exists() or not source_repo.is_dir():
        raise RuntimeError(f"Local repository not found: {source_repo}")

    if source_repo == workspace_repo:
        workspace_repo.mkdir(parents=True, exist_ok=True)
        return workspace_repo

    _sync_local_repo(source_repo, workspace_repo)
    return workspace_repo


def _sync_local_repo(source_repo: Path, workspace_repo: Path) -> None:
    workspace_repo.mkdir(parents=True, exist_ok=True)

    for child in workspace_repo.iterdir():
        if child.name == ".cartography":
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)

    shutil.copytree(source_repo, workspace_repo, dirs_exist_ok=True, ignore=SYNC_IGNORE)


def _git_origin_url(repo_dir: Path) -> str:
    cmd = ["git", "-C", str(repo_dir), "remote", "get-url", "origin"]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        return ""
    return out.stdout.strip()


def _run_git(args: list[str]) -> None:
    cmd = ["git", *args]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise RuntimeError(f"git command failed: {' '.join(cmd)}\n{out.stderr.strip()}")
