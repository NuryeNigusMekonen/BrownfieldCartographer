from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path


def is_github_url(value: str) -> bool:
    return value.startswith("https://github.com/") or value.startswith("git@github.com:")


def resolve_repo_input(repo_input: str, checkout_root: Path | None = None) -> Path:
    """
    Resolve a local path or clone a GitHub URL into a deterministic location.
    """
    if not is_github_url(repo_input):
        return Path(repo_input).resolve()

    checkout_root = (checkout_root or Path("/tmp/cartographer_repos")).resolve()
    checkout_root.mkdir(parents=True, exist_ok=True)
    slug = _slugify_repo(repo_input)
    repo_dir = checkout_root / slug

    if repo_dir.exists() and (repo_dir / ".git").exists():
        _run_git(["-C", str(repo_dir), "fetch", "--all"])
        _run_git(["-C", str(repo_dir), "pull", "--ff-only"])
        return repo_dir

    _run_git(["clone", "--depth", "1", repo_input, str(repo_dir)])
    return repo_dir


def _slugify_repo(url: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9._-]+", "-", url.strip("/"))
    suffix = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
    return f"{base}-{suffix}"


def _run_git(args: list[str]) -> None:
    cmd = ["git", *args]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise RuntimeError(f"git command failed: {' '.join(cmd)}\n{out.stderr.strip()}")

