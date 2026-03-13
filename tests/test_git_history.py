from __future__ import annotations

import subprocess
from pathlib import Path

from src.analyzers import git_history


def test_compute_git_velocity_snapshot_missing_git_dir(tmp_path: Path) -> None:
    snapshot = git_history.compute_git_velocity_snapshot(tmp_path, days=90)
    assert snapshot.history_status == "missing"
    assert "git metadata is missing" in snapshot.history_note.lower()
    assert snapshot.files == tuple()


def test_compute_git_velocity_snapshot_parses_commit_counts(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir(parents=True)

    def fake_run(repo_path: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
        if args[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return subprocess.CompletedProcess(["git"], returncode=0, stdout="true\n", stderr="")
        if args[:2] == ["rev-parse", "--is-shallow-repository"]:
            return subprocess.CompletedProcess(["git"], returncode=0, stdout="false\n", stderr="")
        if args and args[0] == "log":
            return subprocess.CompletedProcess(
                ["git"],
                returncode=0,
                stdout=(
                    "__BC_COMMIT__a1|2026-03-10T10:00:00+00:00\n"
                    "src/app.py\n"
                    "docs/README.md\n"
                    "\n"
                    "__BC_COMMIT__b2|2026-03-09T09:00:00+00:00\n"
                    "src/app.py\n"
                    "src/core/runtime.py\n"
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(["git"], returncode=1, stdout="", stderr="unexpected")

    monkeypatch.setattr(git_history, "_run_git", fake_run)
    snapshot = git_history.compute_git_velocity_snapshot(tmp_path, days=90)

    assert snapshot.history_status == "complete"
    assert snapshot.time_window_days == 90
    rows = {entry.path: entry for entry in snapshot.files}
    assert rows["src/app.py"].commit_count == 2
    assert rows["src/app.py"].last_commit_timestamp == "2026-03-10T10:00:00+00:00"
    assert rows["docs/README.md"].commit_count == 1
    assert rows["src/core/runtime.py"].commit_count == 1
    assert snapshot.commit_events_scanned == 2


def test_compute_git_velocity_snapshot_shallow_clone_note(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir(parents=True)

    def fake_run(repo_path: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
        if args[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return subprocess.CompletedProcess(["git"], returncode=0, stdout="true\n", stderr="")
        if args[:2] == ["rev-parse", "--is-shallow-repository"]:
            return subprocess.CompletedProcess(["git"], returncode=0, stdout="true\n", stderr="")
        if args and args[0] == "log":
            return subprocess.CompletedProcess(["git"], returncode=0, stdout="", stderr="")
        return subprocess.CompletedProcess(["git"], returncode=1, stdout="", stderr="unexpected")

    monkeypatch.setattr(git_history, "_run_git", fake_run)
    snapshot = git_history.compute_git_velocity_snapshot(tmp_path, days=30)
    assert snapshot.history_status == "shallow"
    assert "shallow clone history" in snapshot.history_note.lower()
