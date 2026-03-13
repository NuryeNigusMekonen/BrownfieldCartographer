from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import subprocess
from typing import Iterable


@dataclass(frozen=True)
class GitFileVelocity:
    path: str
    commit_count: int
    last_commit_timestamp: str


@dataclass(frozen=True)
class GitVelocitySnapshot:
    time_window_days: int
    history_status: str  # complete | shallow | missing | unavailable
    history_note: str
    files: tuple[GitFileVelocity, ...]
    commit_events_scanned: int = 0

    def by_path(self) -> dict[str, GitFileVelocity]:
        return {entry.path: entry for entry in self.files}


def compute_git_velocity_snapshot(repo_path: Path, days: int = 90) -> GitVelocitySnapshot:
    repo = repo_path.resolve()
    if days <= 0:
        days = 90
    if not (repo / ".git").exists():
        return GitVelocitySnapshot(
            time_window_days=days,
            history_status="missing",
            history_note="Velocity could not be computed because git metadata is missing.",
            files=tuple(),
            commit_events_scanned=0,
        )

    inside = _run_git(repo, ["rev-parse", "--is-inside-work-tree"])
    if inside.returncode != 0 or inside.stdout.strip().lower() != "true":
        return GitVelocitySnapshot(
            time_window_days=days,
            history_status="missing",
            history_note="Velocity could not be computed because this path is not a git worktree.",
            files=tuple(),
            commit_events_scanned=0,
        )

    shallow_result = _run_git(repo, ["rev-parse", "--is-shallow-repository"])
    shallow = shallow_result.returncode == 0 and shallow_result.stdout.strip().lower() == "true"
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    log_result = _run_git(
        repo,
        [
            "log",
            "--since",
            since,
            "--date=iso-strict",
            "--name-only",
            "--pretty=format:__BC_COMMIT__%H|%cI",
            "--",
        ],
    )
    if log_result.returncode != 0:
        return GitVelocitySnapshot(
            time_window_days=days,
            history_status="unavailable",
            history_note="Velocity could not be fully computed because git log is unavailable.",
            files=tuple(),
            commit_events_scanned=0,
        )

    rows = tuple(_parse_log_lines(log_result.stdout.splitlines()))
    status = "shallow" if shallow else "complete"
    if shallow:
        note = "Velocity is based on shallow clone history."
    else:
        note = "Velocity is based on full git commit history for the selected time window."
    return GitVelocitySnapshot(
        time_window_days=days,
        history_status=status,
        history_note=note,
        files=rows,
        commit_events_scanned=_count_commit_headers(log_result.stdout.splitlines()),
    )


def _run_git(repo_path: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    cmd = ["git", "-C", str(repo_path), *args]
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=False)
    except Exception:
        return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr="git invocation failed")


def _count_commit_headers(lines: Iterable[str]) -> int:
    return sum(1 for line in lines if line.startswith("__BC_COMMIT__"))


def _parse_log_lines(lines: list[str]) -> list[GitFileVelocity]:
    per_file: dict[str, dict[str, object]] = {}
    current_hash = ""
    current_ts = ""
    current_files: set[str] = set()

    def flush() -> None:
        if not current_hash:
            return
        for rel in current_files:
            row = per_file.setdefault(rel, {"count": 0, "last_commit_timestamp": ""})
            row["count"] = int(row["count"]) + 1
            previous_ts = str(row["last_commit_timestamp"] or "")
            if not previous_ts:
                row["last_commit_timestamp"] = current_ts
            else:
                row["last_commit_timestamp"] = _latest_timestamp(previous_ts, current_ts)
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if line.startswith("__BC_COMMIT__"):
            flush()
            payload = line.replace("__BC_COMMIT__", "", 1)
            parts = payload.split("|", 1)
            current_hash = parts[0].strip()
            current_ts = parts[1].strip() if len(parts) > 1 else ""
            current_files = set()
            continue
        if current_hash:
            current_files.add(line)
    flush()

    ranked = sorted(
        (
            GitFileVelocity(
                path=path,
                commit_count=int(payload.get("count", 0)),
                last_commit_timestamp=str(payload.get("last_commit_timestamp", "")),
            )
            for path, payload in per_file.items()
            if int(payload.get("count", 0)) > 0
        ),
        key=lambda item: (-item.commit_count, item.path),
    )
    return ranked


def _latest_timestamp(a: str, b: str) -> str:
    if not a:
        return b
    if not b:
        return a
    try:
        dt_a = datetime.fromisoformat(a.replace("Z", "+00:00"))
        dt_b = datetime.fromisoformat(b.replace("Z", "+00:00"))
        return a if dt_a >= dt_b else b
    except ValueError:
        return max(a, b)
