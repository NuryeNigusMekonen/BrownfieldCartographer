from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REQUIRED_ARTIFACTS = [
    "module_graph.json",
    "lineage_graph.json",
    "semantic_index/module_purpose_index.jsonl",
    "CODEBASE.md",
    "onboarding_brief.md",
    "cartography_trace.jsonl",
    "state.json",
]


class WorkspaceSessionStore:
    def __init__(self, state_file: Path, max_sessions: int = 25) -> None:
        self.state_file = state_file
        self.max_sessions = max_sessions
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def list_sessions(self) -> list[dict[str, Any]]:
        state = self._load_state()
        sessions = state.get("sessions", [])
        return sorted(sessions, key=lambda item: float(item.get("last_analysis_epoch") or 0.0), reverse=True)

    def active_repo_id(self) -> str:
        state = self._load_state()
        return str(state.get("active_repo_id") or "")

    def set_active_repo_id(self, repo_id: str) -> dict[str, Any]:
        state = self._load_state()
        if not any(str(item.get("repo_id")) == repo_id for item in state.get("sessions", [])):
            raise FileNotFoundError(f"Unknown repo_id: {repo_id}")
        state["active_repo_id"] = repo_id
        self._save_state(state)
        return self.get_session(repo_id)

    def get_session(self, repo_id: str) -> dict[str, Any]:
        for session in self.list_sessions():
            if str(session.get("repo_id")) == repo_id:
                return session
        raise FileNotFoundError(f"Unknown repo_id: {repo_id}")

    def active_session(self) -> dict[str, Any] | None:
        repo_id = self.active_repo_id()
        if not repo_id:
            return None
        try:
            return self.get_session(repo_id)
        except FileNotFoundError:
            return None

    def upsert_session(
        self,
        repo_input: str,
        repo_path: Path,
        cartography_dir: Path,
        *,
        set_active: bool = True,
    ) -> dict[str, Any]:
        repo_path = repo_path.resolve()
        cartography_dir = cartography_dir.resolve()
        repo_id = self._repo_id(repo_path)
        payload = self._session_payload(repo_id, repo_input, repo_path, cartography_dir)

        state = self._load_state()
        sessions = [item for item in state.get("sessions", []) if str(item.get("repo_id")) != repo_id]
        sessions.insert(0, payload)
        state["sessions"] = sessions[: self.max_sessions]
        if set_active or not str(state.get("active_repo_id") or ""):
            state["active_repo_id"] = repo_id
        self._save_state(state)
        return payload

    def register_cartography_dir(self, cartography_dir: Path, *, set_active: bool = True) -> dict[str, Any]:
        cartography_dir = cartography_dir.resolve()
        repo_path = cartography_dir.parent
        return self.upsert_session(str(repo_path), repo_path, cartography_dir, set_active=set_active)

    def _session_payload(
        self,
        repo_id: str,
        repo_input: str,
        repo_path: Path,
        cartography_dir: Path,
    ) -> dict[str, Any]:
        state_path = cartography_dir / "state.json"
        state = self._read_json(state_path) if state_path.exists() else {}
        analyzed_at_epoch = float(state.get("analyzed_at_epoch") or 0.0)
        timestamp = self._format_timestamp(analyzed_at_epoch)
        artifacts = self._artifact_metadata(cartography_dir)
        available = [item["name"] for item in artifacts if item["exists"]]

        return {
            "repo_id": repo_id,
            "repo_name": repo_path.name,
            "repo_input": repo_input,
            "repo_path": str(repo_path),
            "cartography_dir": str(cartography_dir),
            "last_analysis_epoch": analyzed_at_epoch,
            "last_analysis_timestamp": timestamp,
            "artifact_location": str(cartography_dir),
            "available_artifacts": available,
            "artifacts": artifacts,
        }

    def _artifact_metadata(self, cartography_dir: Path) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for relative in REQUIRED_ARTIFACTS:
            path = cartography_dir / relative
            exists = path.exists()
            size_bytes = path.stat().st_size if exists and path.is_file() else 0
            entries.append(
                {
                    "name": relative,
                    "exists": exists,
                    "size_bytes": size_bytes,
                }
            )
        return entries

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {"active_repo_id": "", "sessions": []}
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return {"active_repo_id": "", "sessions": []}
            if "sessions" not in payload or not isinstance(payload["sessions"], list):
                payload["sessions"] = []
            if "active_repo_id" not in payload:
                payload["active_repo_id"] = ""
            return payload
        except Exception:
            return {"active_repo_id": "", "sessions": []}

    def _save_state(self, state: dict[str, Any]) -> None:
        self.state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _repo_id(self, repo_path: Path) -> str:
        digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:12]
        return f"repo_{digest}"

    def _format_timestamp(self, epoch: float) -> str:
        if epoch <= 0:
            return "Unknown"
        return datetime.fromtimestamp(epoch, UTC).isoformat().replace("+00:00", "Z")

    def _read_json(self, path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        return {}
