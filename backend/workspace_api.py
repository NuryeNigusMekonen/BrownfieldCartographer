from __future__ import annotations

import json
import socket
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any, Callable, TypeVar

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
import uvicorn

from src.orchestrator import CartographyOrchestrator
from src.repo import DEFAULT_WORKSPACE_REPO_ROOT, resolve_repo_input
from backend.sessions import WorkspaceSessionStore
from backend.workspace_data import CartographyWorkspaceData


ROOT_DIR = Path(__file__).resolve().parents[2]
FRONTEND_DIST_DIR = ROOT_DIR / "frontend" / "dist"
VENDOR_DIR = ROOT_DIR / "lib"
SESSION_STATE_FILE = ROOT_DIR / ".cartography_workspace" / "sessions.json"

T = TypeVar("T")


def _resolve_output_dir(repo_path: Path, output: str) -> Path:
    output_path = Path(output)
    if output_path.is_absolute():
        return output_path
    if output_path.parent == Path("."):
        return (repo_path / output_path).resolve()
    return output_path.resolve()


def _safe_execute(fn: Callable[[], T]) -> T:
    try:
        return fn()
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _safe_static_target(relative_path: str) -> Path | None:
    relative = relative_path.lstrip("/")
    if not relative:
        return None
    candidate = (FRONTEND_DIST_DIR / relative).resolve()
    if not candidate.is_relative_to(FRONTEND_DIST_DIR):
        return None
    return candidate


def _frontend_not_built() -> Response:
    return Response(
        "Frontend build not found. Run: cd frontend && npm install && npm run build",
        status_code=503,
        media_type="text/plain",
    )


def _assert_port_available(host: str, port: int) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
    except OSError as exc:
        raise RuntimeError(f"Could not bind to {host}:{port}. Try another port with --port.") from exc
    finally:
        sock.close()


class WorkspaceBackend:
    def __init__(self, default_cartography_dir: Path | None = None) -> None:
        self.store = WorkspaceSessionStore(SESSION_STATE_FILE)
        self.workspace_repo_root = DEFAULT_WORKSPACE_REPO_ROOT.resolve()
        self.workspace_repo_root.mkdir(parents=True, exist_ok=True)
        self._workspace_cache: dict[str, CartographyWorkspaceData] = {}
        self._lock = threading.Lock()

        if default_cartography_dir and default_cartography_dir.exists():
            self.store.register_cartography_dir(default_cartography_dir)
        self._discover_workspace_sessions()

    def sessions_payload(self) -> dict:
        self._discover_workspace_sessions()
        sessions = self._workspace_sessions()
        active_repo_id = self.store.active_repo_id()
        if active_repo_id and not any(str(item.get("repo_id")) == active_repo_id for item in sessions):
            active_repo_id = ""
        if not active_repo_id and sessions:
            active_repo_id = str(sessions[0]["repo_id"])
            self.store.set_active_repo_id(active_repo_id)
        return {
            "sessions": sessions,
            "active_repo_id": active_repo_id,
        }

    def session_payload(self, repo_id: str = "") -> dict:
        session = self._resolve_session(repo_id)
        return {
            "session": session,
            "active_repo_id": self.store.active_repo_id(),
        }

    def select_session(self, repo_id: str) -> dict:
        session = self.store.set_active_repo_id(repo_id)
        return {
            "session": session,
            "active_repo_id": repo_id,
        }

    def analyze_repo(self, payload: dict) -> dict:
        repo_input = str(payload.get("repo_input") or "").strip()
        if not repo_input:
            raise ValueError("repo_input is required")

        output = str(payload.get("output") or ".cartography")
        checkout_root_raw = str(payload.get("checkout_root") or "").strip()
        checkout_root = Path(checkout_root_raw).expanduser().resolve() if checkout_root_raw else None
        incremental = bool(payload.get("incremental", True))

        started = time.time()
        repo_path = resolve_repo_input(repo_input, checkout_root=checkout_root)
        out_dir = _resolve_output_dir(repo_path, output)
        orchestrator = CartographyOrchestrator(repo_path=repo_path, out_dir=out_dir)
        artifacts = orchestrator.analyze(incremental=incremental)
        session = self.store.upsert_session(repo_input=repo_input, repo_path=repo_path, cartography_dir=out_dir)

        with self._lock:
            self._workspace_cache.pop(session["repo_id"], None)

        return {
            "ok": True,
            "session": session,
            "artifacts": artifacts,
            "duration_seconds": round(time.time() - started, 2),
        }

    def _discover_workspace_sessions(self) -> None:
        if not self.workspace_repo_root.exists():
            return

        for repo_dir in sorted(self.workspace_repo_root.iterdir(), key=lambda item: item.name.lower()):
            if not repo_dir.is_dir():
                continue
            cartography_dir = repo_dir / ".cartography"
            if not cartography_dir.exists() or not cartography_dir.is_dir():
                continue
            self.store.register_cartography_dir(cartography_dir, set_active=False)

    def artifact_metadata_payload(self, repo_id: str = "") -> dict:
        session = self._resolve_session(repo_id)
        return {
            "repo_id": session["repo_id"],
            "cartography_dir": session["cartography_dir"],
            "artifacts": session.get("artifacts", []),
        }

    def artifact_payload(self, repo_id: str, name: str) -> dict:
        session = self._resolve_session(repo_id)
        if not name:
            raise ValueError("artifact name is required")

        cartography_dir = Path(session["cartography_dir"]).resolve()
        artifact_path = (cartography_dir / name).resolve()
        if not artifact_path.is_relative_to(cartography_dir):
            raise ValueError("Invalid artifact path")
        if not artifact_path.exists() or not artifact_path.is_file():
            raise FileNotFoundError(f"Artifact not found: {name}")

        suffix = artifact_path.suffix.lower()
        if suffix == ".json":
            content_type = "json"
            content = json.loads(artifact_path.read_text(encoding="utf-8"))
        elif suffix == ".jsonl":
            content_type = "jsonl"
            content = [
                json.loads(line)
                for line in artifact_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        else:
            content_type = "text"
            content = artifact_path.read_text(encoding="utf-8")

        return {
            "repo_id": session["repo_id"],
            "name": name,
            "path": str(artifact_path),
            "content_type": content_type,
            "content": content,
        }

    def workspace(self, repo_id: str = "") -> CartographyWorkspaceData:
        session = self._resolve_session(repo_id)
        repo_key = str(session["repo_id"])
        cartography_dir = Path(session["cartography_dir"]).resolve()
        if not cartography_dir.exists():
            raise FileNotFoundError(f"Artifacts directory not found: {cartography_dir}")

        with self._lock:
            workspace = self._workspace_cache.get(repo_key)
            if workspace is None:
                workspace = CartographyWorkspaceData(cartography_dir)
                self._workspace_cache[repo_key] = workspace
            return workspace

    def _resolve_session(self, repo_id: str) -> dict:
        self._discover_workspace_sessions()
        repo_id = repo_id.strip()
        if repo_id:
            session = self.store.get_session(repo_id)
            if self._session_in_workspace(session):
                return session
            raise FileNotFoundError(f"Unknown repo_id: {repo_id}")

        active = self.store.active_session()
        if active is not None and self._session_in_workspace(active):
            return active

        sessions = self._workspace_sessions()
        if sessions:
            first = sessions[0]
            self.store.set_active_repo_id(str(first["repo_id"]))
            return first

        raise FileNotFoundError("No repository session selected. Analyze a repository first.")

    def _workspace_sessions(self) -> list[dict]:
        sessions = self.store.list_sessions()
        return [session for session in sessions if self._session_in_workspace(session)]

    def _session_in_workspace(self, session: dict) -> bool:
        try:
            repo_path = Path(str(session.get("repo_path") or "")).resolve()
        except Exception:
            return False
        return repo_path.is_relative_to(self.workspace_repo_root)


def create_workspace_app(cartography_dir: Path | None = None) -> FastAPI:
    app = FastAPI(title="Brownfield Cartographer Workspace API")
    backend = WorkspaceBackend(default_cartography_dir=cartography_dir)
    app.state.backend = backend

    if VENDOR_DIR.exists():
        app.mount("/vendor", StaticFiles(directory=str(VENDOR_DIR)), name="vendor")

    assets_dir = FRONTEND_DIST_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/api/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/sessions")
    def sessions() -> dict[str, Any]:
        return _safe_execute(backend.sessions_payload)

    @app.get("/api/session")
    def session(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.session_payload(repo_id))

    @app.post("/api/session/select")
    def session_select(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
        repo_id = str(payload.get("repo_id") or "").strip()
        if not repo_id:
            raise HTTPException(status_code=400, detail="repo_id is required")
        return _safe_execute(lambda: backend.select_session(repo_id))

    @app.post("/api/analyze")
    def analyze(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
        return _safe_execute(lambda: backend.analyze_repo(payload))

    @app.get("/api/artifacts")
    def artifacts(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.artifact_metadata_payload(repo_id))

    @app.get("/api/artifact")
    def artifact(repo_id: str = Query(default=""), name: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.artifact_payload(repo_id, name))

    @app.get("/api/summary")
    def summary(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).summary_payload())

    @app.get("/api/module-graph")
    def module_graph(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).module_graph_payload())

    @app.get("/api/lineage-graph")
    def lineage_graph(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).lineage_graph_payload())

    @app.get("/api/knowledge")
    def knowledge(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).knowledge_payload())

    @app.get("/api/semanticist")
    def semanticist(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).semantic_payload())

    @app.get("/api/onboarding")
    def onboarding(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).onboarding_payload())

    @app.get("/api/archivist")
    def archivist(repo_id: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).archivist_payload())

    @app.get("/api/semantic-search")
    def semantic_search(repo_id: str = Query(default=""), q: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).semantic_search(q))

    @app.get("/api/query")
    def query(repo_id: str = Query(default=""), q: str = Query(default="")) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).run_query(q))

    @app.get("/api/node-details")
    def node_details(
        repo_id: str = Query(default=""),
        graph: str = Query(default="module"),
        id: str = Query(default=""),
    ) -> dict[str, Any]:
        return _safe_execute(lambda: backend.workspace(repo_id).node_details(graph, id))

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        target = _safe_static_target("favicon.ico")
        if target and target.exists() and target.is_file():
            return FileResponse(target)
        return Response(status_code=204)

    @app.get("/", include_in_schema=False)
    def index() -> Response:
        index_file = FRONTEND_DIST_DIR / "index.html"
        if not index_file.exists():
            return _frontend_not_built()
        return FileResponse(index_file)

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str) -> Response:
        # API and vendor are already routed; preserve 404 behavior for unknown explicit assets.
        if path.startswith("api/") or path.startswith("vendor/"):
            raise HTTPException(status_code=404, detail="Not Found")

        target = _safe_static_target(path)
        if target and target.exists() and target.is_file():
            return FileResponse(target)

        if "." in Path(path).name:
            raise HTTPException(status_code=404, detail="Not Found")

        index_file = FRONTEND_DIST_DIR / "index.html"
        if not index_file.exists():
            return _frontend_not_built()
        return FileResponse(index_file)

    return app


def serve_workspace(
    cartography_dir: Path | None,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = True,
) -> None:
    app = create_workspace_app(cartography_dir)
    _assert_port_available(host, port)
    url = f"http://{host}:{port}"
    if open_browser:
        webbrowser.open_new_tab(url)
    print(f"Workspace running at {url}")
    if cartography_dir:
        print(f"Default artifacts: {cartography_dir}")
    print(f"Session state: {SESSION_STATE_FILE}")

    config = uvicorn.Config(app=app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    try:
        server.run()
    except KeyboardInterrupt:
        pass
