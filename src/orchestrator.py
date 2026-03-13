from __future__ import annotations

from collections.abc import Callable
import json
import subprocess
import time
from pathlib import Path
from typing import Any

from src.analyzers.git_history import GitVelocitySnapshot, compute_git_velocity_snapshot
from src.agents.archivist import ArchivistAgent
from src.agents.hydrologist import HydrologistAgent
from src.agents.semanticist import SemanticistAgent
from src.agents.surveyor import SurveyorAgent
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ModuleNode, TraceEvent
from src.repo import repository_metadata


class CartographyOrchestrator:
    def __init__(
        self,
        repo_path: Path,
        out_dir: Path | None = None,
        repo_input: str | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.repo_path = repo_path.resolve()
        self.out_dir = out_dir or (self.repo_path / ".cartography")
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.out_dir / "state.json"
        self.repo_input = (repo_input or str(self.repo_path)).strip()
        self._progress_callback = progress_callback or self._default_progress

        self.surveyor = SurveyorAgent()
        self.hydrologist = HydrologistAgent()
        self.semanticist = SemanticistAgent()
        self.archivist = ArchivistAgent(self.out_dir)

    def analyze(self, incremental: bool = True) -> dict[str, str]:
        self._progress(f"Starting analysis for {self.repo_path}")
        stage = "bootstrap"
        trace: list[TraceEvent] = []
        module_graph: KnowledgeGraph | None = None
        lineage_graph: KnowledgeGraph | None = None
        modules: dict[str, ModuleNode] = {}
        day_one: dict[str, Any] = {}
        top_modules: list[str] = []
        scc: list[list[str]] = []
        sources: list[str] = []
        sinks: list[str] = []
        git_velocity_90d: GitVelocitySnapshot | None = None
        changed_files = self.changed_files_since_last_run() if incremental else []
        use_incremental = bool(changed_files) and self._has_previous_artifacts()
        try:
            if use_incremental:
                stage = "incremental_reanalysis"
                self._progress(f"Incremental mode: re-analyzing {len(changed_files)} changed files.")
                module_graph, modules, lineage_graph, tr = self._analyze_incremental(changed_files)
                trace.extend(tr)
            else:
                stage = "surveyor"
                self._progress("Running Surveyor agent.")
                module_graph, modules, tr = self.surveyor.run(self.repo_path, progress_callback=self._progress)
                trace.extend(tr)
                stage = "hydrologist"
                self._progress("Running Hydrologist agent.")
                lineage_graph, tr = self.hydrologist.run(
                    self.repo_path,
                    lineage_graph=KnowledgeGraph(),
                    progress_callback=self._progress,
                )
                trace.extend(tr)

            stage = "checkpoint_structural_graphs"
            self._checkpoint_partial_artifacts(
                module_graph=module_graph,
                lineage_graph=lineage_graph,
                modules=modules,
                trace=trace,
            )

            stage = "semanticist"
            self._progress("Running Semanticist agent.")
            modules, tr = self.semanticist.run(self.repo_path, modules)
            trace.extend(tr)
            git_velocity_90d = compute_git_velocity_snapshot(self.repo_path, days=90)
            self._apply_git_velocity_metrics(modules, git_velocity_90d, days=90)

            # Inject semantic metadata into module graph.
            for path, module in modules.items():
                if module_graph is not None and path in module_graph.graph.nodes:
                    module_graph.graph.nodes[path].update(module.model_dump())

            if module_graph is None or lineage_graph is None:
                raise RuntimeError("Structural graphs are unavailable after analysis stage.")

            stage = "day_one_synthesis"
            pagerank = module_graph.pagerank(module_import_only=True)
            top_modules = [k for k, _ in sorted(pagerank.items(), key=lambda kv: kv[1], reverse=True)[:5]]
            scc = module_graph.strongly_connected_components(module_import_only=True)
            sources = self.hydrologist.find_sources(lineage_graph)
            sinks = self.hydrologist.find_sinks(lineage_graph)
            downstream_map = {m: module_graph.downstream(m) for m in top_modules}
            day_one = self.semanticist.answer_day_one_questions(
                list(modules.values()),
                top_modules,
                sources,
                sinks,
                downstream_map,
                module_graph,
                lineage_graph,
                git_velocity_snapshot=git_velocity_90d,
            )
            trace.append(
                TraceEvent(
                    agent="orchestrator",
                    action="day_one_questions_answered",
                    evidence={
                        "top_modules": top_modules,
                        "source_count": len(sources),
                        "sink_count": len(sinks),
                        "git_velocity_status_90d": git_velocity_90d.history_status if git_velocity_90d else "unknown",
                        "git_velocity_note_90d": git_velocity_90d.history_note if git_velocity_90d else "",
                        "git_velocity_file_count_90d": len(git_velocity_90d.files) if git_velocity_90d else 0,
                    },
                    confidence="medium",
                )
            )

            stage = "final_serialization"
            self._progress(f"Serializing artifacts to {self.out_dir}")
            module_graph_path = self.archivist.write_module_graph(module_graph)
            lineage_graph_path = self.archivist.write_lineage_graph(lineage_graph)
            semantic_index_path = self.archivist.write_semantic_index(modules)
            codebase_path = self.archivist.generate_codebase_md(
                modules,
                top_modules,
                scc,
                sources,
                sinks,
                git_velocity_snapshot=git_velocity_90d,
            )
            brief_path = self.archivist.generate_onboarding_brief(day_one)
            trace_path = self.archivist.write_trace(trace)
            self._save_state()
            self._progress("Analysis complete.")

            return {
                "module_graph": str(module_graph_path),
                "lineage_graph": str(lineage_graph_path),
                "semantic_index": str(semantic_index_path),
                "codebase_md": str(codebase_path),
                "onboarding_brief": str(brief_path),
                "trace": str(trace_path),
            }
        except Exception as exc:
            trace.append(
                TraceEvent(
                    agent="orchestrator",
                    action="analysis_failed",
                    evidence={
                        "failed_stage": stage,
                        "error": str(exc),
                    },
                    confidence="high",
                )
            )
            self._progress(f"Analysis failed during '{stage}'; writing partial artifacts.")
            self._checkpoint_partial_artifacts(
                module_graph=module_graph,
                lineage_graph=lineage_graph,
                modules=modules,
                trace=trace,
            )
            raise

    def _apply_git_velocity_metrics(
        self,
        modules: dict[str, ModuleNode],
        snapshot: GitVelocitySnapshot,
        days: int,
    ) -> None:
        by_path = snapshot.by_path()
        for module in modules.values():
            metric = by_path.get(module.path)
            if metric is None:
                module.git_velocity_commit_count = 0
                module.git_velocity_last_commit_timestamp = ""
            else:
                module.git_velocity_commit_count = int(metric.commit_count)
                module.git_velocity_last_commit_timestamp = metric.last_commit_timestamp
            module.git_velocity_time_window_days = int(days)
            module.git_velocity_history_status = snapshot.history_status

    def _save_state(self) -> None:
        metadata = repository_metadata(self.repo_input, self.repo_path)
        data = {
            "head": self._git_head(),
            "analyzed_at_epoch": time.time(),
            "repository": {
                "owner": metadata["owner"],
                "repo_name": metadata["repo_name"],
                "branch": metadata["branch"],
                "display_name": metadata["display_name"],
                "url": metadata["repo_url"],
            },
        }
        self.state_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def changed_files_since_last_run(self) -> list[str]:
        if not self.state_file.exists():
            return []
        try:
            state = json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception:
            return []
        prev = state.get("head")
        if not prev:
            return self._changed_files_by_mtime(state.get("analyzed_at_epoch"))
        changed_from_log = self._changed_files_by_git_log(str(prev))
        if changed_from_log is None:
            return self._changed_files_by_mtime(state.get("analyzed_at_epoch"))
        changed_from_mtime = self._changed_files_by_mtime(state.get("analyzed_at_epoch"))
        return sorted(set(changed_from_log) | set(changed_from_mtime))

    def _git_head(self) -> str:
        cmd = ["git", "-C", str(self.repo_path), "rev-parse", "HEAD"]
        out = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if out.returncode != 0:
            return ""
        return out.stdout.strip()

    def _has_previous_artifacts(self) -> bool:
        return (self.out_dir / "module_graph.json").exists() and (self.out_dir / "lineage_graph.json").exists()

    def _changed_files_by_git_log(self, previous_head: str) -> list[str] | None:
        cmd = [
            "git",
            "-C",
            str(self.repo_path),
            "log",
            "--name-only",
            "--pretty=format:",
            f"{previous_head}..HEAD",
        ]
        out = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if out.returncode != 0:
            return None
        changed = {
            ln.strip()
            for ln in out.stdout.splitlines()
            if ln.strip()
        }
        return sorted(changed)

    def _changed_files_by_mtime(self, analyzed_at_epoch: float | None) -> list[str]:
        if not analyzed_at_epoch:
            return []
        changed: list[str] = []
        for path in self.repo_path.rglob("*"):
            if not path.is_file():
                continue
            if (
                ".cartography" in path.parts
                or ".venv" in path.parts
                or ".git" in path.parts
                or "__pycache__" in path.parts
                or path.suffix == ".pyc"
            ):
                continue
            try:
                if path.stat().st_mtime > analyzed_at_epoch:
                    changed.append(str(path.relative_to(self.repo_path)))
            except OSError:
                continue
        return sorted(changed)

    def _analyze_incremental(
        self, changed_files: list[str]
    ) -> tuple[KnowledgeGraph, dict[str, ModuleNode], KnowledgeGraph, list[TraceEvent]]:
        trace: list[TraceEvent] = []
        include = set(changed_files)
        self._progress(f"Loading existing artifacts from {self.out_dir}")

        module_graph = KnowledgeGraph.load(self.out_dir / "module_graph.json")
        lineage_graph = KnowledgeGraph.load(self.out_dir / "lineage_graph.json")
        modules = self._module_nodes_from_graph(module_graph)

        self._prune_module_graph(module_graph, modules, include)
        self._prune_lineage_graph(lineage_graph, include)

        self._progress("Running Surveyor agent on changed files.")
        fresh_module_graph, fresh_modules, tr = self.surveyor.run(
            self.repo_path,
            include_files=include,
            progress_callback=self._progress,
        )
        trace.extend(tr)
        self._progress("Running Hydrologist agent on changed files.")
        fresh_lineage_graph, tr = self.hydrologist.run(
            self.repo_path,
            lineage_graph=KnowledgeGraph(),
            include_files=include,
            progress_callback=self._progress,
        )
        trace.extend(tr)

        module_graph.graph.update(fresh_module_graph.graph)
        lineage_graph.graph.update(fresh_lineage_graph.graph)
        modules.update(fresh_modules)

        trace.append(
            TraceEvent(
                agent="orchestrator",
                action="incremental_update",
                evidence={"changed_files": changed_files, "re_analyzed": len(include)},
                confidence="high",
            )
        )
        return module_graph, modules, lineage_graph, trace

    def _module_nodes_from_graph(self, graph: KnowledgeGraph) -> dict[str, ModuleNode]:
        out: dict[str, ModuleNode] = {}
        for node_id, attrs in graph.graph.nodes(data=True):
            if attrs.get("node_type") != "module":
                continue
            payload = dict(attrs)
            payload.pop("node_type", None)
            try:
                out[node_id] = ModuleNode(**payload)
            except Exception:
                continue
        return out

    def _prune_module_graph(
        self, graph: KnowledgeGraph, modules: dict[str, ModuleNode], changed: set[str]
    ) -> None:
        for rel in changed:
            if rel in graph.graph:
                graph.graph.remove_node(rel)
            modules.pop(rel, None)

    def _prune_lineage_graph(self, graph: KnowledgeGraph, changed: set[str]) -> None:
        to_remove: set[str] = set()
        for node_id, attrs in graph.graph.nodes(data=True):
            source_file = attrs.get("source_file")
            if isinstance(source_file, str) and source_file in changed:
                to_remove.add(node_id)
        for node in to_remove:
            if node in graph.graph:
                graph.graph.remove_node(node)

    def _checkpoint_partial_artifacts(
        self,
        *,
        module_graph: KnowledgeGraph | None,
        lineage_graph: KnowledgeGraph | None,
        modules: dict[str, ModuleNode] | None,
        trace: list[TraceEvent],
    ) -> None:
        try:
            if module_graph is not None:
                self.archivist.write_module_graph(module_graph)
            if lineage_graph is not None:
                self.archivist.write_lineage_graph(lineage_graph)
            if modules:
                self.archivist.write_semantic_index(modules)
            self.archivist.write_trace(trace)
        except Exception as checkpoint_error:
            self._progress(f"Partial artifact checkpoint failed: {checkpoint_error}")

    def _progress(self, message: str) -> None:
        self._progress_callback(message)

    def _default_progress(self, message: str) -> None:
        print(f"[orchestrator] {message}")
