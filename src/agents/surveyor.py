from __future__ import annotations

from collections.abc import Callable
import math
from datetime import datetime
from pathlib import Path

from src.analyzers.git_history import compute_git_velocity_snapshot
from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import FunctionNode, ModuleNode, TraceEvent


class SurveyorAgent:
    def __init__(self) -> None:
        self.analyzer = TreeSitterAnalyzer()

    def run(
        self,
        repo_path: Path,
        include_files: set[str] | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> tuple[KnowledgeGraph, dict[str, ModuleNode], list[TraceEvent]]:
        kg = KnowledgeGraph()
        modules: dict[str, ModuleNode] = {}
        function_nodes: dict[str, FunctionNode] = {}
        trace: list[TraceEvent] = []
        velocity_snapshot_30d = compute_git_velocity_snapshot(repo_path, days=30)
        velocity_30d = {item.path: item.commit_count for item in velocity_snapshot_30d.files}
        ranked_velocity = sorted(velocity_snapshot_30d.files, key=lambda item: (-item.commit_count, item.path))
        velocity_rank_by_file = {item.path: idx + 1 for idx, item in enumerate(ranked_velocity)}
        top_high_velocity_files = [
            {
                "analysis_method": "git_log_frequency",
                "path": item.path,
                "change_velocity_30d": item.commit_count,
                "commit_count": item.commit_count,
                "time_window_days": velocity_snapshot_30d.time_window_days,
                "last_commit_timestamp": item.last_commit_timestamp,
            }
            for item in ranked_velocity[:10]
            if item.commit_count > 0
        ]
        high_velocity_core = set(self.identify_high_velocity_core(velocity_30d, file_fraction=0.2, change_fraction=0.8))
        file_errors: list[dict[str, str]] = []
        selected_files: list[tuple[Path, str]] = []
        for file_path in self.analyzer.iter_supported_files(repo_path):
            rel = str(file_path.relative_to(repo_path))
            if include_files is not None and rel not in include_files:
                continue
            selected_files.append((file_path, rel))

        if progress_callback:
            progress_callback(f"Surveyor: analyzing {len(selected_files)} supported files.")

        for idx, (file_path, rel) in enumerate(selected_files, start=1):
            try:
                analysis = self.analyzer.analyze_module(file_path, repo_path)
                velocity = velocity_30d.get(analysis.path, 0)
                module = ModuleNode(
                    path=analysis.path,
                    language=analysis.language,
                    complexity_score=analysis.complexity_score,
                    change_velocity_30d=velocity,
                    velocity_rank_30d=velocity_rank_by_file.get(analysis.path, 0),
                    is_high_velocity_core=analysis.path in high_velocity_core,
                    imports=analysis.imports,
                    public_functions=analysis.public_functions,
                    classes=analysis.classes,
                    class_inheritance=analysis.class_inheritance,
                    loc=analysis.loc,
                    comment_ratio=analysis.comment_ratio,
                    last_modified=self.get_last_modified_iso(file_path),
                )
                modules[module.path] = module
                kg.add_module_node(module)
                for fn_name, signature in analysis.function_signatures.items():
                    qname = f"{module.path}::{fn_name}"
                    fn = FunctionNode(
                        qualified_name=qname,
                        parent_module=module.path,
                        signature=signature,
                        is_public_api=fn_name in module.public_functions,
                    )
                    function_nodes[qname] = fn
                    kg.add_function_node(fn)
                    kg.add_configures_edge(module.path, qname, analysis_method="python_ast")

                for caller, callee in analysis.function_calls:
                    src = f"{module.path}::{caller}"
                    # intra-module call edges (direct)
                    dst = f"{module.path}::{callee}"
                    if src in function_nodes and dst in function_nodes:
                        function_nodes[src].call_count_within_repo += 1
                        kg.graph.nodes[src]["call_count_within_repo"] = function_nodes[src].call_count_within_repo
                        kg.add_calls_edge(src, dst, analysis_method="python_ast")
            except Exception as exc:
                file_errors.append({"file": rel, "error": str(exc)})
                if progress_callback:
                    progress_callback(f"Surveyor: skipping {rel} due to error: {exc}")
                continue

            if progress_callback and (idx % 25 == 0 or idx == len(selected_files)):
                progress_callback(f"Surveyor: analyzed {idx}/{len(selected_files)} files.")

        path_lookup = set(modules.keys())
        for module in modules.values():
            for imp in module.imports:
                target_path = self._guess_import_target(imp, module.path, path_lookup)
                if target_path:
                    kg.add_imports_edge(module.path, target_path, weight=1.0)

        import_graph = kg.module_import_graph()
        pagerank_scores = kg.pagerank(module_import_only=True)
        scc_components = self._cycle_components(kg)
        cycle_by_module = self._cycle_membership(scc_components)

        # Dead code candidate heuristic: exported symbols with no inbound imports.
        dead_code_candidates: list[dict[str, object]] = []
        for module in modules.values():
            in_degree = import_graph.in_degree(module.path) if module.path in import_graph else 0
            dead_code_symbols = sorted(module.public_functions) if module.public_functions and in_degree == 0 else []
            module.dead_code_symbols = dead_code_symbols
            module.is_dead_code_candidate = bool(dead_code_symbols)
            module.pagerank_score = pagerank_scores.get(module.path, 0.0)

            cycle_info = cycle_by_module.get(module.path)
            module.is_in_import_cycle = cycle_info is not None
            module.import_cycle_id = cycle_info["cycle_id"] if cycle_info else ""
            module.import_cycle_size = cycle_info["size"] if cycle_info else 0
            module.import_cycle_members = cycle_info["members"] if cycle_info else []

            kg.graph.nodes[module.path]["dead_code_symbols"] = module.dead_code_symbols
            kg.graph.nodes[module.path]["is_dead_code_candidate"] = module.is_dead_code_candidate
            kg.graph.nodes[module.path]["pagerank_score"] = module.pagerank_score
            kg.graph.nodes[module.path]["is_in_import_cycle"] = module.is_in_import_cycle
            kg.graph.nodes[module.path]["import_cycle_id"] = module.import_cycle_id
            kg.graph.nodes[module.path]["import_cycle_size"] = module.import_cycle_size
            kg.graph.nodes[module.path]["import_cycle_members"] = module.import_cycle_members
            kg.graph.nodes[module.path]["import_in_degree"] = in_degree
            if module.is_dead_code_candidate:
                dead_code_candidates.append({"path": module.path, "symbols": module.dead_code_symbols})

        top_pagerank_modules = [
            {"path": path, "pagerank_score": score}
            for path, score in sorted(pagerank_scores.items(), key=lambda kv: kv[1], reverse=True)[:10]
        ]
        structured_cycles = [
            {"cycle_id": f"cycle_{idx + 1}", "size": len(component), "members": component}
            for idx, component in enumerate(scc_components)
        ]
        kg.graph.graph["surveyor_insights"] = {
            "top_high_velocity_files": top_high_velocity_files,
            "top_pagerank_modules": top_pagerank_modules,
            "import_cycles": structured_cycles,
            "dead_code_candidates": dead_code_candidates,
            "git_velocity_status_30d": velocity_snapshot_30d.history_status,
            "git_velocity_note_30d": velocity_snapshot_30d.history_note,
            "git_velocity_commit_events_30d": velocity_snapshot_30d.commit_events_scanned,
        }

        trace.append(
            TraceEvent(
                agent="surveyor",
                action="module_graph_built",
                evidence={
                    "modules": len(modules),
                    "functions": len(function_nodes),
                    "edges": kg.graph.number_of_edges(),
                    "high_velocity_core_count": len(high_velocity_core),
                    "top_high_velocity_files": top_high_velocity_files,
                    "git_velocity_status_30d": velocity_snapshot_30d.history_status,
                    "git_velocity_note_30d": velocity_snapshot_30d.history_note,
                    "top_pagerank_modules": top_pagerank_modules,
                    "import_cycle_count": len(scc_components),
                    "dead_code_candidate_count": len(dead_code_candidates),
                },
                confidence="high",
            )
        )
        if file_errors:
            trace.append(
                TraceEvent(
                    agent="surveyor",
                    action="files_skipped_on_error",
                    evidence={
                        "failed_file_count": len(file_errors),
                        "failed_files": file_errors[:100],
                    },
                    confidence="medium",
                )
            )
        return kg, modules, trace

    def extract_git_velocity(self, repo_path: Path, rel_path: str, days: int = 30) -> int:
        velocity = self.velocity_map(repo_path, days=days)
        return int(velocity.get(rel_path, 0))

    def velocity_map(self, repo_path: Path, days: int = 90) -> dict[str, int]:
        snapshot = compute_git_velocity_snapshot(repo_path, days=days)
        return {item.path: item.commit_count for item in snapshot.files}

    def identify_high_velocity_core(
        self, velocity_by_file: dict[str, int], file_fraction: float = 0.2, change_fraction: float = 0.8
    ) -> list[str]:
        if not velocity_by_file:
            return []
        ranked = sorted(velocity_by_file.items(), key=lambda kv: kv[1], reverse=True)
        total_changes = sum(count for _, count in ranked)
        if total_changes <= 0:
            return []

        min_top_files = max(1, math.ceil(len(ranked) * file_fraction))
        selected: list[str] = []
        cumulative = 0

        for idx, (path, count) in enumerate(ranked):
            selected.append(path)
            cumulative += count
            if idx + 1 >= min_top_files and (cumulative / total_changes) >= change_fraction:
                break
        return selected

    def get_last_modified_iso(self, file_path: Path) -> str:
        try:
            ts = file_path.stat().st_mtime
            return datetime.utcfromtimestamp(ts).isoformat() + "Z"
        except Exception:
            return ""

    def _guess_import_target(self, module_import: str, importer_path: str, available_paths: set[str]) -> str | None:
        resolved_module = self._resolve_import_path(module_import, importer_path)
        if not resolved_module:
            return None
        dotted_candidates = self._dotted_import_candidates(resolved_module, available_paths)
        for dotted in dotted_candidates:
            path_stem = dotted.replace(".", "/")
            for candidate in (f"{path_stem}.py", f"{path_stem}/__init__.py"):
                if candidate in available_paths:
                    return candidate
        return None

    def _resolve_import_path(self, module_import: str, importer_path: str) -> str:
        stripped = module_import.strip()
        if not stripped:
            return ""
        if not stripped.startswith("."):
            return stripped

        leading = len(stripped) - len(stripped.lstrip("."))
        remainder = stripped[leading:]
        importer_parts = Path(importer_path).with_suffix("").parts[:-1]
        # One dot = current package, two dots = parent package, etc.
        up_levels = max(0, leading - 1)
        if up_levels > len(importer_parts):
            return ""
        base_parts = list(importer_parts[: len(importer_parts) - up_levels])
        if remainder:
            base_parts.extend([part for part in remainder.split(".") if part])
        return ".".join(base_parts)

    def _dotted_import_candidates(self, resolved_module: str, available_paths: set[str]) -> list[str]:
        candidates: list[str] = []
        current = resolved_module
        while current:
            candidates.append(current)
            if "." not in current:
                break
            current = current.rsplit(".", 1)[0]

        # Support src-layout repositories where imports use package names but files live under src/.
        if any(path.startswith("src/") for path in available_paths):
            prefixed: list[str] = []
            for candidate in candidates:
                src_candidate = f"src.{candidate}"
                if src_candidate not in candidates:
                    prefixed.append(src_candidate)
            candidates.extend(prefixed)

        seen: set[str] = set()
        ordered: list[str] = []
        for candidate in candidates:
            if candidate and candidate not in seen:
                seen.add(candidate)
                ordered.append(candidate)
        return ordered

    def _cycle_components(self, graph: KnowledgeGraph) -> list[list[str]]:
        components = graph.strongly_connected_components(module_import_only=True)
        return sorted(components, key=lambda comp: (-len(comp), comp[0] if comp else ""))

    def _cycle_membership(self, components: list[list[str]]) -> dict[str, dict[str, object]]:
        membership: dict[str, dict[str, object]] = {}
        for idx, component in enumerate(components):
            cycle_id = f"cycle_{idx + 1}"
            size = len(component)
            for module_path in component:
                membership[module_path] = {"cycle_id": cycle_id, "size": size, "members": component}
        return membership
