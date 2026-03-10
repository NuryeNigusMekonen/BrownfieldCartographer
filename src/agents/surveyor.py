from __future__ import annotations

import subprocess
import math
from datetime import datetime, timedelta
from pathlib import Path

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import EdgeType, FunctionNode, ModuleNode, TraceEvent


class SurveyorAgent:
    def __init__(self) -> None:
        self.analyzer = TreeSitterAnalyzer()

    def run(
        self, repo_path: Path, include_files: set[str] | None = None
    ) -> tuple[KnowledgeGraph, dict[str, ModuleNode], list[TraceEvent]]:
        kg = KnowledgeGraph()
        modules: dict[str, ModuleNode] = {}
        function_nodes: dict[str, FunctionNode] = {}
        trace: list[TraceEvent] = []
        velocity_30d = self.velocity_map(repo_path, days=30)
        high_velocity_core = set(self.identify_high_velocity_core(velocity_30d, file_fraction=0.2, change_fraction=0.8))

        for file_path in self.analyzer.iter_supported_files(repo_path):
            rel = str(file_path.relative_to(repo_path))
            if include_files is not None and rel not in include_files:
                continue
            analysis = self.analyzer.analyze_module(file_path, repo_path)
            velocity = velocity_30d.get(analysis.path, 0)
            module = ModuleNode(
                path=analysis.path,
                language=analysis.language,
                complexity_score=analysis.complexity_score,
                change_velocity_30d=velocity,
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
            kg.add_node(module.path, "module", **module.model_dump())
            for fn_name, signature in analysis.function_signatures.items():
                qname = f"{module.path}::{fn_name}"
                fn = FunctionNode(
                    qualified_name=qname,
                    parent_module=module.path,
                    signature=signature,
                    is_public_api=fn_name in module.public_functions,
                )
                function_nodes[qname] = fn
                kg.add_node(qname, "function", **fn.model_dump())
                kg.add_edge(module.path, qname, EdgeType.CONFIGURES, analysis_method="python_ast")

            for caller, callee in analysis.function_calls:
                src = f"{module.path}::{caller}"
                # intra-module call edges (direct)
                dst = f"{module.path}::{callee}"
                if src in function_nodes and dst in function_nodes:
                    function_nodes[src].call_count_within_repo += 1
                    kg.graph.nodes[src]["call_count_within_repo"] = function_nodes[src].call_count_within_repo
                    kg.add_edge(src, dst, EdgeType.CALLS, analysis_method="python_ast")

        path_lookup = set(modules.keys())
        for module in modules.values():
            for imp in module.imports:
                target_path = self._guess_import_target(imp, module.path, path_lookup)
                if target_path:
                    kg.add_edge(module.path, target_path, EdgeType.IMPORTS, weight=1.0)

        # Dead code candidate heuristic: public symbols but no inbound imports.
        import_graph = kg.module_import_graph()
        for module in modules.values():
            if module.public_functions and import_graph.in_degree(module.path) == 0:
                module.is_dead_code_candidate = True
                kg.graph.nodes[module.path]["is_dead_code_candidate"] = True

        trace.append(
            TraceEvent(
                agent="surveyor",
                action="module_graph_built",
                evidence={
                    "modules": len(modules),
                    "functions": len(function_nodes),
                    "edges": kg.graph.number_of_edges(),
                    "high_velocity_core_count": len(high_velocity_core),
                },
                confidence="high",
            )
        )
        return kg, modules, trace

    def extract_git_velocity(self, repo_path: Path, rel_path: str, days: int = 30) -> int:
        since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        cmd = [
            "git",
            "-C",
            str(repo_path),
            "log",
            "--since",
            since,
            "--follow",
            "--",
            rel_path,
        ]
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, check=False)
        except Exception:
            return 0
        if out.returncode != 0:
            return 0
        return len([line for line in out.stdout.splitlines() if line.startswith("commit ")])

    def velocity_map(self, repo_path: Path, days: int = 90) -> dict[str, int]:
        since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        cmd = ["git", "-C", str(repo_path), "log", "--since", since, "--name-only", "--pretty=format:"]
        out = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if out.returncode != 0:
            return {}
        counts: dict[str, int] = {}
        for line in out.stdout.splitlines():
            rel = line.strip()
            if not rel:
                continue
            counts[rel] = counts.get(rel, 0) + 1
        return counts

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
