from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ConfigEdge:
    source: str
    target: str
    relation: str
    source_file: str
    line_range: tuple[int, int] = (0, 0)
    transformation_type: str = "config_dependency"


class DAGConfigAnalyzer:
    def parse(self, path: Path, repo_root: Path) -> list[ConfigEdge]:
        rel = str(path.relative_to(repo_root))
        raw_text = path.read_text(encoding="utf-8", errors="ignore")
        try:
            data = yaml.safe_load(raw_text)
        except Exception:
            return []

        if not data:
            return []

        edges: list[ConfigEdge] = []
        # dbt schema.yml style: models: - name: ... depends_on: ...
        models = data.get("models") if isinstance(data, dict) else None
        if isinstance(models, list):
            for model in models:
                if not isinstance(model, dict):
                    continue
                target = str(model.get("name", ""))
                depends = model.get("depends_on", [])
                if isinstance(depends, dict):
                    depends = depends.get("nodes", [])
                elif isinstance(depends, str):
                    depends = [depends]
                if isinstance(depends, list):
                    for src in depends:
                        source = str(src).strip()
                        if not source or not target:
                            continue
                        edges.append(
                            ConfigEdge(
                                source=source,
                                target=target,
                                relation="CONFIGURES",
                                source_file=rel,
                                line_range=self._line_range_from_text(raw_text, source, target),
                                transformation_type="dbt_schema_dependency",
                            )
                        )

        # generic DAG blocks with tasks and downstream/upstream references
        edges.extend(self._extract_generic_dag_edges(data, rel, raw_text))
        return edges

    def _extract_generic_dag_edges(self, data: Any, source_file: str, raw_text: str) -> list[ConfigEdge]:
        edges: list[ConfigEdge] = []
        if isinstance(data, dict):
            tasks = data.get("tasks")
            if isinstance(tasks, list):
                for task in tasks:
                    if not isinstance(task, dict):
                        continue
                    task_id = str(task.get("id") or task.get("task_id") or "")
                    downstream = task.get("downstream") or task.get("downstream_task_ids") or []
                    if task_id and isinstance(downstream, list):
                        for d in downstream:
                            edges.append(
                                ConfigEdge(
                                    source=task_id,
                                    target=str(d),
                                    relation="CONFIGURES",
                                    source_file=source_file,
                                    line_range=self._line_range_from_text(raw_text, task_id, str(d)),
                                    transformation_type="yaml_task_dependency",
                                )
                            )
        return edges

    def parse_airflow_python(self, path: Path, repo_root: Path) -> list[ConfigEdge]:
        rel = str(path.relative_to(repo_root))
        text = path.read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(text)
        except SyntaxError:
            return []
        edges: list[ConfigEdge] = []
        for node in ast.walk(tree):
            # task_a >> task_b
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
                left_items = self._names(node.left)
                right_items = self._names(node.right)
                for left in left_items:
                    for right in right_items:
                        edges.append(
                            ConfigEdge(
                                source=left,
                                target=right,
                                relation="CONFIGURES",
                                source_file=rel,
                                line_range=self._node_line_range(node),
                                transformation_type="airflow_task_dependency",
                            )
                        )
            # task_b << task_a
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.LShift):
                left_items = self._names(node.left)
                right_items = self._names(node.right)
                for left in left_items:
                    for right in right_items:
                        edges.append(
                            ConfigEdge(
                                source=right,
                                target=left,
                                relation="CONFIGURES",
                                source_file=rel,
                                line_range=self._node_line_range(node),
                                transformation_type="airflow_task_dependency",
                            )
                        )
            # task_a.set_downstream(task_b)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                owners = self._names(node.func.value)
                method = node.func.attr
                if owners and node.args:
                    others = self._names(node.args[0])
                    if not others:
                        continue
                    for owner in owners:
                        for other in others:
                            if method == "set_downstream":
                                edges.append(
                                    ConfigEdge(
                                        source=owner,
                                        target=other,
                                        relation="CONFIGURES",
                                        source_file=rel,
                                        line_range=self._node_line_range(node),
                                        transformation_type="airflow_task_dependency",
                                    )
                                )
                            elif method == "set_upstream":
                                edges.append(
                                    ConfigEdge(
                                        source=other,
                                        target=owner,
                                        relation="CONFIGURES",
                                        source_file=rel,
                                        line_range=self._node_line_range(node),
                                        transformation_type="airflow_task_dependency",
                                    )
                                )
        return edges

    def _name(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    def _names(self, node: ast.AST) -> list[str]:
        single = self._name(node)
        if single:
            return [single]
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            names: list[str] = []
            for item in node.elts:
                names.extend(self._names(item))
            return sorted(set(names))
        return []

    def _node_line_range(self, node: ast.AST) -> tuple[int, int]:
        start = max(0, int(getattr(node, "lineno", 0) or 0))
        end = max(start, int(getattr(node, "end_lineno", start) or start))
        return (start, end)

    def _line_range_from_text(self, text: str, *anchors: str) -> tuple[int, int]:
        tokens = [token.strip() for token in anchors if token and token.strip()]
        if not tokens:
            return (0, 0)
        lowered_lines = [line.lower() for line in text.splitlines()]
        for idx, line in enumerate(lowered_lines, start=1):
            if all(token.lower() in line for token in tokens):
                return (idx, idx)
        for idx, line in enumerate(lowered_lines, start=1):
            if any(token.lower() in line for token in tokens):
                return (idx, idx)
        return (0, 0)
