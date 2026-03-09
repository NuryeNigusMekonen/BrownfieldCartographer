from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import sqlglot
from sqlglot import expressions as exp
from tree_sitter import Language, Parser
import tree_sitter_python


@dataclass
class PythonDataFlowEvent:
    source_file: str
    line_range: tuple[int, int]
    flow_type: str  # CONSUMES | PRODUCES
    dataset: str
    storage_type: str  # table | file
    analysis_method: str
    unresolved: bool = False


class PythonDataFlowAnalyzer:
    DYNAMIC_REFERENCE = "dynamic reference, cannot resolve"

    def __init__(self) -> None:
        self.parser = Parser(Language(tree_sitter_python.language()))

    def extract_from_file(self, path: Path, repo_root: Path) -> list[PythonDataFlowEvent]:
        source = path.read_text(encoding="utf-8", errors="ignore")
        rel = str(path.relative_to(repo_root))
        tree = self.parser.parse(source.encode("utf-8", errors="ignore"))
        root = tree.root_node
        variables = self._extract_string_variables(root)
        out: list[PythonDataFlowEvent] = []

        for node in self._walk(root):
            if node.type != "call":
                continue
            function = node.child_by_field_name("function")
            arguments = node.child_by_field_name("arguments")
            if not function or not arguments:
                continue
            fn_path = self._flatten_function_path(function)
            if not fn_path:
                continue
            line_range = (node.start_point[0] + 1, node.end_point[0] + 1)
            lower_fn = fn_path.lower()
            terminal = lower_fn.split(".")[-1]

            if lower_fn.endswith(".read_csv"):
                dataset, unresolved = self._resolve_first_argument(arguments, variables)
                out.append(
                    PythonDataFlowEvent(
                        source_file=rel,
                        line_range=line_range,
                        flow_type="CONSUMES",
                        dataset=dataset,
                        storage_type="file",
                        analysis_method="tree_sitter_python",
                        unresolved=unresolved,
                    )
                )
                continue

            if lower_fn.endswith(".read_sql"):
                sql_arg, unresolved = self._resolve_first_argument(arguments, variables)
                if unresolved:
                    out.append(
                        PythonDataFlowEvent(
                            source_file=rel,
                            line_range=line_range,
                            flow_type="CONSUMES",
                            dataset=self.DYNAMIC_REFERENCE,
                            storage_type="table",
                            analysis_method="tree_sitter_python",
                            unresolved=True,
                        )
                    )
                    continue
                tables = self._extract_sql_tables(sql_arg)
                if not tables:
                    tables = [sql_arg]
                for table in tables:
                    out.append(
                        PythonDataFlowEvent(
                            source_file=rel,
                            line_range=line_range,
                            flow_type="CONSUMES",
                            dataset=table,
                            storage_type="table",
                            analysis_method="tree_sitter_python+sqlglot",
                            unresolved=False,
                        )
                    )
                continue

            if lower_fn.endswith(".execute") or lower_fn == "execute":
                sql_arg, unresolved = self._resolve_first_argument(arguments, variables)
                if unresolved:
                    out.append(
                        PythonDataFlowEvent(
                            source_file=rel,
                            line_range=line_range,
                            flow_type="CONSUMES",
                            dataset=self.DYNAMIC_REFERENCE,
                            storage_type="table",
                            analysis_method="tree_sitter_python",
                            unresolved=True,
                        )
                    )
                    continue
                tables = self._extract_sql_tables(sql_arg)
                if not tables:
                    tables = [sql_arg]
                for table in tables:
                    out.append(
                        PythonDataFlowEvent(
                            source_file=rel,
                            line_range=line_range,
                            flow_type="CONSUMES",
                            dataset=table,
                            storage_type="table",
                            analysis_method="tree_sitter_python+sqlglot",
                            unresolved=False,
                        )
                    )
                continue

            # PySpark read chains (spark.read.csv/parquet/json/load/table/...)
            if ".read." in lower_fn or lower_fn.endswith(".read"):
                dataset, unresolved = self._resolve_first_argument(arguments, variables)
                out.append(
                    PythonDataFlowEvent(
                        source_file=rel,
                        line_range=line_range,
                        flow_type="CONSUMES",
                        dataset=dataset,
                        storage_type="file",
                        analysis_method="tree_sitter_python",
                        unresolved=unresolved,
                    )
                )
                continue

            # PySpark write chains (df.write.parquet/csv/json/save/...)
            write_methods = {
                "csv",
                "parquet",
                "json",
                "text",
                "orc",
                "avro",
                "save",
                "saveastextfile",
                "saveastable",
                "insertinto",
            }
            if ".write." in lower_fn and terminal in write_methods:
                dataset, unresolved = self._resolve_first_argument(arguments, variables)
                out.append(
                    PythonDataFlowEvent(
                        source_file=rel,
                        line_range=line_range,
                        flow_type="PRODUCES",
                        dataset=dataset,
                        storage_type="file",
                        analysis_method="tree_sitter_python",
                        unresolved=unresolved,
                    )
                )

        return out

    def _extract_string_variables(self, root: object) -> dict[str, tuple[str, bool]]:
        mapping: dict[str, tuple[str, bool]] = {}
        for node in self._walk(root):
            if node.type != "assignment":
                continue
            left = node.child_by_field_name("left")
            right = node.child_by_field_name("right")
            if not left or not right or left.type != "identifier":
                continue
            name = self._decode(left)
            value, unresolved = self._resolve_expr_to_string(right, mapping)
            if value is not None:
                mapping[name] = (value, unresolved)
        return mapping

    def _resolve_first_argument(
        self, argument_list: object, variables: dict[str, tuple[str, bool]]
    ) -> tuple[str, bool]:
        arg_nodes = [c for c in argument_list.children if c.type not in {"(", ")", ","}]
        if not arg_nodes:
            return self.DYNAMIC_REFERENCE, True
        value, unresolved = self._resolve_expr_to_string(arg_nodes[0], variables)
        if value is None or unresolved:
            return self.DYNAMIC_REFERENCE, True
        return value, False

    def _resolve_expr_to_string(
        self, node: object, variables: dict[str, tuple[str, bool]]
    ) -> tuple[str | None, bool]:
        if node.type == "string":
            return self._string_literal_value(node)
        if node.type == "identifier":
            ref = variables.get(self._decode(node))
            if not ref:
                return None, True
            return ref
        if node.type == "binary_operator":
            children = [c for c in node.children if c.type.strip()]
            if len(children) >= 3 and self._decode(children[1]).strip() == "+":
                left, ul = self._resolve_expr_to_string(children[0], variables)
                right, ur = self._resolve_expr_to_string(children[2], variables)
                if left is None or right is None:
                    return None, True
                return left + right, (ul or ur)
        return None, True

    def _string_literal_value(self, node: object) -> tuple[str | None, bool]:
        # f-strings/interpolations cannot be safely resolved statically.
        if any(c.type == "interpolation" for c in node.children):
            return None, True
        raw = self._decode(node)
        try:
            value = ast.literal_eval(raw)
        except Exception:
            return None, True
        if not isinstance(value, str):
            return None, True
        return value, False

    def _extract_sql_tables(self, sql_text: str) -> list[str]:
        for dialect in ["postgres", "bigquery", "snowflake", "duckdb"]:
            try:
                statements = sqlglot.parse(sql_text, read=dialect)
            except Exception:
                continue
            tables: set[str] = set()
            for stmt in statements:
                for table in stmt.find_all(exp.Table):
                    if table.name:
                        tables.add(table.name)
            if tables:
                return sorted(tables)
        return []

    def _flatten_function_path(self, node: object) -> str:
        if node.type == "identifier":
            return self._decode(node)
        if node.type == "attribute":
            left = node.child_by_field_name("object")
            right = node.child_by_field_name("attribute")
            if left and right:
                left_path = self._flatten_function_path(left)
                right_name = self._decode(right)
                if left_path:
                    return f"{left_path}.{right_name}"
                return right_name
            # fallback on textual representation
            return self._decode(node)
        if node.type == "call":
            fn = node.child_by_field_name("function")
            return self._flatten_function_path(fn) if fn else self._decode(node)
        return self._decode(node)

    def _decode(self, node: object) -> str:
        return node.text.decode("utf-8", errors="ignore")

    def _walk(self, root: object) -> Iterable[object]:
        stack = [root]
        while stack:
            node = stack.pop()
            yield node
            children = list(getattr(node, "children", []))
            stack.extend(reversed(children))
