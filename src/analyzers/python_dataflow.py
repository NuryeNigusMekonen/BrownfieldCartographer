from __future__ import annotations

import ast
from dataclasses import dataclass
import logging
from pathlib import Path
import re
from typing import Iterable

import sqlglot
from sqlglot import expressions as exp
from tree_sitter import Language, Parser
import tree_sitter_python

logger = logging.getLogger(__name__)


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
                dataset, unresolved = self._resolve_argument(
                    arguments,
                    variables,
                    keyword_candidates=("filepath_or_buffer", "path_or_buf", "path"),
                )
                if unresolved:
                    self._log_dynamic_reference(rel, line_range, fn_path)
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
                sql_arg, unresolved = self._resolve_argument(
                    arguments,
                    variables,
                    keyword_candidates=("sql", "sql_query", "query"),
                )
                if unresolved:
                    self._log_dynamic_reference(rel, line_range, fn_path)
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
                sql_arg, unresolved = self._resolve_argument(
                    arguments,
                    variables,
                    keyword_candidates=("statement", "sql", "query", "clause"),
                )
                if unresolved:
                    self._log_dynamic_reference(rel, line_range, fn_path)
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
                    # Ignore operational SQL commands that are not data lineage edges.
                    if self._is_operational_sql(sql_arg):
                        continue
                    # Lightweight fallback when sqlglot cannot parse a valid statement.
                    tables = self._extract_table_hints(sql_arg)
                if not tables:
                    continue
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

            # pandas DataFrame writes (df.to_csv/parquet/json/sql/excel/...)
            if lower_fn.endswith(
                (
                    ".to_csv",
                    ".to_parquet",
                    ".to_json",
                    ".to_excel",
                    ".to_feather",
                    ".to_orc",
                    ".to_hdf",
                    ".to_sql",
                    ".to_gbq",
                )
            ):
                if terminal in {"to_sql", "to_gbq"}:
                    dataset, unresolved = self._resolve_argument(
                        arguments,
                        variables,
                        keyword_candidates=("name", "table_name", "destination_table"),
                    )
                    storage_type = "table"
                else:
                    dataset, unresolved = self._resolve_argument(
                        arguments,
                        variables,
                        keyword_candidates=("path_or_buf", "path", "excel_writer"),
                    )
                    storage_type = "file"
                if unresolved:
                    self._log_dynamic_reference(rel, line_range, fn_path)
                out.append(
                    PythonDataFlowEvent(
                        source_file=rel,
                        line_range=line_range,
                        flow_type="PRODUCES",
                        dataset=dataset,
                        storage_type=storage_type,
                        analysis_method="tree_sitter_python",
                        unresolved=unresolved,
                    )
                )
                continue

            # PySpark read chains (spark.read.csv/parquet/json/load/table/...)
            if ".read." in lower_fn or lower_fn.endswith(".read") or lower_fn.endswith(".table"):
                dataset, unresolved = self._resolve_argument(
                    arguments,
                    variables,
                    keyword_candidates=("path", "table", "name"),
                )
                storage_type = "table" if terminal == "table" else "file"
                if unresolved:
                    self._log_dynamic_reference(rel, line_range, fn_path)
                out.append(
                    PythonDataFlowEvent(
                        source_file=rel,
                        line_range=line_range,
                        flow_type="CONSUMES",
                        dataset=dataset,
                        storage_type=storage_type,
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
                dataset, unresolved = self._resolve_argument(
                    arguments,
                    variables,
                    keyword_candidates=("path", "table", "name"),
                )
                storage_type = "table" if terminal in {"saveastable", "insertinto"} else "file"
                if unresolved:
                    self._log_dynamic_reference(rel, line_range, fn_path)
                out.append(
                    PythonDataFlowEvent(
                        source_file=rel,
                        line_range=line_range,
                        flow_type="PRODUCES",
                        dataset=dataset,
                        storage_type=storage_type,
                        analysis_method="tree_sitter_python",
                        unresolved=unresolved,
                    )
                )

        return out

    def _log_dynamic_reference(self, source_file: str, line_range: tuple[int, int], call_path: str) -> None:
        logger.info(
            "Dynamic reference, cannot resolve in %s:%s-%s for call %s",
            source_file,
            line_range[0],
            line_range[1],
            call_path,
        )

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

    def _resolve_argument(
        self,
        argument_list: object,
        variables: dict[str, tuple[str, bool]],
        keyword_candidates: tuple[str, ...] = (),
    ) -> tuple[str, bool]:
        positional_args, keyword_args = self._collect_call_arguments(argument_list)
        chosen_node = None
        for key in keyword_candidates:
            if key in keyword_args:
                chosen_node = keyword_args[key]
                break
        if chosen_node is None and positional_args:
            chosen_node = positional_args[0]
        if chosen_node is None:
            return self.DYNAMIC_REFERENCE, True
        value, unresolved = self._resolve_expr_to_string(chosen_node, variables)
        if value is None or unresolved:
            return self.DYNAMIC_REFERENCE, True
        return value, False

    def _collect_call_arguments(self, argument_list: object) -> tuple[list[object], dict[str, object]]:
        positional: list[object] = []
        keyword: dict[str, object] = {}
        for child in getattr(argument_list, "children", []):
            if child.type in {"(", ")", ","}:
                continue
            if child.type == "keyword_argument":
                key_node = child.child_by_field_name("name")
                value_node = child.child_by_field_name("value")
                key = self._decode(key_node).strip() if key_node else ""
                if key and value_node:
                    keyword[key] = value_node
                continue
            positional.append(child)
        return positional, keyword

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
        if node.type == "call":
            function = node.child_by_field_name("function")
            arguments = node.child_by_field_name("arguments")
            if not function or not arguments:
                return None, True
            fn_path = self._flatten_function_path(function).lower()
            if fn_path.endswith(".text") or fn_path == "text":
                positional, keyword = self._collect_call_arguments(arguments)
                target = positional[0] if positional else keyword.get("text")
                if target is None:
                    return None, True
                return self._resolve_expr_to_string(target, variables)
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

    def _is_operational_sql(self, sql_text: str) -> bool:
        normalized = " ".join(sql_text.strip().lower().split())
        if not normalized:
            return True
        operational_prefixes = (
            "install ",
            "load ",
            "checkpoint",
            "vacuum",
            "analyze",
            "pragma ",
            "set ",
            "use ",
            "show ",
            "describe ",
            "explain ",
            "call ",
            "begin",
            "commit",
            "rollback",
        )
        return normalized.startswith(operational_prefixes)

    def _extract_table_hints(self, sql_text: str) -> list[str]:
        pattern = re.compile(r"\b(?:from|join|into|update|table)\s+([a-zA-Z_][\w\.\$]*)", flags=re.IGNORECASE)
        tables = {match.group(1).strip() for match in pattern.finditer(sql_text)}
        return sorted(table for table in tables if table)

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
