from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import re

import sqlglot
from sqlglot import expressions as exp

logger = logging.getLogger(__name__)


@dataclass
class SQLDependency:
    source_tables: list[str]
    target_tables: list[str]
    source_file: str
    statement: str
    line_range: tuple[int, int]
    statement_operation: str = "read"
    dialect: str = ""


class SQLLineageAnalyzer:
    DIALECTS = ["postgres", "bigquery", "snowflake", "duckdb"]

    def __init__(self, dialects: list[str] | None = None) -> None:
        self.dialects = dialects or list(self.DIALECTS)

    def extract_from_file(self, path: Path, repo_root: Path) -> list[SQLDependency]:
        sql_text = path.read_text(encoding="utf-8", errors="ignore")
        parse_errors: list[tuple[str, str]] = []
        parse_succeeded = False
        for dialect in self.dialects:
            try:
                parsed = sqlglot.parse(sql_text, read=dialect)
                parse_succeeded = True
                dependencies = self._extract_dependencies(
                    parsed,
                    path,
                    repo_root,
                    file_text=sql_text,
                    dialect=dialect,
                )
                if dependencies:
                    return dependencies
            except Exception as exc:
                parse_errors.append((dialect, str(exc)))
                continue
        dbt_dependencies = self._extract_dbt_only_dependencies(sql_text, path, repo_root)
        if dbt_dependencies:
            return dbt_dependencies

        if parse_errors and not parse_succeeded:
            rel = str(path.relative_to(repo_root))
            sample = ", ".join(
                f"{dialect}: {message.splitlines()[0][:120]}" for dialect, message in parse_errors[:2]
            )
            logger.warning("Skipping unparseable SQL file %s after dialect attempts (%s).", rel, sample)
        return []

    def _extract_dependencies(
        self,
        statements: list[exp.Expression],
        path: Path,
        repo_root: Path,
        file_text: str,
        dialect: str,
    ) -> list[SQLDependency]:
        out: list[SQLDependency] = []
        rel = str(path.relative_to(repo_root))
        search_start = 0
        for stmt in statements:
            source_tables = self._extract_source_tables(stmt)
            source_tables.extend(self._extract_dbt_refs(stmt.sql()))
            target_tables = self._extract_target_tables(stmt)
            statement_operation = self._statement_operation(stmt)

            # dbt models are often SELECT-only files; use filename stem as target model.
            if statement_operation == "read" and not target_tables and source_tables:
                target_tables = [path.stem]

            if source_tables or target_tables:
                line_range, search_start = self._find_line_range(file_text, stmt.sql(), start_at=search_start)
                out.append(
                    SQLDependency(
                        source_tables=sorted(set(source_tables)),
                        target_tables=sorted(set(target_tables)),
                        source_file=rel,
                        statement=stmt.sql()[:2000],
                        line_range=line_range,
                        statement_operation=statement_operation,
                        dialect=dialect,
                    )
                )
        return out

    def _statement_operation(self, stmt: exp.Expression) -> str:
        if isinstance(stmt, (exp.Insert, exp.Create, exp.Merge, exp.Update, exp.Delete)):
            return "write"
        return "read"

    def _extract_source_tables(self, stmt: exp.Expression) -> list[str]:
        root_refs: set[str] = set()
        cte_graph: dict[str, set[str]] = {}

        source_expressions = self._source_expressions(stmt)
        if not source_expressions:
            source_expressions = [stmt]

        for expr in source_expressions:
            root_refs.update(self._direct_table_refs(expr))
            cte_graph.update(self._cte_dependency_map(expr))

        expanded_sources = self._expand_cte_references(root_refs, cte_graph)
        target_tables = set(self._extract_target_tables(stmt))
        out: set[str] = set()
        for source in expanded_sources:
            if source in target_tables:
                continue
            if source.lower() in {"ref", "source"}:
                continue
            out.add(source)
        return sorted(out)

    def _source_expressions(self, stmt: exp.Expression) -> list[exp.Expression]:
        expressions: list[exp.Expression] = []
        if isinstance(stmt, (exp.Insert, exp.Create)):
            query = stmt.args.get("expression")
            if isinstance(query, exp.Expression):
                expressions.append(query)
        elif isinstance(stmt, exp.Merge):
            using_expr = stmt.args.get("using")
            if isinstance(using_expr, exp.Expression):
                expressions.append(using_expr)
        else:
            expressions.append(stmt)
        return expressions

    def _extract_target_tables(self, stmt: exp.Expression) -> list[str]:
        targets: list[str] = []
        for node in stmt.find_all(exp.Insert):
            name = self._table_name(node.this)
            if name:
                targets.append(name)
        for node in stmt.find_all(exp.Create):
            name = self._table_name(node.this)
            if name:
                targets.append(name)
        for node in stmt.find_all(exp.Merge):
            name = self._table_name(node.this)
            if name:
                targets.append(name)
        for node in stmt.find_all(exp.Update):
            name = self._table_name(node.this)
            if name:
                targets.append(name)
        for node in stmt.find_all(exp.Delete):
            name = self._table_name(node.this)
            if name:
                targets.append(name)
        return targets

    def _cte_dependency_map(self, expr: exp.Expression) -> dict[str, set[str]]:
        mapping: dict[str, set[str]] = {}
        if not isinstance(expr, exp.Expression):
            return mapping
        with_expr = expr.args.get("with_")
        if not isinstance(with_expr, exp.With):
            with_expr = None
        if isinstance(with_expr, exp.With):
            for cte in with_expr.expressions:
                alias = (cte.alias_or_name or "").strip()
                if not alias:
                    continue
                cte_query = cte.this if isinstance(cte.this, exp.Expression) else None
                if cte_query is not None:
                    mapping.update(self._cte_dependency_map(cte_query))
                mapping[alias.lower()] = self._direct_table_refs(cte_query)

        # Handle nested WITH blocks inside subqueries.
        for subquery in expr.find_all(exp.Subquery):
            nested = subquery.this
            if isinstance(nested, exp.Expression):
                mapping.update(self._cte_dependency_map(nested))
        return mapping

    def _expand_cte_references(self, refs: set[str], cte_graph: dict[str, set[str]]) -> set[str]:
        expanded: set[str] = set()

        def walk(name: str, stack: set[str]) -> set[str]:
            lowered = name.lower()
            if lowered not in cte_graph:
                return {name}
            if lowered in stack:
                return set()
            leaves: set[str] = set()
            for dep in cte_graph[lowered]:
                leaves.update(walk(dep, stack | {lowered}))
            return leaves

        for ref in refs:
            expanded.update(walk(ref, set()))
        return expanded

    def _direct_table_refs(self, expr: exp.Expression | None) -> set[str]:
        if expr is None:
            return set()
        query = expr.copy()
        # `WITH` CTE blocks are handled separately so we can preserve chain semantics.
        if "with_" in query.args:
            query.set("with_", None)
        refs: set[str] = set()
        for table in query.find_all(exp.Table):
            name = self._table_name(table)
            if name:
                refs.add(name)
        return refs

    def _table_name(self, table: exp.Expression | None) -> str:
        if not isinstance(table, exp.Table):
            return ""
        parts = [table.catalog, table.db, table.name]
        cleaned = [str(p) for p in parts if p]
        return ".".join(cleaned)

    def _extract_dbt_refs(self, sql_text: str) -> list[str]:
        refs = re.findall(
            r"ref\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]+)['\"])?\s*\)",
            sql_text,
        )
        parsed_refs = [f"{package}.{model}" if model else package for package, model in refs]
        sources = re.findall(
            r"source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", sql_text
        )
        source_tables = [f"{src}.{tbl}" for src, tbl in sources]
        return parsed_refs + source_tables

    def _extract_dbt_only_dependencies(self, sql_text: str, path: Path, repo_root: Path) -> list[SQLDependency]:
        refs = self._extract_dbt_refs(sql_text)
        if not refs:
            return []
        rel = str(path.relative_to(repo_root))
        line_count = max(1, sql_text.count("\n") + 1)
        return [
            SQLDependency(
                source_tables=sorted(set(refs)),
                target_tables=[path.stem],
                source_file=rel,
                statement=sql_text[:2000],
                line_range=(1, line_count),
                statement_operation="read",
                dialect="dbt_template",
            )
        ]

    def _find_line_range(self, file_text: str, stmt_sql: str, start_at: int = 0) -> tuple[tuple[int, int], int]:
        stmt_sql = stmt_sql.strip()
        if not stmt_sql:
            return (0, 0), start_at

        idx = file_text.find(stmt_sql, start_at)
        if idx >= 0:
            end_idx = idx + len(stmt_sql)
            return self._line_range_from_span(file_text, idx, end_idx), end_idx

        collapsed_stmt = self._collapse_for_match(stmt_sql)
        collapsed_file, index_map = self._collapse_for_match_with_index(file_text, start_at=start_at)
        collapsed_idx = collapsed_file.find(collapsed_stmt) if collapsed_stmt else -1
        if collapsed_idx >= 0:
            match_start = index_map[collapsed_idx]
            match_end = index_map[collapsed_idx + len(collapsed_stmt) - 1] + 1
            return self._line_range_from_span(file_text, match_start, match_end), match_end

        # Last-resort approximation keeps line evidence usable for traceability.
        start_line = file_text[:start_at].count("\n") + 1
        end_line = start_line + stmt_sql.count("\n")
        return (start_line, end_line), start_at

    def _line_range_from_span(self, file_text: str, start_idx: int, end_idx: int) -> tuple[int, int]:
        start_line = file_text[:start_idx].count("\n") + 1
        end_line = file_text[:end_idx].count("\n") + 1
        return (start_line, max(start_line, end_line))

    def _collapse_for_match(self, text: str) -> str:
        return "".join(ch.lower() for ch in text if not ch.isspace()).rstrip(";")

    def _collapse_for_match_with_index(self, text: str, start_at: int = 0) -> tuple[str, list[int]]:
        collapsed_chars: list[str] = []
        index_map: list[int] = []
        for idx, ch in enumerate(text):
            if idx < start_at:
                continue
            if ch.isspace():
                continue
            collapsed_chars.append(ch.lower())
            index_map.append(idx)
        raw_collapsed = "".join(collapsed_chars)
        collapsed = raw_collapsed.rstrip(";")
        if collapsed != raw_collapsed:
            trim = len(raw_collapsed) - len(collapsed)
            if trim > 0:
                index_map = index_map[:-trim]
        return collapsed, index_map
