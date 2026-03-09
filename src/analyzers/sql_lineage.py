from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import sqlglot
from sqlglot import expressions as exp


@dataclass
class SQLDependency:
    source_tables: list[str]
    target_tables: list[str]
    source_file: str
    statement: str
    line_range: tuple[int, int]


class SQLLineageAnalyzer:
    DIALECTS = ["postgres", "bigquery", "snowflake", "duckdb"]

    def extract_from_file(self, path: Path, repo_root: Path) -> list[SQLDependency]:
        sql_text = path.read_text(encoding="utf-8", errors="ignore")
        dependencies: list[SQLDependency] = []
        for dialect in self.DIALECTS:
            try:
                parsed = sqlglot.parse(sql_text, read=dialect)
                dependencies = self._extract_dependencies(parsed, path, repo_root)
                if dependencies:
                    return dependencies
            except Exception:
                continue
        return dependencies

    def _extract_dependencies(
        self, statements: list[exp.Expression], path: Path, repo_root: Path
    ) -> list[SQLDependency]:
        out: list[SQLDependency] = []
        rel = str(path.relative_to(repo_root))
        file_text = path.read_text(encoding="utf-8", errors="ignore")
        for stmt in statements:
            source_tables = self._extract_source_tables(stmt)
            source_tables.extend(self._extract_dbt_refs(stmt.sql()))
            target_tables: list[str] = []

            inserts = list(stmt.find_all(exp.Insert))
            creates = list(stmt.find_all(exp.Create))
            merges = list(stmt.find_all(exp.Merge))
            for ins in inserts:
                target = ins.this
                if isinstance(target, exp.Table) and target.name:
                    target_tables.append(target.name)
            for crt in creates:
                target = crt.this
                if isinstance(target, exp.Table) and target.name:
                    target_tables.append(target.name)
            for mrg in merges:
                target = mrg.this
                if isinstance(target, exp.Table) and target.name:
                    target_tables.append(target.name)

            # dbt models are often SELECT-only files; use filename stem as target model.
            if not target_tables and source_tables:
                target_tables = [path.stem]

            if source_tables or target_tables:
                out.append(
                    SQLDependency(
                        source_tables=sorted(set(source_tables)),
                        target_tables=sorted(set(target_tables)),
                        source_file=rel,
                        statement=stmt.sql()[:2000],
                        line_range=self._find_line_range(file_text, stmt.sql()),
                    )
                )
        return out

    def _extract_source_tables(self, stmt: exp.Expression) -> list[str]:
        # Gather all CTE aliases so they don't become false source tables.
        cte_aliases = {cte.alias_or_name for cte in stmt.find_all(exp.CTE) if cte.alias_or_name}
        target_names = {
            table.name
            for table in [self._target_table_from_statement(stmt)]
            if isinstance(table, exp.Table) and table.name
        }
        sources: set[str] = set()
        for table in stmt.find_all(exp.Table):
            if not table.name:
                continue
            name = table.name
            if name in cte_aliases:
                continue
            if name in target_names:
                continue
            if name.lower() in {"ref", "source"}:
                continue
            sources.add(name)
        return sorted(sources)

    def _target_table_from_statement(self, stmt: exp.Expression) -> exp.Table | None:
        for node in stmt.find_all(exp.Insert):
            if isinstance(node.this, exp.Table):
                return node.this
        for node in stmt.find_all(exp.Create):
            if isinstance(node.this, exp.Table):
                return node.this
        for node in stmt.find_all(exp.Merge):
            if isinstance(node.this, exp.Table):
                return node.this
        return None

    def _extract_dbt_refs(self, sql_text: str) -> list[str]:
        refs = re.findall(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", sql_text)
        sources = re.findall(
            r"source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", sql_text
        )
        source_tables = [f"{src}.{tbl}" for src, tbl in sources]
        return refs + source_tables

    def _find_line_range(self, file_text: str, stmt_sql: str) -> tuple[int, int]:
        stmt_sql = stmt_sql.strip()
        if not stmt_sql:
            return (0, 0)
        idx = file_text.find(stmt_sql)
        if idx < 0:
            return (0, 0)
        start_line = file_text[:idx].count("\n") + 1
        end_line = start_line + stmt_sql.count("\n")
        return (start_line, end_line)
