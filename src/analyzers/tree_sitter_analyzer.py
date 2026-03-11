from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from tree_sitter import Language, Parser
import tree_sitter_javascript
import tree_sitter_python
import tree_sitter_sql
import tree_sitter_typescript
import tree_sitter_yaml


logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".py", ".sql", ".yaml", ".yml", ".js", ".ts", ".ipynb"}
IGNORED_DIRS = {".git", "__pycache__", ".venv", "venv", "node_modules", ".cartography", "dist", "build"}


@dataclass
class ModuleAnalysis:
    path: str
    language: str
    imports: list[str]
    public_functions: list[str]
    classes: list[str]
    class_inheritance: dict[str, list[str]]
    function_signatures: dict[str, str]
    function_calls: list[tuple[str, str]]
    complexity_score: float
    loc: int
    comment_ratio: float
    resolved_imports: list[str] = field(default_factory=list)
    function_decorators: dict[str, list[str]] = field(default_factory=dict)
    sql_table_references: list[str] = field(default_factory=list)
    sql_query_structure: list[str] = field(default_factory=list)
    yaml_key_hierarchy: list[str] = field(default_factory=list)
    parse_issues: list[str] = field(default_factory=list)
    skipped: bool = False


@dataclass
class StructuredExtraction:
    imports: list[str] = field(default_factory=list)
    resolved_imports: list[str] = field(default_factory=list)
    public_functions: list[str] = field(default_factory=list)
    classes: list[str] = field(default_factory=list)
    class_inheritance: dict[str, list[str]] = field(default_factory=dict)
    function_signatures: dict[str, str] = field(default_factory=dict)
    function_calls: list[tuple[str, str]] = field(default_factory=list)
    function_decorators: dict[str, list[str]] = field(default_factory=dict)
    sql_table_references: list[str] = field(default_factory=list)
    sql_query_structure: list[str] = field(default_factory=list)
    yaml_key_hierarchy: list[str] = field(default_factory=list)
    complexity_score: float = 0.0
    parse_issues: list[str] = field(default_factory=list)
    skipped: bool = False


class LanguageRouter:
    EXT_TO_LANG = {
        ".py": "python",
        ".sql": "sql",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".js": "javascript",
        ".ts": "typescript",
        ".ipynb": "notebook",
    }

    def route(self, path: Path) -> str:
        return self.EXT_TO_LANG.get(path.suffix.lower(), "unknown")


class TreeSitterAnalyzer:
    """
    Reusable multi-language AST analyzer service.
    It loads tree-sitter grammars, routes files by extension, and extracts
    language-specific structure for Python/SQL/YAML (with JS/TS support).
    """

    SQL_KEYWORDS = {
        "as",
        "by",
        "delete",
        "from",
        "group",
        "having",
        "insert",
        "into",
        "join",
        "left",
        "limit",
        "merge",
        "on",
        "order",
        "outer",
        "right",
        "select",
        "set",
        "table",
        "union",
        "update",
        "where",
        "with",
    }

    def __init__(self) -> None:
        self.router = LanguageRouter()
        self.parsers: dict[str, Parser] = {}
        self._init_parsers()

    def _init_parsers(self) -> None:
        language_factories = {
            "python": tree_sitter_python.language,
            "sql": tree_sitter_sql.language,
            "yaml": tree_sitter_yaml.language,
            "javascript": tree_sitter_javascript.language,
            "typescript": tree_sitter_typescript.language_typescript,
        }
        for language_name, language_factory in language_factories.items():
            try:
                language = Language(language_factory())
                self.parsers[language_name] = Parser(language)
            except Exception as exc:
                logger.warning("Tree-sitter grammar unavailable for %s: %s", language_name, exc)

    def iter_supported_files(self, root: Path) -> Iterable[Path]:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS:
                if any(part in IGNORED_DIRS for part in path.parts):
                    continue
                yield path

    def analyze_module(self, path: Path, root: Path) -> ModuleAnalysis:
        language = self.router.route(path)
        rel_path = str(path.relative_to(root))
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:
            issue = f"failed to read file: {exc}"
            logger.warning("Skipping unreadable file %s: %s", rel_path, exc)
            return ModuleAnalysis(
                path=rel_path,
                language=language,
                imports=[],
                public_functions=[],
                classes=[],
                class_inheritance={},
                function_signatures={},
                function_calls=[],
                complexity_score=0.0,
                loc=0,
                comment_ratio=0.0,
                parse_issues=[issue],
                skipped=True,
            )

        lines = text.splitlines()
        loc = len(lines)
        comments = [ln for ln in lines if ln.strip().startswith(("#", "//", "--"))]
        comment_ratio = (len(comments) / loc) if loc else 0.0

        extracted = self.analyze_source(language=language, source=text, importer_path=rel_path)
        for issue in extracted.parse_issues:
            logger.warning("Parser issue in %s (%s): %s", rel_path, language, issue)

        return ModuleAnalysis(
            path=rel_path,
            language=language,
            imports=extracted.imports,
            public_functions=extracted.public_functions,
            classes=extracted.classes,
            class_inheritance=extracted.class_inheritance,
            function_signatures=extracted.function_signatures,
            function_calls=extracted.function_calls,
            complexity_score=extracted.complexity_score,
            loc=loc,
            comment_ratio=comment_ratio,
            resolved_imports=extracted.resolved_imports,
            function_decorators=extracted.function_decorators,
            sql_table_references=extracted.sql_table_references,
            sql_query_structure=extracted.sql_query_structure,
            yaml_key_hierarchy=extracted.yaml_key_hierarchy,
            parse_issues=extracted.parse_issues,
            skipped=extracted.skipped,
        )

    def analyze_source(self, language: str, source: str, importer_path: str = "") -> StructuredExtraction:
        if language == "python":
            return self._analyze_python_ts(source, importer_path)
        if language in {"javascript", "typescript"}:
            return self._analyze_js_ts(source, language)
        if language == "yaml":
            return self._analyze_yaml_ts(source)
        if language == "sql":
            return self._analyze_sql_ts(source)
        return StructuredExtraction()

    def _parse_tree(self, language: str, source: str) -> tuple[object | None, list[str]]:
        parser = self.parsers.get(language)
        if parser is None:
            return None, [f"{language} parser unavailable; skipping AST extraction"]
        try:
            tree = parser.parse(source.encode("utf-8", errors="ignore"))
        except Exception as exc:
            return None, [f"{language} parser failed: {exc}"]
        root = tree.root_node
        issues: list[str] = []
        if getattr(root, "has_error", False):
            issues.append("syntax errors detected; returning partial extraction")
        return root, issues

    def _analyze_python_ts(self, source: str, importer_path: str) -> StructuredExtraction:
        root, parse_issues = self._parse_tree("python", source)
        result = StructuredExtraction(parse_issues=parse_issues)
        if root is None:
            result.skipped = True
            return result

        complexity = 1
        complexity_nodes = {"if_statement", "for_statement", "while_statement", "try_statement", "with_statement"}

        def visit(
            node: object,
            current_class: str | None = None,
            pending_decorators: list[str] | None = None,
        ) -> None:
            nonlocal complexity
            node_type = getattr(node, "type", "")

            if node_type in complexity_nodes:
                complexity += 1

            if node_type == "decorated_definition":
                decorators = self._extract_python_decorators(node)
                decorated = self._extract_decorated_target(node)
                if decorated is not None:
                    visit(decorated, current_class=current_class, pending_decorators=decorators)
                return

            if node_type == "import_statement":
                result.imports.extend(self._extract_python_import_statement(node))
                return

            if node_type == "import_from_statement":
                result.imports.extend(self._extract_python_from_import(node))
                return

            if node_type == "class_definition":
                class_name = self._decode_text(node.child_by_field_name("name"))
                if class_name:
                    bases = self._extract_python_bases(node)
                    result.class_inheritance[class_name] = bases
                    result.classes.append(self._format_class_signature(class_name, bases))
                for child in getattr(node, "children", []):
                    visit(child, current_class=class_name or current_class)
                return

            if node_type in {"function_definition", "async_function_definition"}:
                name = self._decode_text(node.child_by_field_name("name"))
                if name:
                    normalized = name.lstrip("_")
                    if normalized:
                        result.public_functions.append(normalized)
                    qualified_name = f"{current_class}.{name}" if current_class else name
                    signature = self._build_python_signature(node, qualified_name)
                    result.function_signatures[qualified_name] = signature
                    decorators = pending_decorators or []
                    if decorators:
                        result.function_decorators[qualified_name] = decorators
                    result.function_calls.extend(self._extract_python_call_edges(node, qualified_name))
                for child in getattr(node, "children", []):
                    visit(child, current_class=current_class)
                return

            for child in getattr(node, "children", []):
                visit(child, current_class=current_class)

        visit(root)

        raw_imports = sorted(set(i for i in result.imports if i))
        result.imports = raw_imports
        result.resolved_imports = sorted(
            set(self._resolve_python_relative_import(module_import, importer_path) for module_import in raw_imports if module_import)
        )
        result.public_functions = sorted(set(result.public_functions))
        result.classes = sorted(set(result.classes))
        result.function_calls = sorted(set(result.function_calls))
        result.function_decorators = {key: sorted(set(values)) for key, values in sorted(result.function_decorators.items())}
        result.function_signatures = dict(sorted(result.function_signatures.items()))
        result.complexity_score = float(complexity)
        if not result.function_signatures and any("syntax errors" in issue for issue in result.parse_issues):
            result.skipped = True
        return result

    def _extract_python_call_edges(self, fn_node: object, caller: str) -> list[tuple[str, str]]:
        calls: list[tuple[str, str]] = []
        body_node = fn_node.child_by_field_name("body") or fn_node
        for node in self._walk(body_node):
            if node.type != "call":
                continue
            function_node = node.child_by_field_name("function")
            if function_node is None:
                continue
            callee = self._flatten_python_expression(function_node)
            if not callee:
                continue
            calls.append((caller, callee.rsplit(".", 1)[-1]))
        return calls

    def _build_python_signature(self, fn_node: object, qualified_name: str) -> str:
        params_node = fn_node.child_by_field_name("parameters")
        params = self._decode_text(params_node).strip() if params_node else "()"
        return f"{qualified_name}{params}"

    def _extract_python_decorators(self, decorated_definition: object) -> list[str]:
        decorators: list[str] = []
        for child in getattr(decorated_definition, "children", []):
            if child.type != "decorator":
                continue
            text = self._decode_text(child).strip()
            if text.startswith("@"):
                text = text[1:]
            text = text.split("(", 1)[0].strip()
            if text:
                decorators.append(text)
        return decorators

    def _extract_decorated_target(self, decorated_definition: object) -> object | None:
        for child in getattr(decorated_definition, "children", []):
            if child.type in {"function_definition", "async_function_definition", "class_definition"}:
                return child
        return None

    def _extract_python_import_statement(self, node: object) -> list[str]:
        out: list[str] = []
        for child in getattr(node, "children", []):
            if child.type == "dotted_name":
                out.append(self._decode_text(child))
            elif child.type == "aliased_import":
                out.append(self._extract_aliased_symbol(child))
        return out

    def _extract_python_from_import(self, node: object) -> list[str]:
        out: list[str] = []
        module_node = node.child_by_field_name("module_name")
        module_text = self._decode_text(module_node).strip() if module_node else ""
        imported_names: list[str] = []

        for child in getattr(node, "children", []):
            if module_node is not None and child.id == module_node.id:
                continue
            if child.type == "dotted_name":
                imported_names.append(self._decode_text(child).strip())
            elif child.type == "aliased_import":
                imported_names.append(self._extract_aliased_symbol(child))
            elif child.type == "wildcard_import":
                imported_names.append("*")

        if not imported_names:
            if module_text:
                out.append(module_text)
            return out

        for symbol in imported_names:
            if module_text:
                out.append(module_text if symbol == "*" else f"{module_text}.{symbol}")
            else:
                out.append(symbol)
        return out

    def _extract_python_bases(self, class_node: object) -> list[str]:
        bases_node = class_node.child_by_field_name("superclasses")
        if not bases_node:
            return []
        text = self._decode_text(bases_node).strip()
        if text.startswith("(") and text.endswith(")"):
            text = text[1:-1]
        parts = [p.strip() for p in text.split(",") if p.strip()]
        return parts

    def _resolve_python_relative_import(self, module_import: str, importer_path: str) -> str:
        stripped = module_import.strip()
        if not stripped:
            return ""
        if not stripped.startswith("."):
            return stripped

        leading = len(stripped) - len(stripped.lstrip("."))
        remainder = stripped[leading:]
        importer_parts = Path(importer_path).with_suffix("").parts[:-1]
        up_levels = max(0, leading - 1)
        if up_levels > len(importer_parts):
            return remainder
        base_parts = list(importer_parts[: len(importer_parts) - up_levels])
        if remainder:
            base_parts.extend(part for part in remainder.split(".") if part)
        return ".".join(base_parts)

    def _flatten_python_expression(self, node: object | None) -> str:
        if node is None:
            return ""
        if node.type in {"identifier", "dotted_name"}:
            return self._decode_text(node)
        if node.type == "attribute":
            left = node.child_by_field_name("object")
            right = node.child_by_field_name("attribute")
            left_path = self._flatten_python_expression(left)
            right_name = self._decode_text(right).strip()
            if left_path and right_name:
                return f"{left_path}.{right_name}"
            return right_name or left_path
        if node.type == "call":
            inner = node.child_by_field_name("function")
            return self._flatten_python_expression(inner)
        return self._decode_text(node).strip()

    def _analyze_js_ts(self, source: str, language: str) -> StructuredExtraction:
        root, parse_issues = self._parse_tree(language, source)
        result = StructuredExtraction(parse_issues=parse_issues)
        if root is None:
            # Keep legacy regex fallback for JS/TS only.
            result.imports = self._analyze_js_imports(source)
            result.public_functions = self._analyze_js_functions(source)
            result.complexity_score = float(source.count("if (") + source.count("for (") + source.count("while (") + 1.0)
            result.skipped = not (result.imports or result.public_functions)
            return result

        complexity = 1
        complexity_nodes = {"if_statement", "for_statement", "while_statement", "switch_statement", "try_statement"}

        for node in self._walk(root):
            if node.type == "import_statement":
                source_node = node.child_by_field_name("source")
                if source_node:
                    result.imports.append(self._strip_quotes(self._decode_text(source_node)))
            elif node.type == "call_expression":
                fn = node.child_by_field_name("function")
                if fn and self._decode_text(fn) == "require":
                    args = node.child_by_field_name("arguments")
                    if args:
                        match = re.search(r"""['"]([^'"]+)['"]""", self._decode_text(args))
                        if match:
                            result.imports.append(match.group(1))
            elif node.type in {"function_declaration", "method_definition"}:
                name_node = node.child_by_field_name("name")
                if name_node:
                    normalized = self._decode_text(name_node).lstrip("_")
                    if normalized:
                        result.public_functions.append(normalized)
            elif node.type == "class_declaration":
                name_node = node.child_by_field_name("name")
                if not name_node:
                    continue
                class_name = self._decode_text(name_node)
                heritage = ""
                for child in node.children:
                    if child.type == "class_heritage":
                        heritage = self._decode_text(child)
                        break
                bases: list[str] = []
                if heritage.startswith("extends "):
                    bases = [heritage.replace("extends ", "", 1).strip()]
                result.class_inheritance[class_name] = bases
                result.classes.append(self._format_class_signature(class_name, bases))
            elif node.type in complexity_nodes:
                complexity += 1

        result.imports = sorted(set(i for i in result.imports if i))
        result.public_functions = sorted(set(result.public_functions))
        result.classes = sorted(set(result.classes))
        result.complexity_score = float(complexity)
        return result

    def _analyze_sql_ts(self, source: str) -> StructuredExtraction:
        root, parse_issues = self._parse_tree("sql", source)
        result = StructuredExtraction(parse_issues=parse_issues)
        if root is None:
            result.skipped = True
            return result

        query_structure: set[str] = set()
        table_refs: set[str] = set()
        complexity = 1

        for node in self._walk(root):
            operation = self._sql_operation(node.type)
            if operation:
                query_structure.add(operation)
                complexity += 1
            table = self._sql_table_candidate(node)
            if table:
                table_refs.add(table)

        result.sql_query_structure = sorted(query_structure)
        result.sql_table_references = sorted(table_refs)
        result.complexity_score = float(complexity)
        if not result.sql_table_references and any("syntax errors" in issue for issue in result.parse_issues):
            result.skipped = True
        return result

    def _sql_operation(self, node_type: str) -> str:
        lowered = node_type.lower()
        if "select" in lowered:
            return "select"
        if "insert" in lowered:
            return "insert"
        if "update" in lowered:
            return "update"
        if "delete" in lowered:
            return "delete"
        if "merge" in lowered:
            return "merge"
        if "create" in lowered and "view" in lowered:
            return "create_view"
        if "create" in lowered and "table" in lowered:
            return "create_table"
        if "join" in lowered:
            return "join"
        if "where" in lowered:
            return "where"
        if "group" in lowered and "by" in lowered:
            return "group_by"
        if "order" in lowered and "by" in lowered:
            return "order_by"
        if "with" in lowered:
            return "cte"
        if "union" in lowered:
            return "union"
        return ""

    def _sql_table_candidate(self, node: object) -> str:
        node_type = node.type.lower()
        parent_type = node.parent.type.lower() if getattr(node, "parent", None) else ""
        tableish_node = any(
            token in node_type
            for token in ("table", "relation", "from", "join", "target", "source", "identifier", "object_reference")
        )
        tableish_parent = any(
            token in parent_type
            for token in ("from", "join", "into", "update", "delete", "merge", "table", "using")
        )
        if not tableish_node and not tableish_parent:
            return ""

        name_node = node.child_by_field_name("name")
        raw = self._decode_text(name_node or node).strip()
        cleaned = self._normalize_sql_identifier(raw)
        if not cleaned:
            return ""
        if "." in cleaned:
            parts = cleaned.split(".")
            if any((not p) or (p.lower() in self.SQL_KEYWORDS) for p in parts):
                return ""
        if cleaned.lower() in self.SQL_KEYWORDS:
            return ""
        # Treat common function calls and literals as non-table identifiers.
        if "(" in cleaned or ")" in cleaned or cleaned.isdigit():
            return ""
        return cleaned

    def _normalize_sql_identifier(self, text: str) -> str:
        cleaned = text.strip().strip(",;")
        if not cleaned:
            return ""
        cleaned = cleaned.replace("`", "").replace('"', "").replace("[", "").replace("]", "")
        tokens = [tok for tok in cleaned.split() if tok]
        if not tokens:
            return ""
        if len(tokens) >= 2 and tokens[0].lower() in {
            "from",
            "join",
            "into",
            "update",
            "using",
            "table",
            "delete",
            "merge",
        }:
            return tokens[1]
        return tokens[0]

    def _analyze_yaml_ts(self, source: str) -> StructuredExtraction:
        root, parse_issues = self._parse_tree("yaml", source)
        result = StructuredExtraction(parse_issues=parse_issues)
        if root is None:
            result.public_functions = self._analyze_yaml_top_keys(source)
            result.skipped = not result.public_functions
            return result

        top_keys: list[str] = []
        hierarchy: set[str] = set()
        complexity = 1

        for node in self._walk(root):
            if node.type != "block_mapping_pair":
                continue
            key = self._extract_yaml_key(node)
            if not key:
                continue
            top_keys.append(key)
            complexity += 1
            path = self._build_yaml_key_path(node)
            if path:
                hierarchy.add(path)

        result.public_functions = sorted(set(top_keys))
        result.yaml_key_hierarchy = sorted(hierarchy)
        result.complexity_score = float(complexity)
        if not result.yaml_key_hierarchy and any("syntax errors" in issue for issue in result.parse_issues):
            result.skipped = True
        return result

    def _extract_yaml_key(self, node: object) -> str:
        if not getattr(node, "children", None):
            return ""
        first = node.children[0]
        key = self._decode_text(first).strip().strip(":")
        return key.strip("'").strip('"')

    def _build_yaml_key_path(self, node: object) -> str:
        segments: list[str] = []
        cursor = node
        sequence_seen = False

        while cursor is not None:
            cursor_type = getattr(cursor, "type", "")
            if "sequence" in cursor_type:
                sequence_seen = True
            if cursor_type == "block_mapping_pair":
                key = self._extract_yaml_key(cursor)
                if key:
                    segment = f"{key}[]" if sequence_seen else key
                    segments.append(segment)
                    sequence_seen = False
            cursor = getattr(cursor, "parent", None)

        if not segments:
            return ""
        segments.reverse()
        return ".".join(segments)

    def _format_class_signature(self, class_name: str, bases: list[str]) -> str:
        if not bases:
            return class_name
        return f"{class_name}({', '.join(bases)})"

    def _extract_aliased_symbol(self, node: object) -> str:
        text = self._decode_text(node)
        return text.split(" as ", 1)[0].strip()

    def _decode_text(self, node: object | None) -> str:
        if node is None:
            return ""
        return node.text.decode("utf-8", errors="ignore")

    def _strip_quotes(self, text: str) -> str:
        return text.strip().strip("'").strip('"')

    def _walk(self, root: object) -> Iterable[object]:
        stack = [root]
        while stack:
            node = stack.pop()
            yield node
            children = list(getattr(node, "children", []))
            stack.extend(reversed(children))

    def _analyze_js_imports(self, source: str) -> list[str]:
        pattern = r"(?:import\s+.*?\s+from\s+['\"]([^'\"]+)['\"]|require\(\s*['\"]([^'\"]+)['\"]\s*\))"
        out: list[str] = []
        for match in re.finditer(pattern, source):
            out.append((match.group(1) or match.group(2) or "").strip())
        return sorted(set(o for o in out if o))

    def _analyze_js_functions(self, source: str) -> list[str]:
        out = re.findall(r"(?:function\s+([a-zA-Z_]\w*)\s*\(|const\s+([a-zA-Z_]\w*)\s*=\s*\()", source)
        names: list[str] = []
        for first, second in out:
            name = first or second
            if name and not name.startswith("_"):
                names.append(name)
        return sorted(set(names))

    def _analyze_yaml_top_keys(self, source: str) -> list[str]:
        keys = re.findall(r"^([a-zA-Z_][\w-]*):", source, flags=re.MULTILINE)
        return sorted(set(keys))
