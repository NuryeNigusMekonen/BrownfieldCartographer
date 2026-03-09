from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable

from tree_sitter import Language, Parser
import tree_sitter_javascript
import tree_sitter_python
import tree_sitter_sql
import tree_sitter_typescript
import tree_sitter_yaml


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
    Primary static analyzer implemented with tree-sitter grammars.
    AST/regex helpers are used only as targeted fallbacks for resilience.
    """

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
                parser = Parser(language)
                self.parsers[language_name] = parser
            except Exception:
                # Keep parser map sparse; callers degrade gracefully.
                continue

    def iter_supported_files(self, root: Path) -> Iterable[Path]:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS:
                if any(part in IGNORED_DIRS for part in path.parts):
                    continue
                yield path

    def analyze_module(self, path: Path, root: Path) -> ModuleAnalysis:
        language = self.router.route(path)
        text = path.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
        loc = len(lines)
        comments = [ln for ln in lines if ln.strip().startswith(("#", "//", "--"))]
        comment_ratio = (len(comments) / loc) if loc else 0.0

        imports: list[str] = []
        public_functions: list[str] = []
        classes: list[str] = []
        class_inheritance: dict[str, list[str]] = {}
        function_signatures: dict[str, str] = {}
        function_calls: list[tuple[str, str]] = []
        complexity_score = 0.0

        if language == "python" and language in self.parsers:
            (
                imports,
                public_functions,
                classes,
                class_inheritance,
                complexity_score,
            ) = self._analyze_python_ts(text)
            function_signatures, function_calls = self._analyze_python_ast_details(text)
        elif language == "python":
            (
                imports,
                public_functions,
                classes,
                class_inheritance,
                function_signatures,
                function_calls,
                complexity_score,
            ) = self._analyze_python(text)
        elif language in {"javascript", "typescript"} and language in self.parsers:
            imports, public_functions, classes, class_inheritance, complexity_score = self._analyze_js_ts(
                text, language
            )
        elif language in {"javascript", "typescript"}:
            imports = self._analyze_js_imports(text)
            public_functions = self._analyze_js_functions(text)
            complexity_score = float(text.count("if (") + text.count("for (") + text.count("while (") + 1.0)
        elif language == "yaml" and language in self.parsers:
            public_functions, complexity_score = self._analyze_yaml_ts(text)
        elif language == "yaml":
            public_functions = self._analyze_yaml_top_keys(text)
        elif language == "sql" and language in self.parsers:
            complexity_score = self._analyze_sql_ts(text)

        rel_path = str(path.relative_to(root))
        return ModuleAnalysis(
            path=rel_path,
            language=language,
            imports=imports,
            public_functions=public_functions,
            classes=classes,
            class_inheritance=class_inheritance,
            function_signatures=function_signatures,
            function_calls=function_calls,
            complexity_score=complexity_score,
            loc=loc,
            comment_ratio=comment_ratio,
        )

    def _analyze_python(
        self, source: str
    ) -> tuple[
        list[str],
        list[str],
        list[str],
        dict[str, list[str]],
        dict[str, str],
        list[tuple[str, str]],
        float,
    ]:
        imports: list[str] = []
        public_functions: list[str] = []
        classes: list[str] = []
        class_inheritance: dict[str, list[str]] = {}
        function_signatures: dict[str, str] = {}
        function_calls: list[tuple[str, str]] = []
        complexity = 1
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return imports, public_functions, classes, class_inheritance, function_signatures, function_calls, float(
                complexity
            )

        # Build function signature index first.
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                arg_names = [a.arg for a in node.args.args]
                signature = f"{node.name}({', '.join(arg_names)})"
                function_signatures[node.name] = signature

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    prefix = "." * node.level if node.level else ""
                    imports.append(f"{prefix}{node.module}")
                elif node.level:
                    for alias in node.names:
                        imports.append(f"{'.' * node.level}{alias.name}")
            elif isinstance(node, ast.FunctionDef):
                normalized = node.name.lstrip("_")
                if normalized:
                    public_functions.append(normalized)
            elif isinstance(node, ast.AsyncFunctionDef):
                normalized = node.name.lstrip("_")
                if normalized:
                    public_functions.append(normalized)
            elif isinstance(node, ast.ClassDef):
                bases = [self._resolve_python_ast_name(base) for base in node.bases]
                clean_bases = [b for b in bases if b]
                class_inheritance[node.name] = clean_bases
                classes.append(self._format_class_signature(node.name, clean_bases))
            elif isinstance(node, (ast.If, ast.For, ast.While, ast.Try, ast.Match, ast.BoolOp)):
                complexity += 1
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                caller = node.name
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Call):
                        callee = self._resolve_call_name(inner)
                        if callee:
                            function_calls.append((caller, callee))

        return (
            sorted(set(imports)),
            sorted(set(public_functions)),
            sorted(set(classes)),
            class_inheritance,
            function_signatures,
            sorted(set(function_calls)),
            float(complexity),
        )

    def _analyze_python_ast_details(self, source: str) -> tuple[dict[str, str], list[tuple[str, str]]]:
        function_signatures: dict[str, str] = {}
        function_calls: list[tuple[str, str]] = []
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return function_signatures, function_calls
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                arg_names = [a.arg for a in node.args.args]
                signature = f"{node.name}({', '.join(arg_names)})"
                function_signatures[node.name] = signature
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                caller = node.name
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Call):
                        callee = self._resolve_call_name(inner)
                        if callee:
                            function_calls.append((caller, callee))
        return function_signatures, sorted(set(function_calls))

    def _analyze_python_ts(
        self, source: str
    ) -> tuple[list[str], list[str], list[str], dict[str, list[str]], float]:
        parser = self.parsers["python"]
        tree = parser.parse(source.encode("utf-8", errors="ignore"))
        root = tree.root_node
        imports: list[str] = []
        public_functions: list[str] = []
        classes: list[str] = []
        class_inheritance: dict[str, list[str]] = {}
        complexity = 1
        complexity_nodes = {"if_statement", "for_statement", "while_statement", "try_statement", "with_statement"}

        for node in self._walk(root):
            if node.type == "import_statement":
                imports.extend(self._extract_python_import_statement(node))
            elif node.type == "import_from_statement":
                imports.extend(self._extract_python_from_import(node))
            elif node.type in {"function_definition", "async_function_definition"}:
                name_node = node.child_by_field_name("name")
                if name_node:
                    normalized = self._decode_text(name_node).lstrip("_")
                    if normalized:
                        public_functions.append(normalized)
            elif node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if not name_node:
                    continue
                class_name = self._decode_text(name_node)
                bases = self._extract_python_bases(node)
                class_inheritance[class_name] = bases
                classes.append(self._format_class_signature(class_name, bases))
            elif node.type in complexity_nodes:
                complexity += 1

        return (
            sorted(set(imports)),
            sorted(set(public_functions)),
            sorted(set(classes)),
            class_inheritance,
            float(complexity),
        )

    def _resolve_call_name(self, node: ast.Call) -> str | None:
        if isinstance(node.func, ast.Name):
            return node.func.id
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        return None

    def _resolve_python_ast_name(self, node: ast.expr) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            parts: list[str] = []
            current: ast.expr | None = node
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            return ".".join(reversed(parts))
        return ""

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
        module_text = self._decode_text(module_node) if module_node else ""
        imported_names: list[str] = []

        for child in node.children:
            if module_node is not None and child.id == module_node.id:
                continue
            if child.type == "dotted_name":
                imported_names.append(self._decode_text(child))
            elif child.type == "aliased_import":
                imported_names.append(self._extract_aliased_symbol(child))
            elif child.type == "wildcard_import":
                imported_names.append("*")

        if not imported_names:
            if module_text:
                out.append(module_text)
            return out

        for symbol in imported_names:
            if module_text in {".", ".."} and symbol != "*":
                out.append(f"{module_text}{symbol}")
            elif module_text:
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

    def _format_class_signature(self, class_name: str, bases: list[str]) -> str:
        if not bases:
            return class_name
        return f"{class_name}({', '.join(bases)})"

    def _analyze_js_ts(
        self, source: str, language: str
    ) -> tuple[list[str], list[str], list[str], dict[str, list[str]], float]:
        parser = self.parsers[language]
        tree = parser.parse(source.encode("utf-8", errors="ignore"))
        root = tree.root_node
        imports: list[str] = []
        public_functions: list[str] = []
        classes: list[str] = []
        class_inheritance: dict[str, list[str]] = {}
        complexity = 1
        complexity_nodes = {"if_statement", "for_statement", "while_statement", "switch_statement", "try_statement"}

        for node in self._walk(root):
            if node.type == "import_statement":
                source_node = node.child_by_field_name("source")
                if source_node:
                    imports.append(self._strip_quotes(self._decode_text(source_node)))
            elif node.type == "call_expression":
                fn = node.child_by_field_name("function")
                if fn and self._decode_text(fn) == "require":
                    args = node.child_by_field_name("arguments")
                    if args:
                        m = re.search(r"""['"]([^'"]+)['"]""", self._decode_text(args))
                        if m:
                            imports.append(m.group(1))
            elif node.type in {"function_declaration", "method_definition"}:
                name_node = node.child_by_field_name("name")
                if name_node:
                    normalized = self._decode_text(name_node).lstrip("_")
                    if normalized:
                        public_functions.append(normalized)
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
                class_inheritance[class_name] = bases
                classes.append(self._format_class_signature(class_name, bases))
            elif node.type in complexity_nodes:
                complexity += 1

        return (
            sorted(set(i for i in imports if i)),
            sorted(set(public_functions)),
            sorted(set(classes)),
            class_inheritance,
            float(complexity),
        )

    def _analyze_sql_ts(self, source: str) -> float:
        parser = self.parsers["sql"]
        tree = parser.parse(source.encode("utf-8", errors="ignore"))
        root = tree.root_node
        complexity = 1
        complexity_nodes = {"select", "join", "cte", "union", "where", "group_by", "order_by"}
        for node in self._walk(root):
            if node.type in complexity_nodes:
                complexity += 1
        return float(complexity)

    def _analyze_yaml_ts(self, source: str) -> tuple[list[str], float]:
        parser = self.parsers["yaml"]
        tree = parser.parse(source.encode("utf-8", errors="ignore"))
        root = tree.root_node
        top_keys: list[str] = []
        complexity = 1
        for node in self._walk(root):
            if node.type == "block_mapping_pair":
                key = self._extract_yaml_key(node)
                if key:
                    top_keys.append(key)
                    complexity += 1
        return sorted(set(top_keys)), float(complexity)

    def _extract_yaml_key(self, node: object) -> str:
        if not getattr(node, "children", None):
            return ""
        first = node.children[0]
        return self._decode_text(first).strip()

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
        for m in re.finditer(pattern, source):
            out.append((m.group(1) or m.group(2) or "").strip())
        return sorted(set([o for o in out if o]))

    def _analyze_js_functions(self, source: str) -> list[str]:
        out = re.findall(r"(?:function\s+([a-zA-Z_]\w*)\s*\(|const\s+([a-zA-Z_]\w*)\s*=\s*\()", source)
        names: list[str] = []
        for a, b in out:
            name = a or b
            if name and not name.startswith("_"):
                names.append(name)
        return sorted(set(names))

    def _analyze_yaml_top_keys(self, source: str) -> list[str]:
        keys = re.findall(r"^([a-zA-Z_][\w-]*):", source, flags=re.MULTILINE)
        return sorted(set(keys))
