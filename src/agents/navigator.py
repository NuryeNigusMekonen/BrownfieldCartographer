from __future__ import annotations

from typing import TypedDict
from typing import Any

from src.graph.knowledge_graph import KnowledgeGraph


class NavigatorAgent:
    def __init__(self, module_graph: KnowledgeGraph, lineage_graph: KnowledgeGraph) -> None:
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph

    def find_implementation(self, concept: str) -> list[dict]:
        concept = concept.lower()
        matches: list[dict] = []
        for node, attrs in self.module_graph.graph.nodes(data=True):
            text = " ".join(
                [
                    str(attrs.get("path", "")),
                    str(attrs.get("purpose_statement", "")),
                    " ".join(attrs.get("public_functions", [])),
                ]
            ).lower()
            if concept in text:
                matches.append(
                    {
                        "module": node,
                        "evidence": {
                            "source_file": attrs.get("path", node),
                            "line_range": [1, max(int(attrs.get("loc", 1)), 1)],
                            "analysis_method": "semantic_index_match",
                        },
                    }
                )
        return matches[:20]

    def trace_lineage(self, dataset: str, direction: str = "upstream") -> list[dict]:
        node = dataset if dataset.startswith("dataset::") else f"dataset::{dataset}"
        if direction == "upstream":
            nodes = self.lineage_graph.upstream(node)
        else:
            nodes = self.lineage_graph.downstream(node)
        return [{"node": n, "evidence": self._edge_evidence(node, n)} for n in nodes]

    def blast_radius(self, module_path: str) -> list[dict]:
        if module_path in self.module_graph.graph:
            downstream = self.module_graph.downstream(module_path)
            return [
                {
                    "node": n,
                    "evidence": {
                        "source_file": module_path,
                        "line_range": [1, 1],
                        "analysis_method": "module_import_graph_traversal",
                    },
                }
                for n in downstream
            ]
        return []

    def explain_module(self, path: str) -> dict:
        attrs = self.module_graph.graph.nodes.get(path, {})
        if not attrs:
            return {"error": f"No module found for '{path}'."}
        return {
            "module": path,
            "language": attrs.get("language", "unknown"),
            "purpose": attrs.get("purpose_statement", "N/A"),
            "complexity": attrs.get("complexity_score", 0),
            "public_api": attrs.get("public_functions", []),
            "evidence": {
                "source_file": attrs.get("path", path),
                "line_range": [1, max(int(attrs.get("loc", 1)), 1)],
                "analysis_method": "static_analysis_plus_semanticist",
            },
        }

    def _edge_evidence(self, source: str, target: str) -> dict:
        if self.lineage_graph.graph.has_edge(source, target):
            attrs = self.lineage_graph.graph.edges[source, target]
            return {
                "source_file": attrs.get("source_file", ""),
                "line_range": list(attrs.get("line_range", (0, 0))),
                "analysis_method": attrs.get("analysis_method", "graph_traversal"),
            }
        if self.lineage_graph.graph.has_edge(target, source):
            attrs = self.lineage_graph.graph.edges[target, source]
            return {
                "source_file": attrs.get("source_file", ""),
                "line_range": list(attrs.get("line_range", (0, 0))),
                "analysis_method": attrs.get("analysis_method", "graph_traversal"),
            }
        return {"source_file": "", "line_range": [0, 0], "analysis_method": "graph_traversal"}


class NavigatorLangGraphAgent:
    """
    Optional LangGraph-backed navigator wrapper.
    Falls back to direct tool routing if LangGraph isn't installed.
    """

    def __init__(self, navigator: NavigatorAgent) -> None:
        self.navigator = navigator
        self._langgraph_available = self._check_langgraph()
        self._compiled_graph: Any | None = None
        if self._langgraph_available:
            self._compiled_graph = self._build_langgraph()

    def run(self, tool: str, arg: str, direction: str = "upstream") -> Any:
        if self._compiled_graph is not None:
            state = {
                "tool": tool,
                "arg": arg,
                "direction": direction,
                "result": None,
                "error": None,
            }
            final_state = self._compiled_graph.invoke(state)
            if isinstance(final_state, dict):
                if final_state.get("error"):
                    return {"error": str(final_state["error"])}
                return final_state.get("result")
        return self._route(tool, arg, direction)

    def _route(self, tool: str, arg: str, direction: str) -> Any:
        if tool == "find_implementation":
            return self.navigator.find_implementation(arg)
        if tool == "trace_lineage":
            return self.navigator.trace_lineage(arg, direction=direction)
        if tool == "blast_radius":
            return self.navigator.blast_radius(arg)
        if tool == "explain_module":
            return self.navigator.explain_module(arg)
        return {"error": f"Unknown tool '{tool}'."}

    def _check_langgraph(self) -> bool:
        try:
            import langgraph  # noqa: F401

            return True
        except Exception:
            return False

    def _build_langgraph(self) -> Any | None:
        try:
            from langgraph.graph import END, StateGraph
        except Exception:
            return None

        class NavigatorState(TypedDict):
            tool: str
            arg: str
            direction: str
            result: Any
            error: str | None

        graph = StateGraph(NavigatorState)
        graph.add_node("route", self._node_route)
        graph.add_node("find_implementation", self._node_find_implementation)
        graph.add_node("trace_lineage", self._node_trace_lineage)
        graph.add_node("blast_radius", self._node_blast_radius)
        graph.add_node("explain_module", self._node_explain_module)
        graph.add_node("unknown_tool", self._node_unknown_tool)
        graph.set_entry_point("route")
        graph.add_conditional_edges(
            "route",
            self._route_decision,
            {
                "find_implementation": "find_implementation",
                "trace_lineage": "trace_lineage",
                "blast_radius": "blast_radius",
                "explain_module": "explain_module",
                "unknown_tool": "unknown_tool",
            },
        )
        graph.add_edge("find_implementation", END)
        graph.add_edge("trace_lineage", END)
        graph.add_edge("blast_radius", END)
        graph.add_edge("explain_module", END)
        graph.add_edge("unknown_tool", END)
        return graph.compile()

    def _node_route(self, state: dict[str, Any]) -> dict[str, Any]:
        # Router node intentionally does not mutate state.
        return state

    def _route_decision(self, state: dict[str, Any]) -> str:
        tool = str(state.get("tool", ""))
        if tool in {"find_implementation", "trace_lineage", "blast_radius", "explain_module"}:
            return tool
        return "unknown_tool"

    def _node_find_implementation(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            **state,
            "result": self.navigator.find_implementation(str(state.get("arg", ""))),
            "error": None,
        }

    def _node_trace_lineage(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            **state,
            "result": self.navigator.trace_lineage(
                str(state.get("arg", "")),
                direction=str(state.get("direction", "upstream")),
            ),
            "error": None,
        }

    def _node_blast_radius(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            **state,
            "result": self.navigator.blast_radius(str(state.get("arg", ""))),
            "error": None,
        }

    def _node_explain_module(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            **state,
            "result": self.navigator.explain_module(str(state.get("arg", ""))),
            "error": None,
        }

    def _node_unknown_tool(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            **state,
            "result": None,
            "error": f"Unknown tool '{state.get('tool', '')}'.",
        }
