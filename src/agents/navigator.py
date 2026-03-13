from __future__ import annotations

import re
from typing import Any
from typing import TypedDict

import networkx as nx

from src.agents.hydrologist import HydrologistAgent
from src.graph.knowledge_graph import KnowledgeGraph

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
except Exception:  # pragma: no cover - defensive fallback when sklearn is unavailable
    TfidfVectorizer = None
    cosine_similarity = None

try:
    from langgraph.graph import END, StateGraph
except Exception as exc:  # pragma: no cover - exercised when dependency is absent
    END = None
    StateGraph = None
    _LANGGRAPH_IMPORT_ERROR: Exception | None = exc
else:
    _LANGGRAPH_IMPORT_ERROR = None


class NavigatorEvidence(TypedDict, total=False):
    source_file: str
    line_range: list[int]
    analysis_method: str
    node: str
    module: str
    transformation_type: str


class NavigatorState(TypedDict):
    tool: str
    arg: str
    direction: str
    result: Any
    error: str | None
    evidence: list[NavigatorEvidence]


class NavigatorQueryState(TypedDict):
    query: str
    tool: str
    arg: str
    direction: str
    plan: list[dict[str, str]]
    step_results: list[dict[str, Any]]
    result: Any
    error: str | None
    evidence: list[NavigatorEvidence]


class _NavigatorTools:
    def __init__(self, module_graph: KnowledgeGraph, lineage_graph: KnowledgeGraph) -> None:
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self._hydrologist = HydrologistAgent(lineage_graph=lineage_graph)
        self._semantic_records_cache: list[dict[str, Any]] | None = None
        self._semantic_vectorizer: Any = None
        self._semantic_matrix: Any = None

    def find_implementation(self, concept: str) -> dict[str, Any]:
        query = concept.strip()
        if not query:
            evidence = [self._missing_evidence("semantic_index/module_purpose_index.jsonl", "vector_similarity_semantic_index")]
            return {
                "query": concept,
                "matches": [],
                "match_count": 0,
                "message": "Provide a concept to search (for example: find_implementation ingest pipeline).",
                "evidence": evidence,
            }

        records = self._semantic_records()
        if not records:
            evidence = [self._missing_evidence("module_graph.json", "vector_similarity_semantic_index")]
            return {
                "query": concept,
                "matches": [],
                "match_count": 0,
                "message": "No module semantic index is available for this repository.",
                "evidence": evidence,
            }

        scores = self._scores_for_query(query, records)
        ranked = sorted(
            scores,
            key=lambda item: (-float(item["score"]), str(item["record"].get("path", ""))),
        )

        matches: list[dict[str, Any]] = []
        for row in ranked:
            score = float(row["score"])
            if score <= 0:
                continue
            record = row["record"]
            attrs = dict(record.get("attrs", {}))
            module_id = str(record.get("module_id", ""))
            line_range = self._coerce_line_range([1, attrs.get("loc", 1)])
            evidence = self._normalize_evidence(
                {
                    "source_file": str(record.get("path", module_id)),
                    "line_range": line_range,
                    "analysis_method": "vector_similarity_semantic_index_hybrid_llm_static",
                    "module": module_id,
                }
            )
            matches.append(
                {
                    "module": module_id,
                    "language": str(attrs.get("language", "unknown")),
                    "purpose": str(attrs.get("purpose_statement", "")),
                    "domain_cluster": str(attrs.get("domain_cluster", "")),
                    "public_api": list(attrs.get("public_functions", [])),
                    "similarity_score": round(score, 6),
                    "evidence": evidence,
                }
            )
            if len(matches) >= 20:
                break

        evidence = self._collect_nested_evidence(matches)
        if not evidence:
            evidence = [self._missing_evidence("semantic_index/module_purpose_index.jsonl", "vector_similarity_semantic_index")]

        return {
            "query": concept,
            "matches": matches,
            "match_count": len(matches),
            "message": (
                "No implementation candidates found for this concept."
                if not matches
                else "Ranked implementation candidates using semantic vector similarity."
            ),
            "evidence": evidence,
        }

    def trace_lineage(self, dataset: str, direction: str = "upstream") -> dict[str, Any]:
        normalized_direction = "downstream" if direction.strip().lower() == "downstream" else "upstream"
        if normalized_direction == "downstream":
            payload = self._hydrologist.what_depends_on_output(dataset)
            direct_nodes = list(payload.get("direct_downstream", []))
            nodes = list(payload.get("full_downstream", []))
        else:
            payload = self._hydrologist.what_feeds_table(dataset)
            direct_nodes = list(payload.get("direct_upstream", []))
            nodes = list(payload.get("full_upstream", []))

        target = str(payload.get("target", self._normalize_dataset_name(dataset)))
        evidence = [self._normalize_evidence(item) for item in list(payload.get("evidence", [])) if isinstance(item, dict)]
        if not evidence:
            evidence = [self._missing_evidence(target, "static_lineage_graph_lookup", node=target)]

        return {
            "target": target,
            "direction": normalized_direction,
            "direct_nodes": direct_nodes,
            "nodes": nodes,
            "node_count": len(nodes),
            "message": (
                f"No lineage nodes found for '{target}'."
                if not nodes
                else f"Found {len(nodes)} {normalized_direction} lineage nodes for '{target}'."
            ),
            "evidence": evidence,
        }

    def blast_radius(self, target: str) -> dict[str, Any]:
        module_path = self._resolve_module_path(target)
        if module_path:
            impacted_nodes = self._module_blast_radius(module_path)
            evidence = self._collect_nested_evidence(impacted_nodes)
            if not evidence:
                evidence = [self._missing_evidence(module_path, "static_module_import_graph_traversal", module=module_path)]
            return {
                "target": module_path,
                "target_type": "module",
                "impacted_nodes": impacted_nodes,
                "impact_count": len(impacted_nodes),
                "message": (
                    f"No downstream module dependencies found for '{module_path}'."
                    if not impacted_nodes
                    else f"Found {len(impacted_nodes)} impacted modules from '{module_path}'."
                ),
                "evidence": evidence,
            }
        if self._looks_like_module_reference(target):
            reference = target.strip() or "<unknown>"
            evidence = [self._missing_evidence(reference, "static_module_lookup", module=reference)]
            return {
                "target": reference,
                "target_type": "module",
                "impacted_nodes": [],
                "impact_count": 0,
                "message": f"No module found for '{reference}'.",
                "evidence": evidence,
            }

        payload = self._hydrologist.blast_radius(target)
        if isinstance(payload, dict):
            payload_evidence = [self._normalize_evidence(item) for item in list(payload.get("evidence", [])) if isinstance(item, dict)]
            payload_target = str(payload.get("target", self._normalize_dataset_name(target)))
            if not payload_evidence:
                payload_evidence = [self._missing_evidence(payload_target, "static_lineage_graph_lookup", node=payload_target)]
            impacted_nodes = list(payload.get("impacted_nodes", []))
            return {
                **payload,
                "target_type": self._lineage_node_type(payload_target),
                "message": (
                    f"No downstream lineage impact found for '{payload_target}'."
                    if not impacted_nodes
                    else f"Found {len(impacted_nodes)} impacted lineage nodes from '{payload_target}'."
                ),
                "evidence": payload_evidence,
            }
        unknown_target = target.strip() or "<unknown>"
        fallback = [self._missing_evidence(unknown_target, "static_graph_lookup", node=unknown_target)]
        return {
            "target": unknown_target,
            "target_type": "unknown",
            "impacted_nodes": [],
            "impact_count": 0,
            "message": f"No graph target found for '{unknown_target}'.",
            "evidence": fallback,
        }

    def explain_module(self, path: str) -> dict[str, Any]:
        module_path = self._resolve_module_path(path)
        if not module_path:
            reference = path.strip() or "<unknown>"
            evidence = [self._missing_evidence(reference, "static_module_lookup", module=reference)]
            return {
                "error": f"No module found for '{reference}'.",
                "message": f"Could not resolve module path '{reference}'.",
                "evidence": evidence,
            }

        attrs = dict(self.module_graph.module_import_graph().nodes.get(module_path, {}))
        evidence = self._normalize_evidence(
            {
                "source_file": str(attrs.get("path", module_path)),
                "line_range": [1, max(int(attrs.get("loc", 1)), 1)],
                "analysis_method": "hybrid_llm_static_module_semantics",
                "module": module_path,
            }
        )
        return {
            "module": module_path,
            "language": str(attrs.get("language", "unknown")),
            "purpose": str(attrs.get("purpose_statement", "N/A")),
            "complexity": attrs.get("complexity_score", 0),
            "public_api": list(attrs.get("public_functions", [])),
            "message": f"Module summary grounded in static graph metadata for '{module_path}'.",
            "evidence": evidence,
        }

    def _module_blast_radius(self, module_path: str) -> list[dict[str, Any]]:
        graph = self.module_graph.module_import_graph()
        if module_path not in graph:
            return []

        depths = nx.single_source_shortest_path_length(graph, module_path)
        impacted: list[dict[str, Any]] = []
        for node_id, depth in sorted(depths.items(), key=lambda item: (item[1], item[0])):
            if node_id == module_path:
                continue
            impacted.append(
                {
                    "node": node_id,
                    "node_type": "module",
                    "depth": int(depth),
                    "evidence": {
                        "source_file": module_path,
                        "line_range": [1, 1],
                        "analysis_method": "static_module_import_graph_traversal",
                        "module": module_path,
                    },
                }
            )
        return impacted

    def _collect_nested_evidence(self, entries: list[dict[str, Any]]) -> list[NavigatorEvidence]:
        evidence: list[NavigatorEvidence] = []
        for item in entries:
            raw = item.get("evidence")
            if isinstance(raw, dict):
                evidence.append(self._normalize_evidence(raw))
        return evidence

    def _missing_evidence(
        self,
        source_file: str,
        analysis_method: str,
        *,
        node: str | None = None,
        module: str | None = None,
    ) -> NavigatorEvidence:
        payload: dict[str, Any] = {
            "source_file": source_file or "<unknown>",
            "line_range": [1, 1],
            "analysis_method": analysis_method or "static_graph_lookup",
        }
        if node:
            payload["node"] = node
        if module:
            payload["module"] = module
        return self._normalize_evidence(payload)

    def _normalize_dataset_name(self, dataset_name: str) -> str:
        text = dataset_name.strip()
        if not text:
            return "dataset::"
        if text.startswith("dataset::"):
            return text
        return f"dataset::{text}"

    def _lineage_node_type(self, node_id: str) -> str:
        attrs = dict(self.lineage_graph.graph.nodes.get(node_id, {}))
        node_type = str(attrs.get("node_type") or attrs.get("type") or "").strip().lower()
        if node_type:
            return node_type
        if node_id.startswith("dataset::"):
            return "dataset"
        if node_id.startswith("transform::"):
            return "transformation"
        if node_id.startswith("pipeline::"):
            return "pipeline"
        if node_id.startswith("config::"):
            return "config"
        return "unknown"

    def _resolve_module_path(self, path: str) -> str | None:
        graph = self.module_graph.module_import_graph()
        candidate = path.strip()
        if not candidate:
            return None
        if candidate in graph:
            return candidate

        lowered = candidate.lower()
        exact_matches = [str(node_id) for node_id in graph.nodes if str(node_id).lower() == lowered]
        if exact_matches:
            return sorted(exact_matches)[0]

        suffix_matches = [
            str(node_id)
            for node_id in graph.nodes
            if str(node_id).lower().endswith(f"/{lowered}") or str(node_id).lower().endswith(f"\\{lowered}")
        ]
        if suffix_matches:
            return sorted(suffix_matches)[0]
        return None

    def _looks_like_module_reference(self, target: str) -> bool:
        candidate = target.strip().lower()
        if not candidate:
            return False
        module_suffixes = (
            ".py",
            ".sql",
            ".js",
            ".ts",
            ".tsx",
            ".jsx",
            ".java",
            ".kt",
            ".rb",
            ".go",
            ".rs",
            ".php",
            ".scala",
            ".sh",
        )
        return "/" in candidate or "\\" in candidate or candidate.endswith(module_suffixes)

    def _normalize_evidence(self, evidence: dict[str, Any]) -> NavigatorEvidence:
        line_range = self._coerce_line_range(evidence.get("line_range"))

        payload: NavigatorEvidence = {
            "source_file": str(evidence.get("source_file", "")).strip() or "<unknown>",
            "line_range": line_range,
            "analysis_method": str(evidence.get("analysis_method", "")).strip() or "static_graph_lookup",
        }
        if evidence.get("node"):
            payload["node"] = str(evidence["node"])
        if evidence.get("module"):
            payload["module"] = str(evidence["module"])
        if evidence.get("transformation_type"):
            payload["transformation_type"] = str(evidence["transformation_type"])
        return payload

    def _coerce_line_range(self, line_range: Any) -> list[int]:
        raw = line_range if line_range is not None else [1, 1]
        if isinstance(raw, tuple):
            raw = list(raw)
        if not isinstance(raw, list) or len(raw) != 2:
            raw = [1, 1]
        try:
            start = int(raw[0])
        except (TypeError, ValueError):
            start = 1
        try:
            end = int(raw[1])
        except (TypeError, ValueError):
            end = start
        if start <= 0:
            start = 1
        if end < start:
            end = start
        return [start, end]

    def _semantic_records(self) -> list[dict[str, Any]]:
        if self._semantic_records_cache is not None:
            return self._semantic_records_cache

        records: list[dict[str, Any]] = []
        graph = self.module_graph.module_import_graph()
        for node_id, attrs in graph.nodes(data=True):
            module_path = str(attrs.get("path", node_id))
            purpose = str(attrs.get("purpose_statement", ""))
            domain_cluster = str(attrs.get("domain_cluster", ""))
            public_api = " ".join(str(name) for name in attrs.get("public_functions", []))
            doc = " ".join([module_path, purpose, domain_cluster, public_api]).strip()
            records.append(
                {
                    "module_id": str(node_id),
                    "path": module_path,
                    "document": doc,
                    "attrs": dict(attrs),
                }
            )

        self._semantic_records_cache = records
        self._build_semantic_vectors(records)
        return records

    def _build_semantic_vectors(self, records: list[dict[str, Any]]) -> None:
        self._semantic_vectorizer = None
        self._semantic_matrix = None
        if not records or TfidfVectorizer is None:
            return
        try:
            vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), max_features=2048)
            matrix = vectorizer.fit_transform([str(record.get("document", "")) for record in records])
        except Exception:
            self._semantic_vectorizer = None
            self._semantic_matrix = None
            return
        self._semantic_vectorizer = vectorizer
        self._semantic_matrix = matrix

    def _scores_for_query(self, query: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if (
            self._semantic_vectorizer is not None
            and self._semantic_matrix is not None
            and cosine_similarity is not None
        ):
            try:
                query_vector = self._semantic_vectorizer.transform([query])
                similarities = cosine_similarity(query_vector, self._semantic_matrix).ravel().tolist()
                return [
                    {"record": record, "score": float(similarities[idx])}
                    for idx, record in enumerate(records)
                ]
            except Exception:
                pass

        query_lower = query.lower()
        scored = []
        for record in records:
            document = str(record.get("document", "")).lower()
            score = self._keyword_score(query_lower, document)
            scored.append({"record": record, "score": score})
        return scored

    def _keyword_score(self, query: str, document: str) -> float:
        tokens = [token for token in re.split(r"\W+", query.lower()) if len(token) > 1]
        if not tokens:
            return 0.0
        overlap = sum(document.count(token) for token in tokens)
        return float(overlap) / float(len(tokens))


class NavigatorLangGraphAgent:
    SUPPORTED_TOOLS = frozenset({"find_implementation", "trace_lineage", "blast_radius", "explain_module"})
    _AUTO_DISCOVERED_MODULE = "__AUTO_DISCOVERED_MODULE__"

    def __init__(self, module_graph: KnowledgeGraph, lineage_graph: KnowledgeGraph) -> None:
        if StateGraph is None or END is None:
            raise RuntimeError("Navigator requires the 'langgraph' package to be installed.") from _LANGGRAPH_IMPORT_ERROR
        self.tools = _NavigatorTools(module_graph=module_graph, lineage_graph=lineage_graph)
        self._compiled_graph = self._build_langgraph()
        self._compiled_query_graph = self._build_query_langgraph()

    def invoke(self, tool: str, arg: str, direction: str = "upstream") -> NavigatorState:
        initial_state: NavigatorState = {
            "tool": str(tool or "").strip().lower(),
            "arg": str(arg or "").strip(),
            "direction": "downstream" if str(direction or "").strip().lower() == "downstream" else "upstream",
            "result": None,
            "error": None,
            "evidence": [],
        }
        return self._compiled_graph.invoke(initial_state)

    def query(self, raw_query: str) -> NavigatorState:
        initial_state: NavigatorQueryState = {
            "query": str(raw_query or "").strip(),
            "tool": "",
            "arg": "",
            "direction": "upstream",
            "plan": [],
            "step_results": [],
            "result": None,
            "error": None,
            "evidence": [],
        }
        query_state = self._compiled_query_graph.invoke(initial_state)
        return {
            "tool": str(query_state.get("tool", "")),
            "arg": str(query_state.get("arg", "")),
            "direction": str(query_state.get("direction", "upstream")),
            "result": query_state.get("result"),
            "error": query_state.get("error"),
            "evidence": list(query_state.get("evidence", [])),
        }

    def run(self, tool: str, arg: str, direction: str = "upstream") -> Any:
        final_state = self.invoke(tool=tool, arg=arg, direction=direction)
        if final_state.get("error"):
            return {
                "error": str(final_state["error"]),
                "evidence": list(final_state.get("evidence", [])),
            }
        return final_state.get("result")

    def _build_langgraph(self) -> Any:
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

    def _build_query_langgraph(self) -> Any:
        graph = StateGraph(NavigatorQueryState)
        graph.add_node("plan_query", self._node_plan_query)
        graph.add_node("execute_step", self._node_execute_step)
        graph.add_node("finalize_query", self._node_finalize_query)
        graph.add_node("unknown_query", self._node_unknown_query)
        graph.set_entry_point("plan_query")
        graph.add_conditional_edges(
            "plan_query",
            self._query_plan_decision,
            {
                "execute_step": "execute_step",
                "unknown_query": "unknown_query",
            },
        )
        graph.add_conditional_edges(
            "execute_step",
            self._query_execution_decision,
            {
                "execute_step": "execute_step",
                "finalize_query": "finalize_query",
            },
        )
        graph.add_edge("finalize_query", END)
        graph.add_edge("unknown_query", END)
        return graph.compile()

    def _node_route(self, state: NavigatorState) -> NavigatorState:
        return state

    def _route_decision(self, state: NavigatorState) -> str:
        tool = str(state.get("tool", ""))
        if tool in self.SUPPORTED_TOOLS:
            return tool
        return "unknown_tool"

    def _node_find_implementation(self, state: NavigatorState) -> NavigatorState:
        return self._completed_state(state, self.tools.find_implementation(str(state.get("arg", ""))))

    def _node_trace_lineage(self, state: NavigatorState) -> NavigatorState:
        return self._completed_state(
            state,
            self.tools.trace_lineage(
                str(state.get("arg", "")),
                direction=str(state.get("direction", "upstream")),
            ),
        )

    def _node_blast_radius(self, state: NavigatorState) -> NavigatorState:
        return self._completed_state(state, self.tools.blast_radius(str(state.get("arg", ""))))

    def _node_explain_module(self, state: NavigatorState) -> NavigatorState:
        return self._completed_state(state, self.tools.explain_module(str(state.get("arg", ""))))

    def _node_unknown_tool(self, state: NavigatorState) -> NavigatorState:
        evidence = [self.tools._missing_evidence("navigator", "static_router_validation")]
        return {
            **state,
            "result": None,
            "error": f"Unknown tool '{state.get('tool', '')}'. Supported tools: {', '.join(sorted(self.SUPPORTED_TOOLS))}.",
            "evidence": evidence,
        }

    def _completed_state(self, state: NavigatorState, result: Any) -> NavigatorState:
        error = str(result.get("error")) if isinstance(result, dict) and result.get("error") else None
        evidence = self._collect_state_evidence(result)
        if not evidence:
            evidence = [self.tools._missing_evidence("navigator", "static_router_execution")]
        return {
            **state,
            "result": result,
            "error": error,
            "evidence": evidence,
        }

    def _collect_state_evidence(self, result: Any) -> list[NavigatorEvidence]:
        if isinstance(result, dict):
            raw_evidence = result.get("evidence")
            if isinstance(raw_evidence, dict):
                return [self.tools._normalize_evidence(raw_evidence)]
            if isinstance(raw_evidence, list):
                return [self.tools._normalize_evidence(item) for item in raw_evidence if isinstance(item, dict)]
            return []

        if isinstance(result, list):
            evidence: list[NavigatorEvidence] = []
            for item in result:
                if not isinstance(item, dict):
                    continue
                raw = item.get("evidence")
                if isinstance(raw, dict):
                    evidence.append(self.tools._normalize_evidence(raw))
            return evidence

        return []

    def _node_plan_query(self, state: NavigatorQueryState) -> NavigatorQueryState:
        query = str(state.get("query", "")).strip()
        plan = self._build_query_plan(query)
        if not plan:
            evidence = [self.tools._missing_evidence("navigator_query", "static_query_parsing")]
            return {
                **state,
                "error": (
                    "Supported commands: explain_module <path>, find_implementation <concept>, "
                    "trace_lineage <dataset>, blast_radius <target>."
                ),
                "evidence": evidence,
            }

        first = plan[0]
        return {
            **state,
            "tool": first["tool"],
            "arg": first["arg"],
            "direction": first["direction"],
            "plan": plan,
            "step_results": [],
            "result": None,
            "error": None,
            "evidence": [],
        }

    def _query_plan_decision(self, state: NavigatorQueryState) -> str:
        if state.get("error"):
            return "unknown_query"
        if state.get("plan"):
            return "execute_step"
        return "unknown_query"

    def _node_execute_step(self, state: NavigatorQueryState) -> NavigatorQueryState:
        plan = list(state.get("plan", []))
        step_results = list(state.get("step_results", []))
        step_idx = len(step_results)
        if step_idx >= len(plan):
            return state

        step = dict(plan[step_idx])
        step_tool = str(step.get("tool", ""))
        step_direction = str(step.get("direction", "upstream"))
        step_arg = str(step.get("arg", ""))
        if step_arg == self._AUTO_DISCOVERED_MODULE:
            inferred_module = self._infer_module_from_results(step_results)
            if not inferred_module:
                error = "No module could be auto-discovered from previous tool output."
                evidence = [self.tools._missing_evidence("navigator_chain", "static_chain_module_discovery")]
                step_result = {
                    "tool": step_tool,
                    "arg": "",
                    "direction": step_direction,
                    "result": {"error": error, "evidence": evidence},
                    "error": error,
                    "evidence": evidence,
                }
                return {
                    **state,
                    "tool": step_tool,
                    "arg": "",
                    "direction": step_direction,
                    "step_results": [*step_results, step_result],
                    "result": step_result["result"],
                    "error": error,
                    "evidence": self._merge_evidence(list(state.get("evidence", [])), evidence),
                }
            step_arg = inferred_module

        step_state = self.invoke(tool=step_tool, arg=step_arg, direction=step_direction)
        step_result = {
            "tool": step_tool,
            "arg": step_arg,
            "direction": step_direction,
            "result": step_state.get("result"),
            "error": step_state.get("error"),
            "evidence": list(step_state.get("evidence", [])),
        }
        merged_evidence = self._merge_evidence(
            list(state.get("evidence", [])),
            list(step_state.get("evidence", [])),
        )
        return {
            **state,
            "tool": step_tool,
            "arg": step_arg,
            "direction": step_direction,
            "step_results": [*step_results, step_result],
            "result": step_state.get("result"),
            "error": step_state.get("error"),
            "evidence": merged_evidence,
        }

    def _query_execution_decision(self, state: NavigatorQueryState) -> str:
        if state.get("error"):
            return "finalize_query"
        total_steps = len(state.get("plan", []))
        executed_steps = len(state.get("step_results", []))
        if executed_steps < total_steps:
            return "execute_step"
        return "finalize_query"

    def _node_finalize_query(self, state: NavigatorQueryState) -> NavigatorQueryState:
        step_results = list(state.get("step_results", []))
        if not step_results:
            evidence = list(state.get("evidence", []))
            if not evidence:
                evidence = [self.tools._missing_evidence("navigator_query", "static_query_execution")]
            return {
                **state,
                "result": None,
                "error": state.get("error") or "No query steps were executed.",
                "evidence": evidence,
            }

        if len(step_results) == 1:
            step = step_results[0]
            return {
                **state,
                "tool": str(step.get("tool", "")),
                "arg": str(step.get("arg", "")),
                "direction": str(step.get("direction", "upstream")),
                "result": step.get("result"),
                "error": step.get("error"),
                "evidence": self._merge_evidence([], list(step.get("evidence", []))),
            }

        chain_result = {
            "chain_length": len(step_results),
            "message": (
                "Tool chain terminated early because one step failed."
                if state.get("error")
                else "Tool chain executed successfully."
            ),
            "steps": step_results,
            "final_result": step_results[-1].get("result"),
        }
        first_step = step_results[0]
        return {
            **state,
            "tool": str(first_step.get("tool", "")),
            "arg": str(first_step.get("arg", "")),
            "direction": str(first_step.get("direction", "upstream")),
            "result": chain_result,
            "error": state.get("error"),
            "evidence": self._merge_evidence([], list(state.get("evidence", []))),
        }

    def _node_unknown_query(self, state: NavigatorQueryState) -> NavigatorQueryState:
        evidence = list(state.get("evidence", []))
        if not evidence:
            evidence = [self.tools._missing_evidence("navigator_query", "static_query_parsing")]
        return {
            **state,
            "result": None,
            "error": str(state.get("error") or "Could not parse query."),
            "evidence": evidence,
        }

    def _build_query_plan(self, raw_query: str) -> list[dict[str, str]]:
        text = raw_query.strip()
        if not text:
            return []

        lowered = text.lower()
        for marker in [" then explain", " and explain", " then describe module", " and describe module"]:
            idx = lowered.find(marker)
            if idx <= 0:
                continue
            first_query = text[:idx].strip(" ,.;")
            tool, arg, direction = self._parse_single_query(first_query)
            if tool in {"trace_lineage", "blast_radius"} and arg:
                return [
                    {"tool": tool, "arg": arg, "direction": direction},
                    {"tool": "explain_module", "arg": self._AUTO_DISCOVERED_MODULE, "direction": "upstream"},
                ]

        tool, arg, direction = self._parse_single_query(text)
        if not tool or not arg:
            return []
        return [{"tool": tool, "arg": arg, "direction": direction}]

    def _parse_single_query(self, raw_query: str) -> tuple[str, str, str]:
        text = raw_query.strip()
        lowered = text.lower()
        if not text:
            return "", "", "upstream"

        direct_match = re.match(
            r"^(explain_module|blast_radius|find_implementation|trace_lineage|upstream|downstream|feeds|depends_on|what_feeds_table|what_depends_on_output)\\s+(.+)$",
            text,
            flags=re.IGNORECASE,
        )
        if direct_match:
            raw_tool = direct_match.group(1).lower()
            tool = self._normalize_query_tool(raw_tool)
            arg = direct_match.group(2).strip()
            return tool, arg, self._normalize_query_direction(raw_tool, "upstream")

        if lowered.startswith("find implementation "):
            return "find_implementation", text[20:].strip(), "upstream"
        if lowered.startswith("find implementation of "):
            return "find_implementation", text[23:].strip(), "upstream"
        if lowered.startswith("where is ") and lowered.endswith(" implemented"):
            return "find_implementation", text[9:-12].strip(), "upstream"
        if lowered.startswith("which modules implement "):
            return "find_implementation", text[24:].strip(), "upstream"
        if lowered.startswith("trace lineage "):
            return "trace_lineage", text[14:].strip(), "upstream"
        if lowered.startswith("trace lineage of "):
            return "trace_lineage", text[17:].strip(), "upstream"
        if lowered.startswith("trace downstream "):
            return "trace_lineage", text[17:].strip(), "downstream"
        if lowered.startswith("trace downstream of "):
            return "trace_lineage", text[20:].strip(), "downstream"
        if lowered.startswith("upstream "):
            return "trace_lineage", text[9:].strip(), "upstream"
        if lowered.startswith("downstream "):
            return "trace_lineage", text[11:].strip(), "downstream"
        if lowered.startswith("what feeds table "):
            return "trace_lineage", text[17:].strip(), "upstream"
        if lowered.startswith("what depends on output "):
            return "trace_lineage", text[23:].strip(), "downstream"
        if lowered.startswith("feeds "):
            return "trace_lineage", text[6:].strip(), "upstream"
        if lowered.startswith("depends_on "):
            return "trace_lineage", text[11:].strip(), "downstream"
        if lowered.startswith("blast radius "):
            return "blast_radius", text[13:].strip(), "upstream"
        if lowered.startswith("compute blast radius of "):
            return "blast_radius", text[24:].strip(), "upstream"
        if lowered.startswith("explain module "):
            return "explain_module", text[15:].strip(), "upstream"
        if lowered.startswith("explain "):
            return "explain_module", text[8:].strip(), "upstream"
        return "", "", "upstream"

    def _normalize_query_tool(self, tool: str) -> str:
        normalized = tool.strip().lower()
        aliases = {
            "feeds": "trace_lineage",
            "depends_on": "trace_lineage",
            "upstream": "trace_lineage",
            "downstream": "trace_lineage",
            "what_feeds_table": "trace_lineage",
            "what_depends_on_output": "trace_lineage",
        }
        return aliases.get(normalized, normalized)

    def _normalize_query_direction(self, tool: str, direction: str) -> str:
        normalized_tool = tool.strip().lower()
        if normalized_tool in {"downstream", "depends_on", "what_depends_on_output"}:
            return "downstream"
        if normalized_tool in {"upstream", "feeds", "what_feeds_table"}:
            return "upstream"
        return "downstream" if direction.strip().lower() == "downstream" else "upstream"

    def _infer_module_from_results(self, step_results: list[dict[str, Any]]) -> str | None:
        for step in reversed(step_results):
            result = step.get("result")
            if not isinstance(result, dict):
                continue
            if "module" in result and isinstance(result.get("module"), str):
                return str(result["module"])
            for key in ("nodes", "direct_nodes", "impacted_nodes"):
                entries = result.get(key)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    source_file = str(entry.get("source_file") or "")
                    candidate = self.tools._resolve_module_path(source_file)
                    if candidate:
                        return candidate
                    node_id = str(entry.get("node") or "")
                    candidate = self.tools._resolve_module_path(node_id)
                    if candidate:
                        return candidate
            evidence_entries = result.get("evidence")
            if isinstance(evidence_entries, dict):
                evidence_entries = [evidence_entries]
            if isinstance(evidence_entries, list):
                for evidence in evidence_entries:
                    if not isinstance(evidence, dict):
                        continue
                    source_file = str(evidence.get("source_file") or "")
                    candidate = self.tools._resolve_module_path(source_file)
                    if candidate:
                        return candidate
                    node_id = str(evidence.get("node") or "")
                    candidate = self.tools._resolve_module_path(node_id)
                    if candidate:
                        return candidate
        return None

    def _merge_evidence(
        self,
        base: list[NavigatorEvidence],
        incoming: list[NavigatorEvidence],
    ) -> list[NavigatorEvidence]:
        merged: list[NavigatorEvidence] = []
        seen: set[tuple[str, int, int, str, str, str]] = set()
        for raw in [*base, *incoming]:
            normalized = self.tools._normalize_evidence(dict(raw))
            line_range = normalized.get("line_range", [1, 1])
            key = (
                str(normalized.get("source_file", "")),
                int(line_range[0]),
                int(line_range[1]),
                str(normalized.get("analysis_method", "")),
                str(normalized.get("node", "")),
                str(normalized.get("module", "")),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(normalized)
        return merged


class NavigatorAgent(NavigatorLangGraphAgent):
    """Backward-compatible alias for the LangGraph-backed Navigator."""
