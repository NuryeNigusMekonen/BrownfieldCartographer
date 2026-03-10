from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import networkx as nx

from src.agents.hydrologist import HydrologistAgent
from src.agents.navigator import NavigatorAgent
from src.graph.knowledge_graph import KnowledgeGraph


DAY_ONE_TITLES = {
    "1": "Primary Data Ingestion Path",
    "2": "Critical Output Datasets",
    "3": "Blast Radius of Critical Modules",
    "4": "Location of Business Logic",
    "5": "Recent High Velocity Areas",
}


class CartographyWorkspaceData:
    def __init__(self, cartography_dir: Path) -> None:
        self.cartography_dir = cartography_dir
        self.module_graph = KnowledgeGraph.load(cartography_dir / "module_graph.json")
        self.lineage_graph = KnowledgeGraph.load(cartography_dir / "lineage_graph.json")
        self.navigator = NavigatorAgent(self.module_graph, self.lineage_graph)
        self.hydrologist = HydrologistAgent(self.lineage_graph)
        self.semantic_index = self._load_semantic_index()
        self.trace = self._load_trace()
        self.state = self._load_state()

    def summary_payload(self) -> dict[str, Any]:
        module_graph = self.module_graph.module_import_graph()
        lineage_graph = self.lineage_graph.graph
        module_nodes = [
            (node_id, attrs) for node_id, attrs in self.module_graph.graph.nodes(data=True) if attrs.get("node_type") == "module"
        ]
        dead_code_candidates = [
            node_id for node_id, attrs in module_nodes if bool(attrs.get("is_dead_code_candidate", False))
        ]
        datasets = [
            node_id for node_id, attrs in lineage_graph.nodes(data=True) if self._infer_node_type(node_id, attrs) == "dataset"
        ]
        transforms = [
            node_id
            for node_id, attrs in lineage_graph.nodes(data=True)
            if self._infer_node_type(node_id, attrs) == "transformation"
        ]
        centrality = nx.degree_centrality(module_graph) if module_graph.number_of_nodes() > 1 else {}
        top_modules = [
            {
                "id": node_id,
                "label": Path(node_id).name or node_id,
                "centrality": round(score, 4),
            }
            for node_id, score in sorted(centrality.items(), key=lambda item: (-item[1], item[0]))[:5]
        ]

        return {
            "repo_name": self.cartography_dir.parent.name,
            "cartography_dir": str(self.cartography_dir),
            "last_analysis_timestamp": self._state_timestamp(),
            "artifact_status": "Loaded",
            "artifacts": [
                "module_graph.json",
                "lineage_graph.json",
                "semantic_index/module_purpose_index.jsonl",
                "cartography_trace.jsonl",
                "CODEBASE.md",
                "onboarding_brief.md",
            ],
            "metrics": {
                "modules": len(module_nodes),
                "functions": sum(len(attrs.get("public_functions", [])) for _, attrs in module_nodes),
                "datasets": len(datasets),
                "transformations": len(transforms),
                "lineage_edges": lineage_graph.number_of_edges(),
                "dead_code_candidates": len(dead_code_candidates),
            },
            "top_modules": top_modules,
            "critical_path_modules": top_modules,
            "agent_stories": [
                {
                    "agent": "Surveyor",
                    "problem": "navigation blindness",
                    "artifact": "module_graph.json",
                    "summary": "Maps structural architecture, central modules, and risky dead zones.",
                },
                {
                    "agent": "Hydrologist",
                    "problem": "dependency opacity",
                    "artifact": "lineage_graph.json",
                    "summary": "Reconstructs data flow, transformations, and upstream/downstream dependencies.",
                },
                {
                    "agent": "Semanticist",
                    "problem": "silent debt",
                    "artifact": "semantic_index/module_purpose_index.jsonl",
                    "summary": "Explains module purpose, inferred domains, and documentation drift signals.",
                },
                {
                    "agent": "Archivist",
                    "problem": "contextual amnesia",
                    "artifact": "CODEBASE.md + onboarding_brief.md + trace/state",
                    "summary": "Publishes living context, audit traces, and day-one onboarding artifacts.",
                },
            ],
            "quick_links": [
                {"view": "overview", "label": "Overview"},
                {"view": "surveyor", "label": "Surveyor"},
                {"view": "hydrologist", "label": "Hydrologist"},
                {"view": "semanticist", "label": "Semanticist"},
                {"view": "archivist", "label": "Archivist"},
                {"view": "navigator", "label": "Navigator"},
            ],
        }

    def module_graph_payload(self) -> dict[str, Any]:
        graph = self.module_graph.module_import_graph()
        centrality = nx.degree_centrality(graph) if graph.number_of_nodes() > 1 else {}
        nodes = []
        for node_id, attrs in graph.nodes(data=True):
            score = centrality.get(node_id, 0.0)
            degree = graph.degree(node_id)
            nodes.append(
                {
                    "id": node_id,
                    "label": Path(node_id).name or node_id,
                    "path": attrs.get("path", node_id),
                    "language": attrs.get("language", "unknown"),
                    "degree": degree,
                    "degree_centrality": round(score, 4),
                    "dead_code": bool(attrs.get("is_dead_code_candidate", False)),
                    "recent_change_velocity": int(attrs.get("change_velocity_30d", 0)),
                    "complexity_score": float(attrs.get("complexity_score", 0.0)),
                    "purpose_statement": attrs.get("purpose_statement", ""),
                    "last_modified": attrs.get("last_modified", ""),
                    "public_functions": attrs.get("public_functions", []),
                    "classes": attrs.get("classes", []),
                    "imports": attrs.get("imports", []),
                    "size": self._scale(score + (degree / max(1, graph.number_of_nodes())), lower=20, upper=52),
                    "group": "dead" if attrs.get("is_dead_code_candidate", False) else "hub" if score >= 0.12 else "module",
                    "important": score >= 0.08 or degree >= 4,
                }
            )
        edges = [
            {"from": source, "to": target, "type": attrs.get("edge_type", "IMPORTS")}
            for source, target, attrs in graph.edges(data=True)
        ]
        hubs = [node["id"] for node in sorted(nodes, key=lambda item: (-item["degree_centrality"], item["id"]))[:8]]
        return {"nodes": nodes, "edges": edges, "hubs": hubs}

    def lineage_graph_payload(self) -> dict[str, Any]:
        graph = self.lineage_graph.graph
        nodes = []
        for node_id, attrs in graph.nodes(data=True):
            node_type = self._infer_node_type(node_id, attrs)
            degree = graph.degree(node_id)
            nodes.append(
                {
                    "id": node_id,
                    "label": self._lineage_label(node_id),
                    "node_type": node_type,
                    "source_file": attrs.get("source_file", ""),
                    "line_range": list(attrs.get("line_range", (0, 0))),
                    "transformation_type": attrs.get("transformation_type", ""),
                    "storage_type": attrs.get("storage_type", ""),
                    "degree": degree,
                    "size": self._scale(degree, lower=18, upper=42),
                    "level": 1 if node_type == "dataset" else 2 if node_type == "transformation" else 3,
                    "upstream_count": len(self.lineage_graph.upstream(node_id)),
                    "downstream_count": len(self.lineage_graph.downstream(node_id)),
                    "important": degree >= 2 or node_type in {"dataset", "transformation"},
                }
            )
        edges = []
        for source, target, attrs in graph.edges(data=True):
            edges.append(
                {
                    "from": source,
                    "to": target,
                    "type": attrs.get("edge_type", ""),
                    "source_file": attrs.get("source_file", ""),
                    "line_range": list(attrs.get("line_range", (0, 0))),
                    "analysis_method": attrs.get("analysis_method", ""),
                }
            )
        return {"nodes": nodes, "edges": edges}

    def semantic_payload(self) -> dict[str, Any]:
        domain_counts: dict[str, int] = {}
        for entry in self.semantic_index:
            cluster = str(entry.get("domain_cluster", "unknown"))
            domain_counts[cluster] = domain_counts.get(cluster, 0) + 1
        domain_clusters = [
            {"cluster": cluster, "count": count}
            for cluster, count in sorted(domain_counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        return {
            "modules": self.semantic_index[:100],
            "domain_clusters": domain_clusters[:20],
            "drift_flags": self._documentation_drift_flags(),
        }

    def knowledge_payload(self) -> dict[str, Any]:
        codebase_text = self._safe_read("CODEBASE.md")
        sections = self._parse_markdown_sections(codebase_text)
        return {"markdown": codebase_text, "sections": sections}

    def onboarding_payload(self) -> dict[str, Any]:
        onboarding_text = self._safe_read("onboarding_brief.md")
        questions = self._parse_day_one_questions(onboarding_text)
        return {"markdown": onboarding_text, "questions": questions}

    def archivist_payload(self) -> dict[str, Any]:
        return {
            "codebase": self.knowledge_payload(),
            "onboarding": self.onboarding_payload(),
            "trace": self.trace,
            "state": self.state,
        }

    def semantic_search(self, query: str) -> dict[str, Any]:
        term = query.strip().lower()
        if not term:
            return {"query": query, "results": self.semantic_index[:25]}
        scored = []
        for entry in self.semantic_index:
            haystack = " ".join(
                [
                    entry.get("path", ""),
                    entry.get("purpose_statement", ""),
                    entry.get("domain_cluster", ""),
                    " ".join(entry.get("public_functions", [])),
                ]
            ).lower()
            score = haystack.count(term)
            if score > 0:
                scored.append((score, entry))
        results = [entry for _, entry in sorted(scored, key=lambda item: (-item[0], item[1].get("path", "")))]
        return {"query": query, "results": results[:25]}

    def run_query(self, raw_query: str) -> dict[str, Any]:
        tool, arg, direction = self._parse_query(raw_query)
        if not tool:
            return {
                "ok": False,
                "query": raw_query,
                "tool": "",
                "arg": "",
                "direction": "upstream",
                "result": None,
                "error": (
                    "Supported commands: explain_module <path>, find_implementation <concept>, "
                    "trace_lineage <dataset>, what_feeds_table <dataset>, "
                    "what_depends_on_output <dataset>, blast_radius <dataset>."
                ),
            }

        if tool == "find_implementation":
            result = self.navigator.find_implementation(arg)
        elif tool == "trace_lineage":
            result = self.hydrologist.get_downstream(arg) if direction == "downstream" else self.hydrologist.get_upstream(arg)
        elif tool == "upstream":
            result = self.hydrologist.get_upstream(arg)
        elif tool == "downstream":
            result = self.hydrologist.get_downstream(arg)
        elif tool == "what_feeds_table":
            result = self.hydrologist.what_feeds_table(arg)
        elif tool == "what_depends_on_output":
            result = self.hydrologist.what_depends_on_output(arg)
        elif tool == "blast_radius":
            result = self.hydrologist.blast_radius(arg)
            if isinstance(result, dict) and int(result.get("impact_count", 0)) == 0 and arg in self.module_graph.graph:
                module_impacted = self.navigator.blast_radius(arg)
                result = {
                    "target": arg,
                    "impacted_nodes": module_impacted,
                    "impact_count": len(module_impacted),
                    "evidence": [entry.get("evidence", {}) for entry in module_impacted],
                }
        else:
            result = self.navigator.explain_module(arg)
        error = result.get("error") if isinstance(result, dict) else None
        return {
            "ok": error is None,
            "query": raw_query,
            "tool": tool,
            "arg": arg,
            "direction": direction,
            "result": result,
            "error": error,
        }

    def node_details(self, graph_name: str, node_id: str) -> dict[str, Any]:
        graph = self.module_graph.module_import_graph() if graph_name == "module" else self.lineage_graph.graph
        if node_id not in graph:
            return {}
        attrs = dict(graph.nodes[node_id])
        attrs["id"] = node_id
        if graph_name == "module":
            attrs["degree"] = graph.degree(node_id)
            attrs["degree_centrality"] = nx.degree_centrality(graph).get(node_id, 0.0) if graph.number_of_nodes() > 1 else 0.0
            attrs["dead_code_flag"] = bool(attrs.get("is_dead_code_candidate", False))
            attrs["module_path"] = attrs.get("path", node_id)
        else:
            attrs["node_type"] = self._infer_node_type(node_id, attrs)
            attrs["upstream_count"] = len(self.lineage_graph.upstream(node_id))
            attrs["downstream_count"] = len(self.lineage_graph.downstream(node_id))
            attrs["display_label"] = self._lineage_label(node_id)
        return attrs

    def _load_semantic_index(self) -> list[dict[str, Any]]:
        path = self.cartography_dir / "semantic_index" / "module_purpose_index.jsonl"
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(json.loads(line))
        return rows

    def _load_trace(self) -> list[dict[str, Any]]:
        path = self.cartography_dir / "cartography_trace.jsonl"
        if not path.exists():
            return []
        events = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            events.append(json.loads(line))
        return events

    def _load_state(self) -> dict[str, Any]:
        path = self.cartography_dir / "state.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _parse_markdown_sections(self, text: str) -> list[dict[str, str]]:
        sections: list[dict[str, str]] = []
        current_title = "Overview"
        current_lines: list[str] = []
        for line in text.splitlines():
            if line.startswith("## "):
                if current_lines:
                    sections.append({"title": current_title, "body": "\n".join(current_lines).strip()})
                current_title = line[3:].strip()
                current_lines = []
                continue
            if line.startswith("# "):
                continue
            current_lines.append(line)
        if current_lines:
            sections.append({"title": current_title, "body": "\n".join(current_lines).strip()})
        return [section for section in sections if section["body"]]

    def _parse_day_one_questions(self, text: str) -> list[dict[str, Any]]:
        parts = re.split(r"(?m)^##\s+", text)
        questions: list[dict[str, Any]] = []
        for part in parts:
            part = part.strip()
            if not part or part.startswith("FDE Day-One Brief"):
                continue
            lines = part.splitlines()
            heading = lines[0].strip()
            body = "\n".join(lines[1:]).strip()
            answer, evidence = body, []
            if "Evidence:" in body:
                answer, evidence_block = body.split("Evidence:", 1)
                evidence = self._parse_evidence_block(evidence_block)
            qid_match = re.match(r"(\d+)\)\s+(.*)", heading)
            qid = qid_match.group(1) if qid_match else str(len(questions) + 1)
            title = DAY_ONE_TITLES.get(qid, qid_match.group(2).strip() if qid_match else heading)
            questions.append(
                {
                    "id": qid,
                    "title": title,
                    "answer": answer.strip(),
                    "evidence": evidence,
                }
            )
        return questions

    def _parse_evidence_block(self, evidence_block: str) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        for line in evidence_block.splitlines():
            line = line.strip()
            if not line.startswith("-"):
                continue
            payload = line[1:].strip()
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, list):
                    evidence.extend(item for item in parsed if isinstance(item, dict))
                elif isinstance(parsed, dict):
                    evidence.append(parsed)
            except json.JSONDecodeError:
                continue
        return evidence

    def _parse_query(self, raw_query: str) -> tuple[str, str, str]:
        text = raw_query.strip()
        lowered = text.lower()
        if not text:
            return "", "", "upstream"

        direct_match = re.match(
            r"^(explain_module|blast_radius|find_implementation|trace_lineage|what_feeds_table|what_depends_on_output|upstream|downstream|feeds|depends_on)\s+(.+)$",
            text,
            flags=re.IGNORECASE,
        )
        if direct_match:
            tool = self._normalize_query_tool(direct_match.group(1).lower())
            arg = direct_match.group(2).strip()
            return tool, arg, "upstream"

        if lowered.startswith("find implementation "):
            return "find_implementation", text[20:].strip(), "upstream"
        if lowered.startswith("find implementation of "):
            return "find_implementation", text[23:].strip(), "upstream"
        if lowered.startswith("trace lineage "):
            return "trace_lineage", text[14:].strip(), "upstream"
        if lowered.startswith("trace lineage of "):
            return "trace_lineage", text[17:].strip(), "upstream"
        if lowered.startswith("trace downstream "):
            return "trace_lineage", text[17:].strip(), "downstream"
        if lowered.startswith("trace downstream of "):
            return "trace_lineage", text[20:].strip(), "downstream"
        if lowered.startswith("upstream "):
            return "upstream", text[9:].strip(), "upstream"
        if lowered.startswith("downstream "):
            return "downstream", text[11:].strip(), "downstream"
        if lowered.startswith("what feeds table "):
            return "what_feeds_table", text[17:].strip(), "upstream"
        if lowered.startswith("what depends on output "):
            return "what_depends_on_output", text[23:].strip(), "downstream"
        if lowered.startswith("feeds "):
            return "what_feeds_table", text[6:].strip(), "upstream"
        if lowered.startswith("depends_on "):
            return "what_depends_on_output", text[11:].strip(), "downstream"
        if lowered.startswith("blast radius "):
            return "blast_radius", text[13:].strip(), "upstream"
        if lowered.startswith("compute blast radius of "):
            return "blast_radius", text[24:].strip(), "upstream"
        if lowered.startswith("explain module "):
            return "explain_module", text[15:].strip(), "upstream"
        if lowered.startswith("explain "):
            return "explain_module", text[8:].strip(), "upstream"

        query = parse_qs(text)
        tool = self._normalize_query_tool(query.get("tool", [""])[0])
        arg = query.get("arg", [""])[0]
        direction = query.get("direction", ["upstream"])[0]
        return tool, arg, direction

    def _normalize_query_tool(self, tool: str) -> str:
        normalized = tool.strip().lower()
        aliases = {
            "feeds": "what_feeds_table",
            "depends_on": "what_depends_on_output",
        }
        return aliases.get(normalized, normalized)

    def _documentation_drift_flags(self) -> list[dict[str, Any]]:
        flags = []
        for entry in self.semantic_index:
            purpose = str(entry.get("purpose_statement", "")).strip()
            path = str(entry.get("path", ""))
            if not purpose:
                flags.append({"path": path, "flag": "missing purpose statement", "severity": "medium"})
            elif "general application or utility logic" in purpose:
                flags.append({"path": path, "flag": "generic semantic description", "severity": "low"})
        return flags[:30]

    def _safe_read(self, filename: str) -> str:
        path = self.cartography_dir / filename
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")

    def _state_timestamp(self) -> str:
        epoch = self.state.get("analyzed_at_epoch")
        if not epoch:
            return "Unknown"
        return datetime.fromtimestamp(float(epoch), UTC).isoformat().replace("+00:00", "Z")

    def _infer_node_type(self, node_id: str, attrs: dict[str, Any]) -> str:
        node_type = str(attrs.get("node_type") or attrs.get("type") or "").strip().lower()
        if node_type:
            return node_type
        if node_id.startswith("dataset::"):
            return "dataset"
        if node_id.startswith("transform::"):
            return "transformation"
        return "unknown"

    def _lineage_label(self, node_id: str) -> str:
        if "::" not in node_id:
            return node_id
        return node_id.split("::", 1)[1]

    def _scale(self, value: float, lower: int, upper: int) -> int:
        if value <= 0:
            return lower
        return max(lower, min(upper, int(lower + (value * 100))))
