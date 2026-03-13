from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import networkx as nx

from src.agents.navigator import NavigatorLangGraphAgent
from src.graph.knowledge_graph import KnowledgeGraph
from src.repo import repository_metadata


DAY_ONE_TITLES = {
    "1": "Primary Data Ingestion Path",
    "2": "Critical Output Datasets/Endpoints",
    "3": "Blast Radius of Critical Module Failure",
    "4": "Business Logic Concentration",
    "5": "Onboarding-Relevant High-Velocity Areas",
}


class CartographyWorkspaceData:
    def __init__(self, cartography_dir: Path) -> None:
        self.cartography_dir = cartography_dir
        self.module_graph = KnowledgeGraph.load(cartography_dir / "module_graph.json")
        self.lineage_graph = KnowledgeGraph.load(cartography_dir / "lineage_graph.json")
        self.navigator = NavigatorLangGraphAgent(self.module_graph, self.lineage_graph)
        self.semantic_index = self._load_semantic_index()
        self.trace = self._load_trace()
        self.state = self._load_state()
        self.repo_metadata = self._repository_metadata()

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

        artifact_names = [
            "module_graph.json",
            "lineage_graph.json",
            "semantic_index/module_purpose_index.jsonl",
            "cartography_trace.jsonl",
            "CODEBASE.md",
            "onboarding_brief.md",
        ]
        available_artifacts = [name for name in artifact_names if (self.cartography_dir / name).exists()]
        artifact_status = "Loaded" if available_artifacts else "Missing"

        return {
            "repo_name": self.repo_metadata["repo_name"],
            "cartography_dir": str(self.cartography_dir),
            "repository": {
                "owner": self.repo_metadata["owner"],
                "repo_name": self.repo_metadata["repo_name"],
                "branch": self.repo_metadata["branch"],
                "display_name": self.repo_metadata["display_name"],
                "url": self.repo_metadata["repo_url"],
            },
            "last_analysis_timestamp": self._state_timestamp(),
            "artifact_status": artifact_status,
            "artifacts": available_artifacts,
            "artifact_count": len(available_artifacts),
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
        state = self.navigator.query(raw_query)
        result = state.get("result")
        error = state.get("error")
        return {
            "ok": error is None,
            "query": raw_query,
            "tool": str(state.get("tool", "")),
            "arg": str(state.get("arg", "")),
            "direction": str(state.get("direction", "upstream")),
            "result": result,
            "error": error,
            "evidence": state.get("evidence", []),
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
            module_path = str(attrs["module_path"])
            attrs["module_file"] = Path(module_path).name or module_path
            attrs["module_folder"] = str(Path(module_path).parent).replace("\\", "/")
            attrs["module_type"] = self._module_type(attrs)
        else:
            attrs["node_type"] = self._infer_node_type(node_id, attrs)
            attrs["upstream_count"] = len(self.lineage_graph.upstream(node_id))
            attrs["downstream_count"] = len(self.lineage_graph.downstream(node_id))
            attrs["display_label"] = self._lineage_label(node_id)
        return attrs

    def _repository_metadata(self) -> dict[str, str]:
        repository = self.state.get("repository")
        if isinstance(repository, dict):
            owner = str(repository.get("owner") or "").strip()
            repo_name = str(repository.get("repo_name") or "").strip()
            branch = str(repository.get("branch") or "").strip()
            display_name = str(repository.get("display_name") or "").strip()
            repo_url = str(repository.get("url") or "").strip()
            if owner and repo_name:
                return {
                    "owner": owner,
                    "repo_name": repo_name,
                    "branch": branch or "unknown",
                    "display_name": display_name or f"{owner}/{repo_name}",
                    "repo_url": repo_url,
                }

        repo_path = self.cartography_dir.parent
        inferred = repository_metadata(str(repo_path), repo_path)
        repo_name = inferred.get("repo_name") or repo_path.name
        owner = inferred.get("owner") or "local"
        branch = inferred.get("branch") or "unknown"
        display_name = inferred.get("display_name") or f"{owner}/{repo_name}"
        repo_url = inferred.get("repo_url") or ""
        return {
            "owner": owner,
            "repo_name": repo_name,
            "branch": branch,
            "display_name": display_name,
            "repo_url": repo_url,
        }

    def _module_type(self, attrs: dict[str, Any]) -> str:
        language = str(attrs.get("language") or "").strip()
        if language:
            return language.title()
        return "Unknown"

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
            if not part:
                continue
            lines = part.splitlines()
            if not lines:
                continue
            heading = lines[0].strip()
            normalized_heading = re.sub(r"^#+\s*", "", heading).strip().lower()
            if normalized_heading == "fde day-one brief":
                continue
            body = "\n".join(lines[1:]).strip()
            answer, evidence = body, []
            if "Evidence:" in body:
                answer, evidence_block = body.split("Evidence:", 1)
                evidence = self._parse_evidence_block(evidence_block)
            confidence = "medium"
            confidence_label = "medium"
            confidence_score: float | None = None
            confidence_factors: dict[str, float] = {}
            confidence_reason = ""
            confidence_components: dict[str, float] = {}
            cleaned_answer_lines: list[str] = []
            for line in answer.splitlines():
                stripped = line.strip()
                match = re.match(
                    r"^confidence(?:\s+level)?\s*:\s*(low|medium|high)(?:\s*\(score:\s*([0-9]*\.?[0-9]+)\s*\))?\s*$",
                    stripped,
                    flags=re.IGNORECASE,
                )
                if match:
                    confidence = match.group(1).lower()
                    confidence_label = confidence
                    if match.group(2):
                        try:
                            confidence_score = float(match.group(2))
                        except ValueError:
                            confidence_score = None
                    continue
                label_match = re.match(r"^confidence\s+label\s*:\s*(low|medium|high)\s*$", stripped, flags=re.IGNORECASE)
                if label_match:
                    confidence_label = label_match.group(1).lower()
                    confidence = confidence_label
                    continue
                score_match = re.match(r"^confidence\s+score\s*:\s*([0-9]*\.?[0-9]+)\s*$", stripped, flags=re.IGNORECASE)
                if score_match:
                    try:
                        confidence_score = float(score_match.group(1))
                    except ValueError:
                        confidence_score = None
                    continue
                factors_match = re.match(r"^confidence\s+factors\s*:\s*(.+)$", stripped, flags=re.IGNORECASE)
                if factors_match:
                    confidence_factors = self._parse_confidence_factors(factors_match.group(1))
                    confidence_components = self._factors_to_legacy_components(confidence_factors)
                    continue
                reason_match = re.match(r"^confidence\s+reason\s*:\s*(.+)$", stripped, flags=re.IGNORECASE)
                if reason_match:
                    confidence_reason = reason_match.group(1).strip()
                    continue
                components_match = re.match(r"^confidence\s+details\s*:\s*(.+)$", stripped, flags=re.IGNORECASE)
                if components_match:
                    confidence_components = self._parse_confidence_components(components_match.group(1))
                    confidence_factors = self._legacy_components_to_factors(confidence_components)
                    continue
                cleaned_answer_lines.append(line)
            answer = "\n".join(cleaned_answer_lines).strip()
            if not answer and not evidence:
                continue
            qid_match = re.match(r"(\d+)\)\s+(.*)", heading)
            qid = qid_match.group(1) if qid_match else str(len(questions) + 1)
            title = DAY_ONE_TITLES.get(qid, qid_match.group(2).strip() if qid_match else heading)
            questions.append(
                {
                    "id": qid,
                    "title": title,
                    "answer": answer.strip(),
                    "confidence": confidence,
                    "confidence_label": confidence_label,
                    "confidence_score": confidence_score,
                    "confidence_factors": confidence_factors,
                    "confidence_reason": confidence_reason,
                    "confidence_components": confidence_components,
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

    def _parse_confidence_components(self, text: str) -> dict[str, float]:
        components: dict[str, float] = {}
        for chunk in text.split(","):
            part = chunk.strip()
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            normalized = key.strip().lower()
            if normalized not in {
                "evidence_count_score",
                "evidence_diversity_score",
                "graph_coverage_score",
                "heuristic_reliability_score",
                "signal_agreement_score",
                "repo_type_fit_score",
            }:
                continue
            try:
                components[normalized] = float(value.strip())
            except ValueError:
                continue
        return components

    def _parse_confidence_factors(self, text: str) -> dict[str, float]:
        factors: dict[str, float] = {}
        for chunk in text.split(","):
            part = chunk.strip()
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            normalized = key.strip().lower()
            if normalized not in {
                "evidence_count",
                "evidence_diversity",
                "graph_coverage",
                "heuristic_reliability",
                "signal_agreement",
                "repo_type_fit",
            }:
                continue
            try:
                factors[normalized] = float(value.strip())
            except ValueError:
                continue
        return factors

    def _factors_to_legacy_components(self, factors: dict[str, float]) -> dict[str, float]:
        return {
            "evidence_count_score": float(factors.get("evidence_count", 0.0)),
            "evidence_diversity_score": float(factors.get("evidence_diversity", 0.0)),
            "graph_coverage_score": float(factors.get("graph_coverage", 0.0)),
            "heuristic_reliability_score": float(factors.get("heuristic_reliability", 0.0)),
            "signal_agreement_score": float(factors.get("signal_agreement", 0.0)),
            "repo_type_fit_score": float(factors.get("repo_type_fit", 0.0)),
        }

    def _legacy_components_to_factors(self, components: dict[str, float]) -> dict[str, float]:
        return {
            "evidence_count": float(components.get("evidence_count_score", 0.0)),
            "evidence_diversity": float(components.get("evidence_diversity_score", 0.0)),
            "graph_coverage": float(components.get("graph_coverage_score", 0.0)),
            "heuristic_reliability": float(components.get("heuristic_reliability_score", 0.0)),
            "signal_agreement": float(components.get("signal_agreement_score", 0.0)),
            "repo_type_fit": float(components.get("repo_type_fit_score", 0.0)),
        }

    def _parse_query(self, raw_query: str) -> tuple[str, str, str]:
        text = raw_query.strip()
        lowered = text.lower()
        if not text:
            return "", "", "upstream"

        direct_match = re.match(
            r"^(explain_module|blast_radius|find_implementation|trace_lineage|upstream|downstream|feeds|depends_on)\s+(.+)$",
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

        query = parse_qs(text)
        raw_tool = query.get("tool", [""])[0]
        tool = self._normalize_query_tool(raw_tool)
        arg = query.get("arg", [""])[0]
        direction = self._normalize_query_direction(raw_tool, query.get("direction", ["upstream"])[0])
        return tool, arg, direction

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
