from __future__ import annotations

import json
import os
from pathlib import Path
import re
from typing import Any

from ollama import Client
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ContextWindowBudget, DayOneAnswer, ModuleNode, TraceEvent


class SemanticistAgent:
    """
    Local semantic layer with deterministic heuristics.
    LLM calls are intentionally optional so this works in offline/sandbox environments.
    """

    def __init__(self) -> None:
        model_fast = os.getenv("CARTOGRAPHER_MODEL_FAST", "llama3.2:3b")
        model_synth = os.getenv("CARTOGRAPHER_MODEL_SYNTH", "llama3.1:8b")
        self.embed_model = os.getenv("CARTOGRAPHER_EMBED_MODEL", "nomic-embed-text")
        self.budget = ContextWindowBudget(model_fast=model_fast, model_synth=model_synth)
        self.ollama_host = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
        self.client = Client(host=self.ollama_host)
        # Local models typically have zero marginal API cost.
        self.cost_per_1k_tokens = {
            self.budget.model_fast: 0.0,
            self.budget.model_synth: 0.0,
        }

    def generate_purpose_statement(self, module: ModuleNode, file_text: str) -> str:
        llm_result = self._generate_purpose_with_llm(module.path, file_text)
        if llm_result:
            return llm_result
        signals = []
        if "sql" in file_text.lower():
            signals.append("SQL transformation or query logic")
        if "airflow" in file_text.lower() or "dag" in file_text.lower():
            signals.append("pipeline orchestration")
        if "pandas" in file_text.lower() or "spark" in file_text.lower():
            signals.append("data processing")
        if module.public_functions:
            signals.append(f"{len(module.public_functions)} public entry points")
        if not signals:
            signals.append("general application or utility logic")
        return (
            f"This module primarily handles {', '.join(signals)}. "
            f"It appears in the {module.language} layer and exposes key behaviors through code-defined interfaces."
        )

    def detect_doc_drift(self, file_text: str, purpose_statement: str) -> bool:
        doc = self._extract_module_docstring(file_text).lower()
        if not doc:
            return False
        keywords = {"sql", "dag", "pipeline", "api", "model", "transform", "ingest"}
        doc_hits = len([k for k in keywords if k in doc])
        purpose_hits = len([k for k in keywords if k in purpose_statement.lower()])
        return abs(doc_hits - purpose_hits) >= 3

    def cluster_into_domains(self, modules: list[ModuleNode], k: int = 5) -> dict[str, str]:
        if not modules:
            return {}
        texts = [m.purpose_statement or m.path for m in modules]
        # Follow challenge range where possible (k in [5,8]), while handling tiny repos.
        if len(modules) >= 5:
            k = min(8, max(5, k))
        else:
            k = len(modules)
        vectors = self._embed_texts(texts)
        if vectors is None:
            vec = TfidfVectorizer(max_features=512)
            vectors = vec.fit_transform(texts).toarray().tolist()
        model = KMeans(n_clusters=k, random_state=42, n_init="auto")
        labels = model.fit_predict(vectors)
        label_names = self._infer_domain_names(texts, labels, k)
        mapping: dict[str, str] = {}
        for module, label in zip(modules, labels):
            mapping[module.path] = label_names.get(int(label), f"domain_{int(label)}")
        return mapping

    def answer_day_one_questions(
        self,
        modules: list[ModuleNode],
        top_modules: list[str],
        sources: list[str],
        sinks: list[str],
        downstream_map: dict[str, list[str]],
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
    ) -> dict[str, DayOneAnswer]:
        llm_answers = self._synthesize_day_one_with_llm(
            modules=modules,
            top_modules=top_modules,
            sources=sources,
            sinks=sinks,
            downstream_map=downstream_map,
            module_graph=module_graph,
            lineage_graph=lineage_graph,
        )
        if llm_answers:
            return llm_answers

        # Fallback deterministic synthesis if model is unavailable.
        ingestion = ", ".join(sources[:5]) or "No obvious ingestion node detected."
        outputs = ", ".join(sinks[:5]) or "No obvious output dataset detected."
        blast = ""
        if top_modules:
            top = top_modules[0]
            affected = len(downstream_map.get(top, []))
            blast = f"If {top} fails, at least {affected} downstream nodes may be impacted."
        else:
            blast = "Critical blast radius not inferable."
        business_logic = "Concentrated in high-complexity/high-velocity modules." if modules else "Insufficient evidence."
        velocity = ", ".join([m.path for m in sorted(modules, key=lambda m: m.change_velocity_30d, reverse=True)[:5]])
        source_evidence = self._lineage_evidence_for_nodes(lineage_graph, sources[:5], limit=5)
        sink_evidence = self._lineage_evidence_for_nodes(lineage_graph, sinks[:5], limit=5)
        top_complexity = sorted(modules, key=lambda m: m.complexity_score, reverse=True)[:5]
        top_velocity = sorted(modules, key=lambda m: m.change_velocity_30d, reverse=True)[:5]
        return {
            "q1_primary_ingestion": DayOneAnswer(
                question_id="q1_primary_ingestion",
                answer=ingestion,
                evidence=source_evidence or [{"analysis_method": "lineage_graph_sources", "source_file": "", "line_range": [0, 0]}],
            ),
            "q2_critical_outputs": DayOneAnswer(
                question_id="q2_critical_outputs",
                answer=outputs,
                evidence=sink_evidence or [{"analysis_method": "lineage_graph_sinks", "source_file": "", "line_range": [0, 0]}],
            ),
            "q3_blast_radius": DayOneAnswer(
                question_id="q3_blast_radius",
                answer=blast,
                evidence=[
                    {
                        "analysis_method": "module_graph_descendants",
                        "source_file": top_modules[0] if top_modules else "",
                        "line_range": [1, 1],
                    }
                ],
            ),
            "q4_logic_concentration": DayOneAnswer(
                question_id="q4_logic_concentration",
                answer=business_logic,
                evidence=[
                    {"analysis_method": "complexity_and_velocity_signals", "source_file": m.path, "line_range": [1, 1]}
                    for m in top_complexity
                ],
            ),
            "q5_change_velocity": DayOneAnswer(
                question_id="q5_change_velocity",
                answer=velocity or "No git history available.",
                evidence=[
                    {"analysis_method": "git_log_frequency", "source_file": m.path, "line_range": [1, 1]}
                    for m in top_velocity
                ],
            ),
        }

    def run(self, repo_path: Path, modules: dict[str, ModuleNode]) -> tuple[dict[str, ModuleNode], list[TraceEvent]]:
        trace: list[TraceEvent] = []
        for path, module in modules.items():
            full = repo_path / path
            text = full.read_text(encoding="utf-8", errors="ignore") if full.exists() else ""
            purpose = self.generate_purpose_statement(module, text)
            module.purpose_statement = purpose
            if self.detect_doc_drift(text, purpose):
                module.purpose_statement += " [Documentation Drift Suspected]"
        clusters = self.cluster_into_domains(list(modules.values()))
        for path, cluster in clusters.items():
            modules[path].domain_cluster = cluster
        trace.append(
            TraceEvent(
                agent="semanticist",
                action="purpose_statements_generated",
                evidence={
                    "modules": len(modules),
                    "estimated_tokens": self.budget.estimated_tokens,
                    "spent_tokens": self.budget.spent_tokens,
                    "estimated_cost_usd": self.budget.estimated_cost_usd,
                    "spent_cost_usd": self.budget.spent_cost_usd,
                    "model_fast": self.budget.model_fast,
                    "model_synth": self.budget.model_synth,
                    "embed_model": self.embed_model,
                },
                confidence="medium",
            )
        )
        return modules, trace

    def _extract_module_docstring(self, text: str) -> str:
        text = text.strip()
        if text.startswith('"""'):
            end = text.find('"""', 3)
            if end > 3:
                return text[3:end]
        if text.startswith("'''"):
            end = text.find("'''", 3)
            if end > 3:
                return text[3:end]
        return ""

    def _generate_purpose_with_llm(self, path: str, file_text: str) -> str | None:
        snippet = file_text[:12000]
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a software architecture analyst. Write 2-3 sentences describing the module's business "
                    "function (not implementation details), using only the code provided."
                ),
            },
            {
                "role": "user",
                "content": f"Path: {path}\n\nCode:\n{snippet}",
            },
        ]
        return self._ollama_chat(
            model=self.budget.model_fast,
            messages=messages,
            task_type="bulk_summary",
            temperature=0.1,
        )

    def _synthesize_day_one_with_llm(
        self,
        modules: list[ModuleNode],
        top_modules: list[str],
        sources: list[str],
        sinks: list[str],
        downstream_map: dict[str, list[str]],
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
    ) -> dict[str, DayOneAnswer] | None:
        context = self._build_day_one_context(modules, top_modules, sources, sinks, downstream_map, module_graph, lineage_graph)
        prompt = (
            "Answer the Five FDE Day-One Questions as JSON with keys "
            "q1_primary_ingestion, q2_critical_outputs, q3_blast_radius, q4_logic_concentration, q5_change_velocity. "
            "Each value must be an object with fields: answer (string), evidence (array). "
            "Each evidence item must include source_file (string), line_range ([start,end]), and analysis_method (string). "
            "Use only provided context; include concrete file paths and line ranges whenever possible.\n\n"
            f"Context:\n{json.dumps(context, ensure_ascii=True)[:40000]}"
        )
        raw = self._ollama_chat(
            model=self.budget.model_synth,
            messages=[
                {"role": "system", "content": "You are a precise software architecture synthesis agent."},
                {"role": "user", "content": prompt},
            ],
            task_type="synthesis",
            temperature=0.1,
        )
        if not raw:
            return None
        data = self._extract_json_object(raw)
        if not isinstance(data, dict):
            return None

        out: dict[str, DayOneAnswer] = {}
        for qid in [
            "q1_primary_ingestion",
            "q2_critical_outputs",
            "q3_blast_radius",
            "q4_logic_concentration",
            "q5_change_velocity",
        ]:
            payload = data.get(qid, {})
            answer = str(payload.get("answer", "")).strip()
            evidence = payload.get("evidence", [])
            if not isinstance(evidence, list):
                evidence = []
            out[qid] = DayOneAnswer(question_id=qid, answer=answer, evidence=evidence)
        if all(v.answer for v in out.values()):
            return out
        return None

    def _build_day_one_context(
        self,
        modules: list[ModuleNode],
        top_modules: list[str],
        sources: list[str],
        sinks: list[str],
        downstream_map: dict[str, list[str]],
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
    ) -> dict[str, Any]:
        module_summary = [
            {
                "path": m.path,
                "language": m.language,
                "complexity_score": m.complexity_score,
                "change_velocity_30d": m.change_velocity_30d,
                "purpose_statement": m.purpose_statement,
                "domain_cluster": m.domain_cluster,
            }
            for m in sorted(modules, key=lambda x: x.change_velocity_30d, reverse=True)[:50]
        ]
        lineage_edges: list[dict[str, Any]] = []
        for source, target, attrs in lineage_graph.graph.edges(data=True):
            lineage_edges.append(
                {
                    "source": source,
                    "target": target,
                    "source_file": attrs.get("source_file", ""),
                    "line_range": list(attrs.get("line_range", (0, 0))),
                    "analysis_method": attrs.get("analysis_method", ""),
                }
            )
        return {
            "top_modules": top_modules,
            "sources": sources[:30],
            "sinks": sinks[:30],
            "downstream_map": {k: v[:50] for k, v in downstream_map.items()},
            "module_summary": module_summary,
            "module_graph_stats": {
                "nodes": module_graph.graph.number_of_nodes(),
                "edges": module_graph.graph.number_of_edges(),
            },
            "lineage_graph_stats": {
                "nodes": lineage_graph.graph.number_of_nodes(),
                "edges": lineage_graph.graph.number_of_edges(),
            },
            "lineage_edges": lineage_edges[:300],
        }

    def _embed_texts(self, texts: list[str]) -> list[list[float]] | None:
        if not texts:
            return None
        joined = "\n".join(texts)
        self._record_estimate(self.embed_model, joined, task_type="embedding")
        try:
            response = self.client.embed(model=self.embed_model, input=texts)
            embeddings = response.get("embeddings")
            if isinstance(embeddings, list) and embeddings and isinstance(embeddings[0], list):
                self._record_spend(self.embed_model, joined, json.dumps(embeddings)[:2000], task_type="embedding")
                return embeddings
        except Exception:
            return None
        return None

    def _infer_domain_names(self, texts: list[str], labels: Any, k: int) -> dict[int, str]:
        names: dict[int, str] = {}
        vec = TfidfVectorizer(max_features=256, stop_words="english")
        x = vec.fit_transform(texts)
        features = vec.get_feature_names_out()
        for cluster_id in range(k):
            indices = [i for i, lbl in enumerate(labels) if int(lbl) == cluster_id]
            if not indices:
                names[cluster_id] = f"domain_{cluster_id}"
                continue
            centroid = x[indices].mean(axis=0)
            dense = centroid.A1 if hasattr(centroid, "A1") else centroid.tolist()[0]
            ranked = sorted(range(len(dense)), key=lambda idx: dense[idx], reverse=True)
            keywords = [features[idx] for idx in ranked[:3] if dense[idx] > 0]
            if keywords:
                names[cluster_id] = "domain_" + "_".join(keywords)
            else:
                names[cluster_id] = f"domain_{cluster_id}"
        return names

    def _ollama_chat(
        self,
        model: str,
        messages: list[dict[str, str]],
        task_type: str,
        temperature: float = 0.1,
    ) -> str | None:
        prompt_text = "\n".join([m.get("content", "") for m in messages])
        self._record_estimate(model, prompt_text, task_type=task_type)
        try:
            response = self.client.chat(
                model=model,
                messages=messages,
                options={"temperature": temperature},
            )
            content = (
                response.get("message", {}).get("content", "").strip()
                if isinstance(response, dict)
                else ""
            )
        except Exception:
            return None
        if not content:
            return None
        self._record_spend(model, prompt_text, content, task_type=task_type)
        return content

    def _record_estimate(self, model: str, prompt_text: str, task_type: str) -> None:
        estimated_in = self._estimate_tokens(prompt_text)
        self.budget.estimated_tokens += estimated_in
        self.budget.estimated_cost_usd += (estimated_in / 1000.0) * self.cost_per_1k_tokens.get(model, 0.0)

    def _record_spend(self, model: str, prompt_text: str, completion_text: str, task_type: str) -> None:
        used = self._estimate_tokens(prompt_text) + self._estimate_tokens(completion_text)
        self.budget.spent_tokens += used
        self.budget.spent_cost_usd += (used / 1000.0) * self.cost_per_1k_tokens.get(model, 0.0)

    def _estimate_tokens(self, text: str) -> int:
        if not text:
            return 0
        return max(1, len(text) // 4)

    def _extract_json_object(self, text: str) -> dict[str, Any] | None:
        stripped = text.strip()
        try:
            parsed = json.loads(stripped)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            pass
        match = re.search(r"\{.*\}", stripped, flags=re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    def _lineage_evidence_for_nodes(
        self, lineage_graph: KnowledgeGraph, node_ids: list[str], limit: int = 5
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        node_set = set(node_ids)
        for source, target, attrs in lineage_graph.graph.edges(data=True):
            if source in node_set or target in node_set:
                line_range = list(attrs.get("line_range", (0, 0)))
                out.append(
                    {
                        "analysis_method": attrs.get("analysis_method", "lineage_graph"),
                        "source_file": attrs.get("source_file", ""),
                        "line_range": line_range if len(line_range) == 2 else [0, 0],
                    }
                )
            if len(out) >= limit:
                break
        return out
