from __future__ import annotations

import ast
from collections import Counter
import json
import math
import os
from pathlib import Path
import re
from typing import Any

import networkx as nx
from ollama import Client
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

from src.analyzers.git_history import GitVelocitySnapshot
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
        self.token_budget_limit = max(1, int(os.getenv("CARTOGRAPHER_TOKEN_BUDGET", "120000")))
        self.cost_budget_limit_usd = max(0.0, float(os.getenv("CARTOGRAPHER_COST_BUDGET_USD", "10.0")))
        self.ollama_host = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
        self.client = Client(host=self.ollama_host)
        # Local models typically have zero marginal API cost.
        self.cost_per_1k_tokens = {
            self.budget.model_fast: 0.0,
            self.budget.model_synth: 0.0,
        }
        self.model_usage_counts: Counter[str] = Counter()
        self.model_task_counts: Counter[str] = Counter()
        self._llm_available: bool | None = None

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
        return bool(self.detect_doc_drift_details(file_text, purpose_statement).get("drift_detected", False))

    def detect_doc_drift_details(self, file_text: str, purpose_statement: str) -> dict[str, Any]:
        doc = self._extract_module_docstring(file_text).strip()
        if not doc:
            return {
                "drift_detected": False,
                "severity": "none",
                "docstring_excerpt": "",
                "contradictions": [],
                "keyword_overlap": [],
                "analysis_method": "docstring_absent",
            }

        llm_details = self._detect_doc_drift_with_llm(doc, file_text, purpose_statement)
        if llm_details:
            return llm_details

        doc_tokens = self._semantic_keywords(doc)
        purpose_tokens = self._semantic_keywords(purpose_statement)
        overlap = sorted(doc_tokens & purpose_tokens)
        contradictions: list[dict[str, str]] = []

        if ("api" in doc_tokens or "http" in doc_tokens) and {"sql", "table", "pipeline"} & purpose_tokens:
            contradictions.append(
                {
                    "type": "system_boundary_mismatch",
                    "doc_claim": "Docstring frames API/service behavior.",
                    "implementation_signal": "Purpose indicates SQL/pipeline data processing behavior.",
                }
            )
        if {"read", "reader", "ingest"} & doc_tokens and {"write", "writer", "export"} & purpose_tokens:
            contradictions.append(
                {
                    "type": "io_direction_mismatch",
                    "doc_claim": "Docstring suggests ingestion/read path.",
                    "implementation_signal": "Implementation summary indicates output/write behavior.",
                }
            )
        if {"write", "writer", "export"} & doc_tokens and {"read", "reader", "ingest"} & purpose_tokens:
            contradictions.append(
                {
                    "type": "io_direction_mismatch",
                    "doc_claim": "Docstring suggests output/write behavior.",
                    "implementation_signal": "Implementation summary indicates ingestion/read behavior.",
                }
            )

        imbalance = abs(len(doc_tokens) - len(purpose_tokens))
        drift_detected = bool(contradictions) or (imbalance >= 4 and len(overlap) <= 1)
        if drift_detected and len(contradictions) >= 2:
            severity = "high"
        elif drift_detected and (contradictions or imbalance >= 6):
            severity = "medium"
        elif drift_detected:
            severity = "low"
        else:
            severity = "none"

        return {
            "drift_detected": drift_detected,
            "severity": severity,
            "docstring_excerpt": doc[:280],
            "contradictions": contradictions,
            "keyword_overlap": overlap,
            "analysis_method": "heuristic_docstring_vs_implementation",
        }

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
        git_velocity_snapshot: GitVelocitySnapshot | None = None,
    ) -> dict[str, DayOneAnswer]:
        repo_profile = self._infer_repo_profile(modules)
        module_call_centrality = self._module_call_centrality(module_graph)
        module_downstream_counts = self._module_downstream_count_map(module_graph)
        module_by_path = {m.path: m for m in modules}
        derived_sources = self._rank_primary_ingestion_nodes(lineage_graph, limit=5)
        derived_outputs = self._rank_critical_output_nodes(lineage_graph, limit=5)
        fallback_sources = self._dataset_nodes_from_ids(lineage_graph, sources)
        fallback_outputs = self._dataset_nodes_from_ids(lineage_graph, sinks)
        lineage_scan_complete = self._lineage_scan_completed(lineage_graph, modules)

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
            llm_context = self._infer_confidence_context_from_answers(
                answers=llm_answers,
                modules=modules,
                module_graph=module_graph,
                module_downstream_counts=module_downstream_counts,
                module_call_centrality=module_call_centrality,
                derived_outputs=derived_outputs,
                fallback_outputs=fallback_outputs,
                lineage_scan_complete=lineage_scan_complete,
            )
            return self._apply_day_one_confidence_model(
                answers=llm_answers,
                modules=modules,
                module_graph=module_graph,
                lineage_graph=lineage_graph,
                confidence_context=llm_context,
            )

        # Fallback deterministic synthesis if model is unavailable.
        source_nodes = self._filter_ingestion_candidates_by_evidence(
            lineage_graph,
            derived_sources or fallback_sources,
        )
        sink_nodes = derived_outputs or fallback_outputs

        source_evidence = self._lineage_evidence_for_nodes(lineage_graph, source_nodes[:5], limit=5)
        entrypoint_ingestion_used = False
        lineage_signals_weak = self._ingestion_signal_is_weak(source_nodes, source_evidence)
        if self._ingestion_signal_is_weak(source_nodes, source_evidence):
            derived_entrypoints = self._rank_ingestion_entrypoint_modules(
                modules=modules,
                module_graph=module_graph,
                call_centrality=module_call_centrality,
                repo_profile=repo_profile,
                limit=5,
            )
            if derived_entrypoints:
                if source_nodes:
                    merged_nodes = list(source_nodes)
                    for path in derived_entrypoints:
                        if path not in merged_nodes:
                            merged_nodes.append(path)
                    source_nodes = merged_nodes[:5]
                    source_evidence.extend(
                        [
                            {
                                "analysis_method": "module_entrypoint_ingestion_heuristic",
                                "source_file": path,
                                "line_range": [1, 1],
                            }
                            for path in derived_entrypoints[:5]
                        ]
                    )
                    source_evidence = source_evidence[:5]
                    entrypoint_ingestion_used = True
                else:
                    source_nodes = derived_entrypoints
                    source_evidence = [
                        {
                            "analysis_method": "module_entrypoint_ingestion_heuristic",
                            "source_file": path,
                            "line_range": [1, 1],
                        }
                        for path in derived_entrypoints[:5]
                    ]
                    entrypoint_ingestion_used = True

        ingestion = ", ".join(source_nodes[:5]) or "No obvious ingestion dataset detected."
        outputs = ", ".join(sink_nodes[:5]) or "No obvious output dataset detected."

        blast_module = self._select_blast_radius_module(
            top_modules,
            modules,
            downstream_map,
            module_graph,
            module_downstream_counts,
        )
        blast_downstream_count: int | None = None
        blast_zero_coverage_gap = False
        if blast_module:
            affected = int(module_downstream_counts.get(blast_module, len(downstream_map.get(blast_module, []))))
            blast_downstream_count = affected
            target_module = module_by_path.get(blast_module)
            if self._is_deprecated_module(target_module):
                if affected == 0:
                    blast = (
                        f"{blast_module} appears to be a deprecated guard module. "
                        "Static import graph shows 0 downstream nodes, so operational blast radius should "
                        "be assessed on the replacement module path."
                    )
                else:
                    blast = (
                        f"{blast_module} appears to be a deprecated guard module; "
                        f"at least {affected} downstream nodes may still be impacted by legacy imports."
                    )
            elif affected == 0:
                blast_zero_coverage_gap = self._blast_zero_is_coverage_gap(
                    blast_module,
                    affected,
                    module_graph,
                    module_downstream_counts,
                )
                blast = (
                    f"If {blast_module} fails, static import graph currently shows 0 downstream nodes. "
                    "This may indicate limited graph coverage or an entrypoint mismatch."
                )
            else:
                blast = f"If {blast_module} fails, at least {affected} downstream nodes may be impacted."
        else:
            blast = "Critical blast radius not inferable."

        logic_modules = [
            m
            for m in modules
            if self._is_logic_module_path(m.path)
            and not self._is_excluded_business_logic_path(m.path)
            and not self._is_support_module_path(m.path.lower())
        ]
        scored_logic = sorted(
            logic_modules,
            key=lambda m: self._business_logic_score(
                module=m,
                call_centrality=module_call_centrality.get(m.path, 0),
            ),
            reverse=True,
        )
        focus_modules = scored_logic[:5]
        if focus_modules:
            business_logic = ", ".join(m.path for m in focus_modules)
        elif modules:
            business_logic = "Concentrated in modules with detectable complexity/velocity signals."
        else:
            business_logic = "Insufficient evidence."
        business_logic_centrality_strong = self._business_logic_has_strong_signal(
            [module.path for module in focus_modules],
            module_by_path,
            module_call_centrality,
        )

        raw_git_velocity = self._git_velocity_rows(modules=modules, snapshot=git_velocity_snapshot)
        git_status = raw_git_velocity["status"]
        git_note = raw_git_velocity["note"]
        git_window_days = int(raw_git_velocity["time_window_days"])
        raw_rows = raw_git_velocity["rows"]
        top_velocity_rows = self._rank_onboarding_velocity_rows(
            rows=raw_rows,
            module_by_path=module_by_path,
            module_call_centrality=module_call_centrality,
            repo_profile=repo_profile,
            limit=5,
        )
        raw_active_files = len([row for row in raw_rows if int(row.get("commit_count", 0)) > 0])
        raw_total_changes = sum(int(row.get("commit_count", 0)) for row in raw_rows)
        velocity_listing = ", ".join(str(row.get("path", "")).strip() for row in top_velocity_rows if str(row.get("path", "")).strip())
        low_velocity_signal = (
            git_status in {"shallow", "missing", "unavailable"}
            or raw_active_files <= 1
            or raw_total_changes <= max(3, raw_active_files)
        )
        if not velocity_listing:
            if git_status == "missing":
                velocity = "No git history available because repository metadata is missing."
            elif git_status == "unavailable":
                velocity = "No git history available because git log could not be executed."
            else:
                velocity = f"No onboarding-relevant high-velocity files found in the last {git_window_days} days."
        elif git_status == "shallow":
            velocity = (
                f"Onboarding-relevant high-velocity areas from git history ({git_window_days}d): {velocity_listing}. "
                "Velocity is based on shallow clone history."
            )
        else:
            velocity = (
                f"Onboarding-relevant high-velocity areas from git history ({git_window_days}d): {velocity_listing}."
            )
        q5_evidence = [
            {
                "analysis_method": "git_log_frequency",
                "source_file": str(row.get("path", "")).strip(),
                "line_range": [1, 1],
                "commit_count": int(row.get("commit_count", 0)),
                "time_window_days": git_window_days,
                "last_commit_timestamp": str(row.get("last_commit_timestamp", "")),
                "history_status": git_status,
            }
            for row in top_velocity_rows
            if str(row.get("path", "")).strip()
        ]
        if not q5_evidence:
            q5_evidence = [
                {
                    "analysis_method": "git_log_frequency",
                    "source_file": "",
                    "line_range": [0, 0],
                    "commit_count": 0,
                    "time_window_days": git_window_days,
                    "last_commit_timestamp": "",
                    "history_status": git_status,
                }
            ]

        sink_evidence = self._lineage_evidence_for_nodes(lineage_graph, sink_nodes[:5], limit=5)
        answers = {
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
                        "source_file": blast_module or "",
                        "line_range": [1, 1],
                    }
                ],
            ),
            "q4_logic_concentration": DayOneAnswer(
                question_id="q4_logic_concentration",
                answer=business_logic,
                evidence=[
                    {"analysis_method": "complexity_and_velocity_signals", "source_file": m.path, "line_range": [1, 1]}
                    for m in focus_modules
                ],
            ),
            "q5_change_velocity": DayOneAnswer(
                question_id="q5_change_velocity",
                answer=velocity or "No git history available.",
                evidence=q5_evidence,
            ),
        }
        confidence_context = {
            "q1_primary_ingestion": {
                "entrypoint_ingestion_used": entrypoint_ingestion_used,
                "lineage_signals_weak": lineage_signals_weak,
            },
            "q2_critical_outputs": {
                "no_lineage_sinks_detected": not sink_nodes,
                "lineage_scan_complete": lineage_scan_complete,
            },
            "q3_blast_radius": {
                "blast_module": blast_module,
                "blast_downstream_count": blast_downstream_count,
                "blast_zero_coverage_gap": blast_zero_coverage_gap,
            },
            "q4_logic_concentration": {
                "focus_modules": [m.path for m in focus_modules],
                "business_logic_centrality_strong": business_logic_centrality_strong,
            },
            "q5_change_velocity": {
                "velocity_git_only": True,
                "velocity_low_signal": low_velocity_signal,
                "velocity_scope_count": len(raw_rows),
                "velocity_active_files": raw_active_files,
                "velocity_history_status": git_status,
                "velocity_history_note": git_note,
                "velocity_time_window_days": git_window_days,
            },
        }
        return self._apply_day_one_confidence_model(
            answers=answers,
            modules=modules,
            module_graph=module_graph,
            lineage_graph=lineage_graph,
            confidence_context=confidence_context,
        )

    def run(self, repo_path: Path, modules: dict[str, ModuleNode]) -> tuple[dict[str, ModuleNode], list[TraceEvent]]:
        trace: list[TraceEvent] = []
        drift_count = 0
        drift_severity_counts: Counter[str] = Counter()
        for path, module in modules.items():
            full = repo_path / path
            text = full.read_text(encoding="utf-8", errors="ignore") if full.exists() else ""
            purpose = self.generate_purpose_statement(module, text)
            module.is_deprecated_guard = self._is_deprecation_guard(text)
            if module.is_deprecated_guard and "deprecated guard module" not in purpose.lower():
                purpose = (
                    f"{purpose} This file appears to be a deprecated guard module "
                    "that redirects callers to a replacement path."
                )
            module.purpose_statement = purpose
            drift_details = self.detect_doc_drift_details(text, purpose)
            module.doc_drift = drift_details
            severity = str(drift_details.get("severity", "none")).strip().lower() or "none"
            drift_severity_counts[severity] += 1
            if drift_details.get("drift_detected"):
                drift_count += 1
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
                    "token_budget_limit": self.token_budget_limit,
                    "cost_budget_limit_usd": self.cost_budget_limit_usd,
                    "model_fast": self.budget.model_fast,
                    "model_synth": self.budget.model_synth,
                    "embed_model": self.embed_model,
                    "model_usage_counts": dict(sorted(self.model_usage_counts.items())),
                    "model_task_counts": dict(sorted(self.model_task_counts.items())),
                    "doc_drift_modules": drift_count,
                    "doc_drift_severity_counts": dict(sorted(drift_severity_counts.items())),
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

    def _implementation_excerpt(self, path: str, file_text: str, max_chars: int = 12000) -> str:
        suffix = Path(path).suffix.lower()
        text = file_text or ""
        if suffix == ".py":
            stripped = self._python_without_docstrings(text)
            return stripped[:max_chars]
        # Remove obvious comment-only lines for other languages while retaining executable content.
        lines = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(("#", "--", "//")):
                continue
            lines.append(raw_line)
        return "\n".join(lines)[:max_chars]

    def _python_without_docstrings(self, text: str) -> str:
        try:
            tree = ast.parse(text)
        except Exception:
            return text
        skip_ranges: list[tuple[int, int]] = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            body = getattr(node, "body", [])
            if not body:
                continue
            first = body[0]
            if isinstance(first, ast.Expr) and isinstance(getattr(first, "value", None), ast.Constant):
                value = first.value.value
                if isinstance(value, str):
                    start = int(getattr(first, "lineno", 0) or 0)
                    end = int(getattr(first, "end_lineno", start) or start)
                    if start > 0 and end >= start:
                        skip_ranges.append((start, end))
        if not skip_ranges:
            return text
        lines = text.splitlines()
        keep: list[str] = []
        for idx, line in enumerate(lines, start=1):
            if any(start <= idx <= end for start, end in skip_ranges):
                continue
            keep.append(line)
        return "\n".join(keep)

    def _semantic_keywords(self, text: str) -> set[str]:
        normalized = re.findall(r"[a-zA-Z_]+", str(text or "").lower())
        stop_words = {
            "the",
            "this",
            "that",
            "with",
            "from",
            "into",
            "using",
            "module",
            "file",
            "function",
            "class",
            "and",
            "for",
            "data",
        }
        return {token for token in normalized if len(token) >= 3 and token not in stop_words}

    def _detect_doc_drift_with_llm(
        self,
        docstring_text: str,
        file_text: str,
        purpose_statement: str,
    ) -> dict[str, Any] | None:
        implementation = self._implementation_excerpt("module.py", file_text, max_chars=6000)
        if not implementation.strip():
            return None
        prompt = (
            "Compare the module docstring against implementation behavior and purpose summary. "
            "Return strict JSON with keys: drift_detected (bool), severity (none|low|medium|high), "
            "contradictions (array of objects with keys type, doc_claim, implementation_signal), keyword_overlap (array of strings). "
            "Judge implementation from executable code behavior, not comments/docstrings."
        )
        raw = self._ollama_chat(
            model=self._select_model_for_task("bulk_summary"),
            messages=[
                {"role": "system", "content": "You are a strict JSON-only documentation drift analyzer."},
                {
                    "role": "user",
                    "content": (
                        f"{prompt}\n\nDocstring:\n{docstring_text[:1500]}\n\n"
                        f"Purpose summary:\n{purpose_statement[:1200]}\n\n"
                        f"Implementation excerpt:\n{implementation}"
                    ),
                },
            ],
            task_type="doc_drift",
            temperature=0.0,
        )
        if not raw:
            return None
        payload = self._extract_json_object(raw)
        if not isinstance(payload, dict):
            return None

        severity = str(payload.get("severity", "none")).strip().lower()
        if severity not in {"none", "low", "medium", "high"}:
            severity = "none"
        drift_detected = bool(payload.get("drift_detected", False))
        contradictions = payload.get("contradictions", [])
        if not isinstance(contradictions, list):
            contradictions = []
        normalized_contradictions: list[dict[str, str]] = []
        for item in contradictions[:5]:
            if not isinstance(item, dict):
                continue
            normalized_contradictions.append(
                {
                    "type": str(item.get("type", "")).strip(),
                    "doc_claim": str(item.get("doc_claim", "")).strip(),
                    "implementation_signal": str(item.get("implementation_signal", "")).strip(),
                }
            )
        keyword_overlap = payload.get("keyword_overlap", [])
        if isinstance(keyword_overlap, list):
            normalized_overlap = [str(item).strip() for item in keyword_overlap if str(item).strip()][:8]
        else:
            normalized_overlap = []
        return {
            "drift_detected": drift_detected,
            "severity": severity,
            "docstring_excerpt": docstring_text[:280],
            "contradictions": normalized_contradictions,
            "keyword_overlap": normalized_overlap,
            "analysis_method": "llm_docstring_vs_implementation",
        }

    def _generate_purpose_with_llm(self, path: str, file_text: str) -> str | None:
        snippet = self._implementation_excerpt(path, file_text, max_chars=12000)
        if not snippet.strip():
            snippet = file_text[:12000]
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a software architecture analyst. Write 2-3 sentences describing the module's business "
                    "function (not implementation details), using only executable implementation signals from code. "
                    "Ignore docstrings, comments, README-like prose, and TODO text."
                ),
            },
            {
                "role": "user",
                "content": f"Path: {path}\n\nImplementation excerpt:\n{snippet}",
            },
        ]
        return self._ollama_chat(
            model=self._select_model_for_task("bulk_summary"),
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
            "Each value must be an object with fields: answer (string), confidence (low|medium|high), evidence (array). "
            "Each evidence item must include source_file (string), line_range ([start,end]), and analysis_method (string). "
            "Do not treat SQL operational statements (INSTALL, LOAD, CALL, CHECKPOINT, VACUUM, PRAGMA, SET, USE) as datasets. "
            "Exclude tests/, test_*, migrations/, alembic/, and fixtures/ paths from business logic concentration and blast radius prioritization. "
            "If SQL lineage ingestion signals are weak, infer likely ingestion entrypoints from connectors/plugins/API clients/orchestration modules. "
            "De-prioritize utility shims (utils/helpers/__init__) when selecting blast-radius critical modules; prioritize CLI/orchestration/job/plugin execution modules. "
            "Adapt wording to repository type: SQL-heavy repos should read as warehouse/transformation systems; Python-heavy repos as platform/orchestration systems. "
            "If blast radius is zero, explicitly note uncertainty and possible graph coverage gaps. "
            "If a module is a deprecation shim/guard, mention that and avoid implying zero operational risk. "
            "Use only provided context; include concrete file paths and line ranges whenever possible.\n\n"
            f"Context:\n{json.dumps(context, ensure_ascii=True)[:40000]}"
        )
        raw = self._ollama_chat(
            model=self._select_model_for_task("synthesis"),
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
            confidence = self._normalize_confidence(payload.get("confidence", "medium"))
            evidence = payload.get("evidence", [])
            if not isinstance(evidence, list):
                evidence = []
            evidence = self._normalize_day_one_evidence(
                question_id=qid,
                evidence=evidence,
                answer_text=answer,
                top_modules=top_modules,
                modules=modules,
                module_graph=module_graph,
                lineage_graph=lineage_graph,
            )
            out[qid] = DayOneAnswer(question_id=qid, answer=answer, evidence=evidence, confidence=confidence)
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
                "pagerank_score": m.pagerank_score,
                "purpose_statement": m.purpose_statement,
                "domain_cluster": m.domain_cluster,
                "excluded_from_logic": self._is_excluded_business_logic_path(m.path),
            }
            for m in sorted(modules, key=lambda x: x.change_velocity_30d, reverse=True)[:50]
        ]
        call_centrality = self._module_call_centrality(module_graph)
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
            "repo_profile": self._infer_repo_profile(modules),
            "top_modules": top_modules,
            "sources": sources[:30],
            "sinks": sinks[:30],
            "downstream_map": {k: v[:50] for k, v in downstream_map.items()},
            "module_call_centrality": dict(sorted(call_centrality.items(), key=lambda kv: kv[1], reverse=True)[:50]),
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

    def _normalize_day_one_evidence(
        self,
        question_id: str,
        evidence: list[dict[str, Any]],
        answer_text: str,
        top_modules: list[str],
        modules: list[ModuleNode],
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in evidence[:12]:
            if not isinstance(item, dict):
                continue
            source_file = str(item.get("source_file", "")).strip()
            line_range_raw = item.get("line_range", [0, 0])
            line_range = [0, 0]
            if isinstance(line_range_raw, tuple):
                line_range_raw = list(line_range_raw)
            if isinstance(line_range_raw, list) and len(line_range_raw) == 2:
                try:
                    start = max(0, int(line_range_raw[0]))
                    end = max(start, int(line_range_raw[1]))
                    line_range = [start, end]
                except (TypeError, ValueError):
                    line_range = [0, 0]
            normalized.append(
                {
                    "analysis_method": str(item.get("analysis_method", "")).strip() or "llm_synthesis",
                    "source_file": source_file,
                    "line_range": line_range,
                }
            )

        has_concrete_citation = any(
            str(item.get("source_file", "")).strip() and item.get("line_range", [0, 0]) != [0, 0]
            for item in normalized
        )
        if has_concrete_citation:
            return normalized

        seeded = self._seed_evidence_for_question(
            question_id=question_id,
            answer_text=answer_text,
            top_modules=top_modules,
            modules=modules,
            module_graph=module_graph,
            lineage_graph=lineage_graph,
        )
        if seeded:
            normalized.append(seeded)
        return normalized

    def _seed_evidence_for_question(
        self,
        question_id: str,
        answer_text: str,
        top_modules: list[str],
        modules: list[ModuleNode],
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
    ) -> dict[str, Any] | None:
        if question_id in {"q1_primary_ingestion", "q2_critical_outputs"}:
            for _, _, attrs in lineage_graph.graph.edges(data=True):
                source_file = str(attrs.get("source_file", "")).strip()
                if not source_file:
                    continue
                line_range = attrs.get("line_range", [0, 0])
                if isinstance(line_range, tuple):
                    line_range = list(line_range)
                if not isinstance(line_range, list) or len(line_range) != 2:
                    line_range = [0, 0]
                return {
                    "analysis_method": str(attrs.get("analysis_method", "")).strip() or "lineage_graph",
                    "source_file": source_file,
                    "line_range": [int(line_range[0]), int(line_range[1])],
                }

        if question_id in {"q3_blast_radius", "q4_logic_concentration"}:
            parsed_paths = self._extract_paths_from_text(answer_text)
            candidate_paths = parsed_paths + list(top_modules) + [module.path for module in modules]
            for path in candidate_paths:
                if path in module_graph.graph.nodes:
                    return {
                        "analysis_method": "module_graph_descendants"
                        if question_id == "q3_blast_radius"
                        else "complexity_and_velocity_signals",
                        "source_file": path,
                        "line_range": [1, 1],
                    }

        if question_id == "q5_change_velocity":
            for module in sorted(modules, key=lambda m: int(m.change_velocity_30d), reverse=True):
                if int(module.change_velocity_30d) <= 0:
                    continue
                return {
                    "analysis_method": "git_log_frequency",
                    "source_file": module.path,
                    "line_range": [1, 1],
                }
        return None

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
                cluster_terms = self._semantic_keywords(" ".join(texts[i] for i in indices))
                ranked_terms = sorted(term for term in cluster_terms if term not in {"module", "logic", "layer"})
                if ranked_terms:
                    names[cluster_id] = "domain_" + "_".join(ranked_terms[:3])
                else:
                    names[cluster_id] = f"domain_{cluster_id}"
        return names

    def _select_model_for_task(self, task_type: str) -> str:
        task = str(task_type or "").strip().lower()
        if task in {"synthesis", "cross_module_synthesis"}:
            if self._budget_exceeded():
                return self.budget.model_fast
            return self.budget.model_synth
        return self.budget.model_fast

    def _budget_exceeded(self) -> bool:
        if self.budget.spent_tokens >= self.token_budget_limit:
            return True
        if self.budget.spent_cost_usd >= self.cost_budget_limit_usd:
            return True
        return False

    def _ollama_chat(
        self,
        model: str,
        messages: list[dict[str, str]],
        task_type: str,
        temperature: float = 0.1,
    ) -> str | None:
        if self._llm_available is False:
            return None
        prompt_text = "\n".join([m.get("content", "") for m in messages])
        self.model_usage_counts[str(model)] += 1
        self.model_task_counts[f"{task_type}:{model}"] += 1
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
            self._llm_available = False
            return None
        if not content:
            return None
        self._llm_available = True
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

    def _dataset_nodes_from_ids(self, lineage_graph: KnowledgeGraph, node_ids: list[str]) -> list[str]:
        datasets: list[str] = []
        for node_id in node_ids:
            attrs = lineage_graph.graph.nodes.get(node_id, {})
            if attrs.get("node_type") != "dataset":
                continue
            if self._dataset_is_noise(node_id, attrs):
                continue
            datasets.append(str(node_id))
        return datasets

    def _rank_primary_ingestion_nodes(self, lineage_graph: KnowledgeGraph, limit: int) -> list[str]:
        keywords = (
            "raw",
            "source",
            "ingest",
            "landing",
            "bronze",
            "staging",
            "s3",
            "gcs",
            "csv",
            "json",
            "parquet",
            "api",
            "stream",
            "event",
            "log",
        )
        scored: list[tuple[int, int, int, str]] = []
        for node_id, attrs in lineage_graph.graph.nodes(data=True):
            if attrs.get("node_type") != "dataset":
                continue
            if self._dataset_is_noise(node_id, attrs):
                continue
            in_degree = lineage_graph.graph.in_degree(node_id)
            out_degree = lineage_graph.graph.out_degree(node_id)
            if in_degree != 0 or out_degree == 0:
                continue
            label = str(attrs.get("name") or node_id.replace("dataset::", "", 1)).lower()
            keyword_score = sum(1 for kw in keywords if kw in label)
            downstream_score = len(lineage_graph.downstream(node_id))
            score = (keyword_score * 5) + min(out_degree, 5) + min(downstream_score, 30)
            scored.append((score, out_degree, downstream_score, str(node_id)))
        scored.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
        return [item[3] for item in scored[:limit]]

    def _rank_critical_output_nodes(self, lineage_graph: KnowledgeGraph, limit: int) -> list[str]:
        keywords = ("report", "dashboard", "mart", "fact", "dim", "dataset", "export", "serving")
        scored: list[tuple[int, int, int, str]] = []
        for node_id, attrs in lineage_graph.graph.nodes(data=True):
            if attrs.get("node_type") != "dataset":
                continue
            if self._dataset_is_noise(node_id, attrs):
                continue
            in_degree = lineage_graph.graph.in_degree(node_id)
            out_degree = lineage_graph.graph.out_degree(node_id)
            if out_degree != 0 or in_degree == 0:
                continue
            label = str(attrs.get("name") or node_id.replace("dataset::", "", 1)).lower()
            keyword_score = sum(1 for kw in keywords if kw in label)
            upstream_score = len(lineage_graph.upstream(node_id))
            score = (keyword_score * 4) + min(in_degree, 10) + min(upstream_score, 30)
            scored.append((score, in_degree, upstream_score, str(node_id)))
        scored.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
        return [item[3] for item in scored[:limit]]

    def _dataset_is_noise(self, node_id: str, attrs: dict[str, Any]) -> bool:
        raw = str(attrs.get("name") or node_id.replace("dataset::", "", 1)).strip()
        lowered = raw.lower()
        if not lowered:
            return True
        if "dynamic reference, cannot resolve" in lowered:
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
        if lowered.startswith(operational_prefixes):
            return True
        return False

    def _ingestion_signal_is_weak(
        self,
        source_nodes: list[str],
        source_evidence: list[dict[str, Any]],
    ) -> bool:
        if not source_nodes:
            return True
        quality_nodes = [node for node in source_nodes if not self._is_low_quality_ingestion_label(node)]
        if len(quality_nodes) < 2:
            return True
        strong_methods = {"sqlglot", "tree_sitter_python+sqlglot", "lineage_graph"}
        method_hits = sum(1 for item in source_evidence if str(item.get("analysis_method", "")) in strong_methods)
        return method_hits < 2

    def _filter_ingestion_candidates_by_evidence(
        self,
        lineage_graph: KnowledgeGraph,
        candidates: list[str],
    ) -> list[str]:
        filtered: list[str] = []
        for node_id in candidates:
            if self._is_low_quality_ingestion_label(node_id):
                continue
            if self._dataset_is_noise(node_id, lineage_graph.graph.nodes.get(node_id, {})):
                continue
            if not self._dataset_has_non_excluded_evidence(lineage_graph, node_id):
                continue
            filtered.append(node_id)
        return filtered

    def _is_low_quality_ingestion_label(self, node_id: str) -> bool:
        lowered = str(node_id).replace("dataset::", "", 1).strip().lower()
        if not lowered:
            return True
        generic = {
            "test",
            "tests",
            "job",
            "jobs",
            "run",
            "tmp",
            "temp",
            "embed_tokens",
            "token",
            "tokens",
        }
        if lowered in generic:
            return True
        if lowered.startswith("test_") or lowered.startswith("tmp_"):
            return True
        if len(lowered) <= 3:
            return True
        return False

    def _dataset_has_non_excluded_evidence(self, lineage_graph: KnowledgeGraph, node_id: str) -> bool:
        has_any = False
        for source, target, attrs in lineage_graph.graph.edges(data=True):
            if source != node_id and target != node_id:
                continue
            has_any = True
            source_file = str(attrs.get("source_file", "")).strip()
            if source_file and not self._is_excluded_business_logic_path(source_file):
                return True
        return not has_any

    def _rank_ingestion_entrypoint_modules(
        self,
        modules: list[ModuleNode],
        module_graph: KnowledgeGraph,
        call_centrality: dict[str, int],
        repo_profile: str,
        limit: int = 5,
    ) -> list[str]:
        profile_keywords: tuple[str, ...]
        if repo_profile == "sql_heavy":
            profile_keywords = ("dbt", "elt", "extract", "load", "source", "staging")
        elif repo_profile == "python_heavy":
            profile_keywords = ("plugin", "connector", "pipeline", "orchestr", "job", "cli")
        else:
            profile_keywords = ()

        base_keywords = (
            "plugin",
            "connector",
            "pipeline",
            "ingest",
            "extract",
            "source",
            "loader",
            "tap",
            "api",
            "client",
            "orchestr",
            "dagster",
            "airflow",
            "entrypoint",
            "job",
            "executor",
            "invoke",
        )
        keywords = tuple(dict.fromkeys(base_keywords + profile_keywords))
        scored: list[tuple[float, str]] = []
        import_centrality = self._module_import_centrality(module_graph)
        for module in modules:
            path = module.path
            lowered = path.lower()
            if self._is_excluded_business_logic_path(path):
                continue
            if self._is_support_module_path(lowered):
                continue
            if not lowered.endswith((".py", ".js", ".ts", ".java", ".scala")):
                continue
            keyword_score = sum(1 for kw in keywords if kw in lowered)
            purpose_text = str(module.purpose_statement or "").lower()
            keyword_score += sum(1 for kw in keywords if kw in purpose_text)
            if keyword_score == 0:
                continue
            execution_bonus = float(self._execution_priority(lowered) * 3.0)
            score = (
                (keyword_score * 3.0)
                + (float(import_centrality.get(path, 0.0)) * 80.0)
                + (min(call_centrality.get(path, 0), 80) * 0.5)
                + (float(module.change_velocity_30d) * 0.25)
                + execution_bonus
            )
            if score <= 0:
                continue
            scored.append((score, path))
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [path for _, path in scored[:limit]]

    def _module_import_centrality(self, module_graph: KnowledgeGraph) -> dict[str, float]:
        centrality: dict[str, float] = {}
        for node_id, attrs in module_graph.graph.nodes(data=True):
            if attrs.get("node_type") != "module":
                continue
            try:
                centrality[str(node_id)] = float(attrs.get("pagerank_score", 0.0))
            except Exception:
                centrality[str(node_id)] = 0.0
        return centrality

    def _module_call_centrality(self, module_graph: KnowledgeGraph) -> dict[str, int]:
        counts: dict[str, int] = {}
        for source, target, attrs in module_graph.graph.edges(data=True):
            if str(attrs.get("edge_type", "")) != "CALLS":
                continue
            src_mod = str(source).split("::", 1)[0]
            dst_mod = str(target).split("::", 1)[0]
            counts[src_mod] = counts.get(src_mod, 0) + 1
            counts[dst_mod] = counts.get(dst_mod, 0) + 1
        return counts

    def _is_excluded_business_logic_path(self, path: str) -> bool:
        lowered = path.lower().strip()
        if not lowered:
            return False
        parts = [part for part in lowered.split("/") if part]
        filename = Path(lowered).name
        excluded_markers = {"tests", "migrations", "alembic", "fixtures"}
        if any(part in excluded_markers for part in parts):
            return True
        if filename.startswith("test_"):
            return True
        return False

    def _is_support_module_path(self, lowered_path: str) -> bool:
        return any(token in lowered_path for token in ("utils", "helpers", "__init__.py"))

    def _is_execution_module_path(self, lowered_path: str) -> bool:
        return self._execution_priority(lowered_path) > 0

    def _execution_priority(self, lowered_path: str) -> int:
        strong_markers = (
            "cli",
            "main.py",
            "pipeline",
            "orchestr",
            "job",
            "worker",
            "runner",
            "invoke",
            "entrypoint",
        )
        medium_markers = (
            "plugin",
            "dagster",
            "airflow",
            "executor",
        )
        if any(marker in lowered_path for marker in strong_markers):
            return 2
        if any(marker in lowered_path for marker in medium_markers):
            return 1
        return 0

    def _is_abstraction_module_path(self, lowered_path: str) -> bool:
        filename = Path(lowered_path).name
        return filename in {"base.py", "types.py", "constants.py"}

    def _infer_repo_profile(self, modules: list[ModuleNode]) -> str:
        if not modules:
            return "mixed"
        sql_weight = 0.0
        py_weight = 0.0
        for module in modules:
            ext = Path(module.path).suffix.lower()
            weight = max(1.0, float(module.loc or 0))
            if ext == ".sql":
                sql_weight += weight
            if ext == ".py":
                py_weight += weight
        total = sql_weight + py_weight
        if total <= 0:
            return "mixed"
        sql_ratio = sql_weight / total
        py_ratio = py_weight / total
        if sql_ratio >= 0.6:
            return "sql_heavy"
        if py_ratio >= 0.6:
            return "python_heavy"
        return "mixed"

    def _select_blast_radius_module(
        self,
        top_modules: list[str],
        modules: list[ModuleNode],
        downstream_map: dict[str, list[str]],
        module_graph: KnowledgeGraph,
        module_downstream_counts: dict[str, int] | None = None,
    ) -> str:
        if not modules and not top_modules:
            return ""
        module_lookup = {module.path: module for module in modules}
        call_centrality = self._module_call_centrality(module_graph)
        import_centrality = self._module_import_centrality(module_graph)
        downstream_counts = module_downstream_counts or self._module_downstream_count_map(module_graph)
        candidate_paths = list(top_modules)
        if not candidate_paths:
            candidate_paths = [module.path for module in modules]
        for module in sorted(modules, key=lambda item: float(item.pagerank_score), reverse=True)[:20]:
            if module.path not in candidate_paths:
                candidate_paths.append(module.path)
        strong_candidates = [
            path
            for path in candidate_paths
            if not self._is_support_module_path(path.lower())
            and not self._is_excluded_business_logic_path(path)
            and self._execution_priority(path.lower()) >= 1
        ]
        if strong_candidates:
            max_priority = max(self._execution_priority(path.lower()) for path in strong_candidates)
            candidate_paths = [path for path in strong_candidates if self._execution_priority(path.lower()) == max_priority]

        def score(path: str) -> float:
            module = module_lookup.get(path)
            downstream_count = int(downstream_counts.get(path, len(downstream_map.get(path, []))))
            lowered = path.lower()
            complexity = float(module.complexity_score) if module else 0.0
            velocity = int(module.change_velocity_30d) if module else 0
            support_penalty = 8.0 if self._is_support_module_path(lowered) else 0.0
            excluded_penalty = 12.0 if self._is_excluded_business_logic_path(path) else 0.0
            abstraction_penalty = 14.0 if self._is_abstraction_module_path(lowered) else 0.0
            execution_bonus = float(self._execution_priority(lowered) * 8.0)
            return (
                (math.log1p(max(downstream_count, 0)) * 35.0)
                + (complexity * 0.5)
                + (velocity * 0.3)
                + (float(import_centrality.get(path, 0.0)) * 180.0)
                + (min(call_centrality.get(path, 0), 80) * 0.6)
                + execution_bonus
                - support_penalty
                - abstraction_penalty
                - excluded_penalty
            )

        ranked = sorted(candidate_paths, key=lambda path: (score(path), path), reverse=True)
        return ranked[0] if ranked else candidate_paths[0]

    def _module_downstream_count_map(self, module_graph: KnowledgeGraph) -> dict[str, int]:
        subgraph = module_graph.module_import_graph()
        counts: dict[str, int] = {}
        for node_id in subgraph.nodes:
            try:
                counts[str(node_id)] = len(nx.descendants(subgraph, node_id))
            except Exception:
                counts[str(node_id)] = 0
        return counts

    def _is_logic_module_path(self, path: str) -> bool:
        ext = Path(path).suffix.lower()
        return ext in {".py", ".sql", ".scala", ".java", ".js", ".ts", ".ipynb"}

    def _business_logic_location_score(self, path: str) -> float:
        lowered = path.lower()
        positive_markers = (
            "models",
            "marts",
            "dimensional",
            "reporting",
            "pipeline",
            "orchestr",
            "jobs",
            "tasks",
            "plugins",
            "connectors",
            "assets",
            "etl",
            "elt",
        )
        negative_markers = ("utils", "helpers")
        score = float(sum(1 for marker in positive_markers if marker in lowered))
        score -= float(sum(1 for marker in negative_markers if marker in lowered))
        return score

    def _business_logic_score(self, module: ModuleNode, call_centrality: int) -> float:
        return (
            (float(module.complexity_score) * 2.0)
            + (min(int(module.change_velocity_30d), 50) * 0.4)
            + (float(module.pagerank_score) * 180.0)
            + (min(call_centrality, 80) * 0.6)
            + self._business_logic_location_score(module.path)
        )

    def _velocity_location_score(self, path: str, repo_profile: str) -> float:
        lowered = path.lower()
        positive_markers = (
            "src/",
            "core/",
            "pipeline",
            "orchestr",
            "plugin",
            "block/",
            "runner",
            "invoke",
            "models/",
            "marts/",
            "reporting/",
        )
        if repo_profile == "sql_heavy":
            positive_markers = positive_markers + ("sql", "dbt", "staging/", "dimensional/")
        elif repo_profile == "python_heavy":
            positive_markers = positive_markers + ("cli/", "state", "job", "extract_load")

        negative_markers = (
            "docs/",
            "documentation/",
            "scripts/",
            ".github/",
            ".gitlab/",
            "examples/",
            "fixtures/",
            "migrations/",
            "alembic/",
            "noxfile.py",
            "docusaurus",
            "sidebars.js",
        )
        score = float(sum(1 for marker in positive_markers if marker in lowered))
        score -= float(sum(1 for marker in negative_markers if marker in lowered)) * 1.5
        return score

    def _velocity_priority_score(self, module: ModuleNode, call_centrality: int, repo_profile: str) -> float:
        return (
            (float(module.change_velocity_30d) * 8.0)
            + (float(module.pagerank_score) * 120.0)
            + (min(call_centrality, 80) * 0.4)
            + (float(module.complexity_score) * 0.8)
            + self._velocity_location_score(module.path, repo_profile)
        )

    def _git_velocity_rows(
        self,
        modules: list[ModuleNode],
        snapshot: GitVelocitySnapshot | None,
    ) -> dict[str, Any]:
        if snapshot is not None:
            return {
                "rows": [
                    {
                        "path": item.path,
                        "commit_count": int(item.commit_count),
                        "last_commit_timestamp": item.last_commit_timestamp,
                    }
                    for item in snapshot.files
                    if int(item.commit_count) > 0
                ],
                "status": snapshot.history_status,
                "note": snapshot.history_note,
                "time_window_days": int(snapshot.time_window_days),
            }

        # Fallback for unit tests and legacy call sites where git snapshot is not provided.
        fallback_rows = [
            {
                "path": module.path,
                "commit_count": int(module.change_velocity_30d),
                "last_commit_timestamp": str(getattr(module, "git_velocity_last_commit_timestamp", "") or ""),
            }
            for module in modules
            if int(module.change_velocity_30d) > 0
        ]
        fallback_rows = sorted(fallback_rows, key=lambda row: (-int(row["commit_count"]), str(row["path"])))
        return {
            "rows": fallback_rows,
            "status": "complete" if fallback_rows else "unavailable",
            "note": (
                "Velocity derived from module-level git metrics."
                if fallback_rows
                else "Velocity could not be fully computed because git metadata is unavailable."
            ),
            "time_window_days": 30,
        }

    def _rank_onboarding_velocity_rows(
        self,
        rows: list[dict[str, Any]],
        module_by_path: dict[str, ModuleNode],
        module_call_centrality: dict[str, int],
        repo_profile: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []

        def score(row: dict[str, Any]) -> float:
            path = str(row.get("path", "")).strip()
            if not path:
                return -1e9
            commit_count = max(0, int(row.get("commit_count", 0)))
            module = module_by_path.get(path)
            call_cent = module_call_centrality.get(path, 0)
            module_signal = 0.0
            if module is not None:
                module_signal = (
                    (float(module.pagerank_score) * 100.0)
                    + (float(module.complexity_score) * 0.6)
                    + (min(call_cent, 80) * 0.35)
                )
            onboarding_penalty = 0.0
            lowered = path.lower()
            if self._is_excluded_business_logic_path(path):
                onboarding_penalty += 10.0
            if self._is_support_module_path(lowered):
                onboarding_penalty += 6.0
            return (
                (float(commit_count) * 12.0)
                + (self._velocity_location_score(path, repo_profile) * 2.5)
                + module_signal
                - onboarding_penalty
            )

        ranked = sorted(rows, key=lambda row: (score(row), int(row.get("commit_count", 0)), str(row.get("path", ""))), reverse=True)
        return ranked[:limit]

    def _is_deprecation_guard(self, file_text: str) -> bool:
        lowered = file_text.lower()
        has_deprecation_hint = "deprecated" in lowered or "do not import" in lowered
        raises_import_error = "raise importerror" in lowered
        return has_deprecation_hint and raises_import_error

    def _is_deprecated_module(self, module: ModuleNode | None) -> bool:
        if module is None:
            return False
        if bool(getattr(module, "is_deprecated_guard", False)):
            return True
        purpose = str(module.purpose_statement or "").lower()
        return "deprecated guard module" in purpose

    def _normalize_confidence(self, value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"low", "medium", "high"}:
            return text
        return "medium"

    def _lineage_scan_completed(self, lineage_graph: KnowledgeGraph, modules: list[ModuleNode]) -> bool:
        if lineage_graph.graph.number_of_nodes() > 0 or lineage_graph.graph.number_of_edges() > 0:
            return True
        return len(modules) > 0

    def _infer_confidence_context_from_answers(
        self,
        answers: dict[str, DayOneAnswer],
        modules: list[ModuleNode],
        module_graph: KnowledgeGraph,
        module_downstream_counts: dict[str, int],
        module_call_centrality: dict[str, int],
        derived_outputs: list[str],
        fallback_outputs: list[str],
        lineage_scan_complete: bool,
    ) -> dict[str, dict[str, Any]]:
        module_by_path = {module.path: module for module in modules}

        q1 = answers.get("q1_primary_ingestion", DayOneAnswer(question_id="q1_primary_ingestion", answer=""))
        q1_methods = {str(item.get("analysis_method", "")).strip().lower() for item in q1.evidence}
        q1_entrypoint = bool(q1_methods) and q1_methods.issubset({"module_entrypoint_ingestion_heuristic"})

        q2 = answers.get("q2_critical_outputs", DayOneAnswer(question_id="q2_critical_outputs", answer=""))
        no_lineage_sinks_detected = not (derived_outputs or fallback_outputs)
        if no_lineage_sinks_detected:
            q2_answer = str(q2.answer or "").lower()
            if "output dataset" in q2_answer and "detected" not in q2_answer:
                no_lineage_sinks_detected = False

        q3 = answers.get("q3_blast_radius", DayOneAnswer(question_id="q3_blast_radius", answer=""))
        blast_module = ""
        if q3.evidence:
            blast_module = str(q3.evidence[0].get("source_file", "")).strip()
        if not blast_module:
            blast_module = self._extract_blast_module_from_answer_text(q3.answer)
        blast_downstream_count = None
        if blast_module:
            blast_downstream_count = int(module_downstream_counts.get(blast_module, 0))
        blast_zero_coverage_gap = False
        if blast_module and blast_downstream_count is not None:
            blast_zero_coverage_gap = self._blast_zero_is_coverage_gap(
                blast_module,
                blast_downstream_count,
                module_graph,
                module_downstream_counts,
            )

        q4 = answers.get("q4_logic_concentration", DayOneAnswer(question_id="q4_logic_concentration", answer=""))
        q4_paths = [
            str(item.get("source_file", "")).strip()
            for item in q4.evidence
            if str(item.get("source_file", "")).strip()
        ]
        if not q4_paths:
            q4_paths = self._extract_paths_from_text(q4.answer)
        business_logic_centrality_strong = self._business_logic_has_strong_signal(
            q4_paths,
            module_by_path,
            module_call_centrality,
        )

        q5 = answers.get("q5_change_velocity", DayOneAnswer(question_id="q5_change_velocity", answer=""))
        q5_methods = {str(item.get("analysis_method", "")).strip().lower() for item in q5.evidence}
        velocity_git_only = bool(q5_methods) and q5_methods.issubset({"git_log_frequency"})
        history_statuses = [
            str(item.get("history_status", "")).strip().lower()
            for item in q5.evidence
            if str(item.get("history_status", "")).strip()
        ]
        velocity_history_status = history_statuses[0] if history_statuses else ("complete" if q5.evidence else "unavailable")
        velocity_low_signal = (
            "low-confidence" in str(q5.answer).lower()
            or "sparse recent git history" in str(q5.answer).lower()
            or velocity_history_status in {"shallow", "missing", "unavailable"}
        )
        velocity_active_files = len(
            [
                item
                for item in q5.evidence
                if str(item.get("source_file", "")).strip() and int(item.get("commit_count", 0) or 0) > 0
            ]
        )
        velocity_time_window_days = 0
        for item in q5.evidence:
            try:
                velocity_time_window_days = int(item.get("time_window_days", 0) or 0)
            except (TypeError, ValueError):
                velocity_time_window_days = 0
            if velocity_time_window_days > 0:
                break

        return {
            "q1_primary_ingestion": {
                "entrypoint_ingestion_used": q1_entrypoint,
                "lineage_signals_weak": q1_entrypoint,
            },
            "q2_critical_outputs": {
                "no_lineage_sinks_detected": no_lineage_sinks_detected,
                "lineage_scan_complete": lineage_scan_complete,
            },
            "q3_blast_radius": {
                "blast_module": blast_module,
                "blast_downstream_count": blast_downstream_count,
                "blast_zero_coverage_gap": blast_zero_coverage_gap,
            },
            "q4_logic_concentration": {
                "focus_modules": q4_paths,
                "business_logic_centrality_strong": business_logic_centrality_strong,
            },
            "q5_change_velocity": {
                "velocity_git_only": velocity_git_only,
                "velocity_low_signal": velocity_low_signal,
                "velocity_scope_count": len(q5.evidence),
                "velocity_active_files": velocity_active_files,
                "velocity_history_status": velocity_history_status,
                "velocity_time_window_days": velocity_time_window_days,
            },
        }

    def _extract_blast_module_from_answer_text(self, answer_text: str) -> str:
        text = str(answer_text or "")
        match = re.search(r"If\s+(.+?)\s+fails", text)
        if not match:
            return ""
        return match.group(1).strip().strip("`")

    def _extract_paths_from_text(self, text: str) -> list[str]:
        candidates = re.findall(r"[A-Za-z0-9_\-./]+(?:\.[A-Za-z0-9]+)", str(text or ""))
        out: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            lowered = candidate.lower()
            if lowered in seen:
                continue
            if "/" not in candidate and "." not in candidate:
                continue
            seen.add(lowered)
            out.append(candidate)
        return out

    def _apply_day_one_confidence_model(
        self,
        answers: dict[str, DayOneAnswer],
        modules: list[ModuleNode],
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
        confidence_context: dict[str, dict[str, Any]],
    ) -> dict[str, DayOneAnswer]:
        question_order = [
            "q1_primary_ingestion",
            "q2_critical_outputs",
            "q3_blast_radius",
            "q4_logic_concentration",
            "q5_change_velocity",
        ]
        graph_stats = self._collect_graph_stats(module_graph, lineage_graph, modules)
        out: dict[str, DayOneAnswer] = {}
        for question_id in question_order:
            answer = answers.get(question_id, DayOneAnswer(question_id=question_id, answer=""))
            section_context = confidence_context.get(question_id, {})
            factors = self._compute_confidence_factors(
                question_id=question_id,
                answer=answer,
                section_context=section_context,
                graph_stats=graph_stats,
            )
            score = (
                (0.25 * factors["evidence_count"])
                + (0.20 * factors["evidence_diversity"])
                + (0.20 * factors["graph_coverage"])
                + (0.15 * factors["heuristic_reliability"])
                + (0.10 * factors["signal_agreement"])
                + (0.10 * factors["repo_type_fit"])
            )
            if question_id == "q5_change_velocity" and section_context.get("velocity_git_only"):
                history_status = str(section_context.get("velocity_history_status", "")).strip().lower()
                if history_status in {"missing", "unavailable"}:
                    score = min(score, 0.40)
                elif history_status == "shallow" or section_context.get("velocity_low_signal"):
                    score = min(score, 0.54)
                elif answer.evidence:
                    score = max(0.52, min(score, 0.74))
            label = self._confidence_label_from_score(score)
            if question_id == "q5_change_velocity" and section_context.get("velocity_git_only"):
                history_status = str(section_context.get("velocity_history_status", "")).strip().lower()
                if history_status in {"missing", "unavailable"}:
                    label = "low"
                elif history_status == "shallow":
                    label = "low" if score < 0.45 else "medium"
                elif answer.evidence:
                    label = "medium"
            reason = self._confidence_reason(
                question_id=question_id,
                label=label,
                score=score,
                factors=factors,
                section_context=section_context,
            )
            components = {
                "evidence_count_score": factors["evidence_count"],
                "evidence_diversity_score": factors["evidence_diversity"],
                "graph_coverage_score": factors["graph_coverage"],
                "heuristic_reliability_score": factors["heuristic_reliability"],
                "signal_agreement_score": factors["signal_agreement"],
                "repo_type_fit_score": factors["repo_type_fit"],
            }
            out[question_id] = answer.model_copy(
                update={
                    "confidence": label,
                    "confidence_label": label,
                    "confidence_score": round(self._clamp01(score), 4),
                    "confidence_factors": {name: round(self._clamp01(value), 4) for name, value in factors.items()},
                    "confidence_components": {name: round(self._clamp01(value), 4) for name, value in components.items()},
                    "confidence_reason": reason,
                }
            )
        return out

    def _collect_graph_stats(
        self,
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
        modules: list[ModuleNode],
    ) -> dict[str, float]:
        module_subgraph = module_graph.module_import_graph()
        repo_profile = self._infer_repo_profile(modules)
        dataset_nodes = [node_id for node_id, attrs in lineage_graph.graph.nodes(data=True) if attrs.get("node_type") == "dataset"]
        transformation_nodes = [
            node_id for node_id, attrs in lineage_graph.graph.nodes(data=True) if attrs.get("node_type") == "transformation"
        ]
        dataset_sink_count = 0
        for node_id in dataset_nodes:
            if lineage_graph.graph.out_degree(node_id) == 0 and lineage_graph.graph.in_degree(node_id) > 0:
                dataset_sink_count += 1
        return {
            "module_nodes": float(module_subgraph.number_of_nodes()),
            "module_edges": float(module_subgraph.number_of_edges()),
            "lineage_nodes": float(lineage_graph.graph.number_of_nodes()),
            "lineage_edges": float(lineage_graph.graph.number_of_edges()),
            "dataset_nodes": float(len(dataset_nodes)),
            "transformation_nodes": float(len(transformation_nodes)),
            "dataset_sink_count": float(dataset_sink_count),
            "module_count": float(len(modules)),
            "active_velocity_modules": float(len([m for m in modules if int(m.change_velocity_30d) > 0])),
            "sql_modules": float(len([m for m in modules if Path(m.path).suffix.lower() == ".sql"])),
            "python_modules": float(len([m for m in modules if Path(m.path).suffix.lower() == ".py"])),
            "repo_profile_sql_heavy": 1.0 if repo_profile == "sql_heavy" else 0.0,
            "repo_profile_python_heavy": 1.0 if repo_profile == "python_heavy" else 0.0,
            "repo_profile_mixed": 1.0 if repo_profile == "mixed" else 0.0,
        }

    def _compute_confidence_factors(
        self,
        question_id: str,
        answer: DayOneAnswer,
        section_context: dict[str, Any],
        graph_stats: dict[str, float],
    ) -> dict[str, float]:
        return {
            "evidence_count": self._compute_evidence_count_score(question_id, answer.evidence, section_context),
            "evidence_diversity": self._compute_evidence_diversity_score(question_id, answer.evidence, section_context),
            "graph_coverage": self._compute_graph_coverage_score(question_id, section_context, graph_stats),
            "heuristic_reliability": self._compute_heuristic_reliability_score(question_id, answer.evidence, section_context),
            "signal_agreement": self._compute_signal_agreement_score(question_id, answer, section_context),
            "repo_type_fit": self._compute_repo_type_fit_score(question_id, answer, section_context, graph_stats),
        }

    def _compute_evidence_count_score(
        self,
        question_id: str,
        evidence: list[dict[str, Any]],
        section_context: dict[str, Any],
    ) -> float:
        if (
            question_id == "q2_critical_outputs"
            and section_context.get("no_lineage_sinks_detected")
            and section_context.get("lineage_scan_complete")
        ):
            return 0.95
        if not evidence:
            return 0.0
        weighted_count = sum(self._method_reliability(str(item.get("analysis_method", ""))) for item in evidence)
        score = min(1.0, weighted_count / 3.0)
        if question_id == "q1_primary_ingestion" and section_context.get("lineage_signals_weak"):
            score *= 0.75
        if question_id == "q3_blast_radius" and section_context.get("blast_zero_coverage_gap"):
            score *= 0.55
        if question_id == "q4_logic_concentration" and section_context.get("business_logic_centrality_strong"):
            score = max(score, 0.90)
        if question_id == "q5_change_velocity" and section_context.get("velocity_git_only"):
            score = min(score, 0.82)
        return self._clamp01(score)

    def _compute_evidence_diversity_score(
        self,
        question_id: str,
        evidence: list[dict[str, Any]],
        section_context: dict[str, Any],
    ) -> float:
        if (
            question_id == "q2_critical_outputs"
            and section_context.get("no_lineage_sinks_detected")
            and section_context.get("lineage_scan_complete")
        ):
            return 0.85
        methods = [
            str(item.get("analysis_method", "")).strip().lower()
            for item in evidence
            if str(item.get("analysis_method", "")).strip()
        ]
        if not methods:
            return 0.0
        unique_methods = sorted(set(methods))
        diversity = min(1.0, len(unique_methods) / 4.0)
        avg_quality = sum(self._method_reliability(method) for method in unique_methods) / max(1, len(unique_methods))
        score = (0.6 * diversity) + (0.4 * avg_quality)
        if question_id == "q4_logic_concentration" and section_context.get("business_logic_centrality_strong"):
            score = max(score, 0.70)
        if question_id == "q3_blast_radius" and section_context.get("blast_zero_coverage_gap"):
            score = min(score, 0.30)
        if question_id == "q5_change_velocity" and section_context.get("velocity_git_only"):
            score = min(max(score, 0.35), 0.45)
        return self._clamp01(score)

    def _compute_graph_coverage_score(
        self,
        question_id: str,
        section_context: dict[str, Any],
        graph_stats: dict[str, float],
    ) -> float:
        module_nodes = graph_stats.get("module_nodes", 0.0)
        module_edges = graph_stats.get("module_edges", 0.0)
        lineage_nodes = graph_stats.get("lineage_nodes", 0.0)
        lineage_edges = graph_stats.get("lineage_edges", 0.0)
        dataset_nodes = graph_stats.get("dataset_nodes", 0.0)
        transformation_nodes = graph_stats.get("transformation_nodes", 0.0)
        dataset_sink_count = graph_stats.get("dataset_sink_count", 0.0)
        density_lineage = min(1.0, lineage_edges / max(1.0, dataset_nodes + transformation_nodes))
        density_module = min(1.0, module_edges / max(1.0, module_nodes))

        if question_id == "q1_primary_ingestion":
            if section_context.get("entrypoint_ingestion_used"):
                if module_nodes > 0:
                    return 0.55
                return 0.35
            if dataset_nodes > 0:
                return self._clamp01(0.55 + (0.45 * density_lineage))
            if lineage_nodes > 0:
                return 0.65
            return 0.50 if module_nodes > 0 else 0.20

        if question_id == "q2_critical_outputs":
            if section_context.get("no_lineage_sinks_detected") and section_context.get("lineage_scan_complete"):
                return 0.95
            if dataset_nodes > 0:
                sink_ratio = dataset_sink_count / max(1.0, dataset_nodes)
                return self._clamp01(0.40 + (0.35 * sink_ratio) + (0.25 * density_lineage))
            if lineage_nodes > 0:
                return 0.60
            return 0.75 if section_context.get("lineage_scan_complete") else 0.25

        if question_id == "q3_blast_radius":
            if module_nodes <= 0:
                return 0.20
            score = 0.25 + (0.45 * density_module)
            blast_module = str(section_context.get("blast_module", "")).strip()
            if blast_module:
                score += 0.30
            else:
                score += 0.10
            if section_context.get("blast_zero_coverage_gap"):
                score = min(score, 0.30)
            return self._clamp01(score)

        if question_id == "q4_logic_concentration":
            focus_modules = section_context.get("focus_modules", [])
            focus_count = float(len(focus_modules)) if isinstance(focus_modules, list) else 0.0
            score = 0.45 + (0.35 * min(1.0, focus_count / 5.0)) + (0.20 * density_module)
            if section_context.get("business_logic_centrality_strong"):
                score = max(score, 0.82)
            return self._clamp01(score)

        if question_id == "q5_change_velocity":
            module_count = graph_stats.get("module_count", 0.0)
            active = graph_stats.get("active_velocity_modules", 0.0)
            active_ratio = active / max(1.0, module_count)
            score = 0.45 + (0.35 * active_ratio) + (0.20 * density_module)
            history_status = str(section_context.get("velocity_history_status", "")).strip().lower()
            if history_status in {"missing", "unavailable"}:
                return 0.22
            if history_status == "shallow":
                score = min(score, 0.44)
            elif section_context.get("velocity_git_only"):
                score = min(score, 0.70)
            if section_context.get("velocity_low_signal") or history_status == "shallow":
                score = min(score, 0.58)
            return self._clamp01(score)

        return 0.50

    def _compute_heuristic_reliability_score(
        self,
        question_id: str,
        evidence: list[dict[str, Any]],
        section_context: dict[str, Any],
    ) -> float:
        methods = [
            str(item.get("analysis_method", "")).strip().lower()
            for item in evidence
            if str(item.get("analysis_method", "")).strip()
        ]
        if methods:
            base = sum(self._method_reliability(method) for method in methods) / max(1, len(methods))
        else:
            base = 0.30

        if (
            question_id == "q2_critical_outputs"
            and section_context.get("no_lineage_sinks_detected")
            and section_context.get("lineage_scan_complete")
        ):
            return 0.90
        if question_id == "q1_primary_ingestion" and section_context.get("entrypoint_ingestion_used"):
            base = min(base, 0.42)
        if question_id == "q3_blast_radius" and section_context.get("blast_zero_coverage_gap"):
            base = min(base, 0.20)
        if question_id == "q4_logic_concentration" and section_context.get("business_logic_centrality_strong"):
            base = max(base, 0.88)
        if question_id == "q5_change_velocity" and section_context.get("velocity_git_only"):
            history_status = str(section_context.get("velocity_history_status", "")).strip().lower()
            if history_status in {"missing", "unavailable"}:
                base = 0.20
            elif history_status == "shallow":
                base = 0.42
            else:
                base = 0.55 if section_context.get("velocity_low_signal") else 0.70
        return self._clamp01(base)

    def _compute_signal_agreement_score(
        self,
        question_id: str,
        answer: DayOneAnswer,
        section_context: dict[str, Any],
    ) -> float:
        if (
            question_id == "q2_critical_outputs"
            and section_context.get("no_lineage_sinks_detected")
            and section_context.get("lineage_scan_complete")
        ):
            return 0.95
        if question_id == "q3_blast_radius" and section_context.get("blast_zero_coverage_gap"):
            return 0.25

        methods = [
            str(item.get("analysis_method", "")).strip().lower()
            for item in answer.evidence
            if str(item.get("analysis_method", "")).strip()
        ]
        if not methods:
            return 0.20

        family_presence = {"lineage": False, "module": False, "git": False, "heuristic": False}
        for method in methods:
            family = self._signal_family(method)
            if family in family_presence:
                family_presence[family] = True
        expected_families = self._expected_signal_families(question_id)
        expected_hits = len([family for family in expected_families if family_presence.get(family, False)])
        expected_score = expected_hits / max(1, len(expected_families))
        strong_method_ratio = len([m for m in methods if self._method_reliability(m) >= 0.75]) / max(1, len(methods))
        answer_text = str(answer.answer or "").lower()
        contradiction_penalty = 0.0
        if "no obvious output dataset detected" in answer_text and expected_hits > 0 and question_id == "q2_critical_outputs":
            contradiction_penalty = 0.05
        if "0 downstream nodes" in answer_text and question_id == "q3_blast_radius" and expected_hits == 0:
            contradiction_penalty = 0.10
        score = (0.60 * expected_score) + (0.40 * strong_method_ratio) - contradiction_penalty
        if question_id == "q1_primary_ingestion" and section_context.get("entrypoint_ingestion_used"):
            score = min(score, 0.60)
        if question_id == "q5_change_velocity" and section_context.get("velocity_git_only"):
            history_status = str(section_context.get("velocity_history_status", "")).strip().lower()
            if history_status in {"missing", "unavailable"}:
                score = min(score, 0.28)
            elif history_status == "shallow":
                score = min(max(score, 0.35), 0.55)
            else:
                score = max(0.60, min(score, 0.78))
        return self._clamp01(score)

    def _compute_repo_type_fit_score(
        self,
        question_id: str,
        answer: DayOneAnswer,
        section_context: dict[str, Any],
        graph_stats: dict[str, float],
    ) -> float:
        sql_heavy = bool(graph_stats.get("repo_profile_sql_heavy", 0.0) > 0.0)
        python_heavy = bool(graph_stats.get("repo_profile_python_heavy", 0.0) > 0.0)
        mixed = bool(graph_stats.get("repo_profile_mixed", 0.0) > 0.0)
        source_files = [str(item.get("source_file", "")).strip().lower() for item in answer.evidence]
        sql_evidence_ratio = (
            len([path for path in source_files if path.endswith(".sql")]) / max(1, len(source_files))
            if source_files
            else 0.0
        )
        py_evidence_ratio = (
            len([path for path in source_files if path.endswith(".py")]) / max(1, len(source_files))
            if source_files
            else 0.0
        )
        answer_text = str(answer.answer or "").lower()
        mentions_operational_outputs = any(
            marker in answer_text for marker in ("plugin runs", "state", "job metadata", "orchestration")
        )
        has_entrypoint_signals = bool(section_context.get("entrypoint_ingestion_used"))

        if sql_heavy:
            if question_id in {"q1_primary_ingestion", "q2_critical_outputs", "q4_logic_concentration"}:
                return self._clamp01(0.55 + (0.45 * sql_evidence_ratio))
            return 0.70
        if python_heavy:
            if question_id == "q1_primary_ingestion":
                if has_entrypoint_signals:
                    return 0.85
                return self._clamp01(0.55 + (0.35 * py_evidence_ratio))
            if question_id == "q2_critical_outputs":
                if section_context.get("no_lineage_sinks_detected") and section_context.get("lineage_scan_complete"):
                    return 0.90 if mentions_operational_outputs else 0.80
                return self._clamp01(0.45 + (0.35 * py_evidence_ratio))
            if question_id in {"q3_blast_radius", "q4_logic_concentration", "q5_change_velocity"}:
                return self._clamp01(0.55 + (0.35 * py_evidence_ratio))
            return 0.70
        if mixed:
            if source_files:
                return self._clamp01(0.55 + (0.25 * max(sql_evidence_ratio, py_evidence_ratio)))
            return 0.60
        return 0.60

    def _signal_family(self, method: str) -> str:
        method_norm = str(method or "").strip().lower()
        if method_norm in {"sqlglot", "tree_sitter_python+sqlglot", "tree_sitter_python", "lineage_graph_sources", "lineage_graph_sinks"}:
            return "lineage"
        if method_norm in {"module_graph_descendants", "complexity_and_velocity_signals"}:
            return "module"
        if method_norm in {"git_log_frequency"}:
            return "git"
        if method_norm in {"module_entrypoint_ingestion_heuristic"}:
            return "heuristic"
        return "heuristic"

    def _expected_signal_families(self, question_id: str) -> list[str]:
        expected = {
            "q1_primary_ingestion": ["lineage", "heuristic"],
            "q2_critical_outputs": ["lineage"],
            "q3_blast_radius": ["module"],
            "q4_logic_concentration": ["module"],
            "q5_change_velocity": ["git"],
        }
        return expected.get(question_id, ["heuristic"])

    def _confidence_reason(
        self,
        question_id: str,
        label: str,
        score: float,
        factors: dict[str, float],
        section_context: dict[str, Any],
    ) -> str:
        score_text = f"{self._clamp01(score):.2f}"
        if (
            question_id == "q2_critical_outputs"
            and section_context.get("no_lineage_sinks_detected")
            and section_context.get("lineage_scan_complete")
        ):
            return (
                f"Confidence is {label} ({score_text}) because lineage graph coverage is complete and consistently "
                "shows no terminal output datasets."
            )
        if question_id == "q1_primary_ingestion" and section_context.get("entrypoint_ingestion_used"):
            return (
                f"Confidence is {label} ({score_text}) because ingestion is inferred from CLI/orchestration entrypoint "
                "heuristics with limited lineage cross-validation."
            )
        if question_id == "q3_blast_radius" and section_context.get("blast_zero_coverage_gap"):
            return (
                f"Confidence is {label} ({score_text}) because blast radius depends on a sparse module graph where "
                "zero downstream edges may reflect incomplete coverage."
            )
        if question_id == "q5_change_velocity" and section_context.get("velocity_git_only"):
            history_status = str(section_context.get("velocity_history_status", "")).strip().lower()
            window = int(section_context.get("velocity_time_window_days", 0) or 0)
            window_text = f" over {window} days" if window > 0 else ""
            if history_status == "shallow":
                return (
                    f"Confidence is {label} ({score_text}) because velocity is derived from git-frequency signals{window_text} "
                    "but clone history is shallow."
                )
            if history_status in {"missing", "unavailable"}:
                return (
                    f"Confidence is {label} ({score_text}) because velocity could not be fully computed from git history "
                    "and available evidence is incomplete."
                )
            return (
                f"Confidence is {label} ({score_text}) because the result is derived from git-frequency signals only"
                f"{window_text}, "
                "with limited corroborating structural evidence."
            )
        sorted_factors = sorted(factors.items(), key=lambda item: item[1], reverse=True)
        strongest = sorted_factors[0][0] if sorted_factors else "graph_coverage"
        weakest = sorted_factors[-1][0] if sorted_factors else "heuristic_reliability"
        return (
            f"Confidence is {label} ({score_text}) because {strongest} provides the strongest support while {weakest} "
            "is the primary limiting factor."
        )

    def _method_reliability(self, method: str) -> float:
        method_norm = str(method or "").strip().lower()
        reliability = {
            "sqlglot": 0.92,
            "tree_sitter_python+sqlglot": 0.88,
            "tree_sitter_python": 0.70,
            "lineage_graph_sources": 0.82,
            "lineage_graph_sinks": 0.86,
            "module_graph_descendants": 0.86,
            "complexity_and_velocity_signals": 0.84,
            "git_log_frequency": 0.78,
            "module_entrypoint_ingestion_heuristic": 0.35,
        }
        return reliability.get(method_norm, 0.55)

    def _blast_zero_is_coverage_gap(
        self,
        blast_module: str,
        affected_count: int,
        module_graph: KnowledgeGraph,
        module_downstream_counts: dict[str, int],
    ) -> bool:
        if affected_count != 0:
            return False
        subgraph = module_graph.module_import_graph()
        node_count = subgraph.number_of_nodes()
        edge_count = subgraph.number_of_edges()
        if node_count <= 1:
            return True
        if edge_count == 0:
            return True
        non_zero_descendants = len([count for count in module_downstream_counts.values() if int(count) > 0])
        if non_zero_descendants <= max(1, node_count // 10):
            return True
        if blast_module in subgraph and subgraph.degree(blast_module) == 0 and edge_count < max(1, node_count - 1):
            return True
        return False

    def _business_logic_has_strong_signal(
        self,
        focus_paths: list[str],
        module_by_path: dict[str, ModuleNode],
        module_call_centrality: dict[str, int],
    ) -> bool:
        if not focus_paths:
            return False
        scored = 0
        for path in focus_paths:
            module = module_by_path.get(path)
            if not module:
                continue
            has_complexity = float(module.complexity_score) > 0
            has_centrality = float(module.pagerank_score) > 0 or module_call_centrality.get(path, 0) > 0
            if has_complexity and has_centrality:
                scored += 1
        return scored >= max(1, len(focus_paths) // 2)

    def _confidence_label_from_score(self, score: float) -> str:
        normalized = self._clamp01(score)
        if normalized >= 0.75:
            return "high"
        if normalized >= 0.45:
            return "medium"
        return "low"

    def _clamp01(self, value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def _lineage_evidence_for_nodes(
        self, lineage_graph: KnowledgeGraph, node_ids: list[str], limit: int = 5
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[tuple[str, tuple[int, int], str]] = set()
        node_set = set(node_ids)
        for source, target, attrs in lineage_graph.graph.edges(data=True):
            if source in node_set or target in node_set:
                source_file = str(attrs.get("source_file", ""))
                line_range = list(attrs.get("line_range", (0, 0)))
                if len(line_range) != 2:
                    line_range = [0, 0]
                key = (source_file, (int(line_range[0]), int(line_range[1])), str(attrs.get("analysis_method", "")))
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "analysis_method": attrs.get("analysis_method", "lineage_graph"),
                        "source_file": source_file,
                        "line_range": [key[1][0], key[1][1]],
                    }
                )
            if len(out) >= limit:
                break
        return out
