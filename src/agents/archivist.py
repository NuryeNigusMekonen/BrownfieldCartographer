from __future__ import annotations

import json
from pathlib import Path
import re

from src.analyzers.git_history import GitVelocitySnapshot
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import DayOneAnswer, ModuleNode, TraceEvent


class ArchivistAgent:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def write_trace(self, events: list[TraceEvent]) -> Path:
        path = self.out_dir / "cartography_trace.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for event in events:
                payload = event.model_dump(mode="json")
                payload["evidence"] = self._enrich_trace_evidence(payload)
                f.write(json.dumps(payload) + "\n")
        return path

    def write_module_graph(self, graph: KnowledgeGraph) -> Path:
        path = self.out_dir / "module_graph.json"
        graph.serialize(path)
        return path

    def write_lineage_graph(self, graph: KnowledgeGraph) -> Path:
        path = self.out_dir / "lineage_graph.json"
        graph.serialize(path)
        return path

    def write_semantic_index(self, modules: dict[str, ModuleNode]) -> Path:
        semantic_dir = self.out_dir / "semantic_index"
        semantic_dir.mkdir(parents=True, exist_ok=True)
        path = semantic_dir / "module_purpose_index.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for module in modules.values():
                f.write(
                    json.dumps(
                        {
                            "path": module.path,
                            "purpose_statement": module.purpose_statement,
                            "domain_cluster": module.domain_cluster,
                            "language": module.language,
                            "complexity_score": module.complexity_score,
                            "change_velocity_30d": module.change_velocity_30d,
                        }
                    )
                    + "\n"
                )
        return path

    def generate_codebase_md(
        self,
        modules: dict[str, ModuleNode],
        top_modules: list[str],
        scc: list[list[str]],
        sources: list[str],
        sinks: list[str],
        git_velocity_snapshot: GitVelocitySnapshot | None = None,
    ) -> Path:
        path = self.out_dir / "CODEBASE.md"
        source_items = self._prepare_codebase_lineage_entities(sources)
        sink_items = self._prepare_codebase_lineage_entities(sinks)
        architecture_overview = self._codebase_architecture_overview(
            modules=modules,
            top_modules=top_modules,
            source_items=source_items,
            sink_items=sink_items,
        )
        raw_velocity_lines = self._raw_git_velocity_lines(modules, git_velocity_snapshot, limit=10)
        debt_lines = [f"- Circular dependency: {', '.join(comp)}" for comp in scc]
        doc_drift = [m.path for m in modules.values() if "Documentation Drift" in m.purpose_statement]
        debt_lines.extend([f"- Documentation drift: {d}" for d in doc_drift[:10]])
        if not debt_lines:
            debt_lines = ["- No major structural debt flags detected."]

        content = [
            "# CODEBASE",
            "",
            "## Architecture Overview",
            architecture_overview,
            "",
            "## Critical Path",
            *[f"- {m}" for m in top_modules[:5]],
            "",
            "## Data Sources",
            *[f"- {s}" for s in source_items[:20]],
            "",
            "## Data Sinks",
            *[f"- {s}" for s in sink_items[:20]],
            "",
            "## Known Debt",
            *debt_lines,
            "",
            "## High-Velocity Files",
            "Raw git history shows the most frequently modified files, while onboarding-relevant velocity highlights fast-changing runtime areas most important for a new engineer.",
            *raw_velocity_lines,
            "",
            "## Module Purpose Index",
            *[f"- {m.path}: {m.purpose_statement}" for m in list(modules.values())[:200]],
        ]
        path.write_text("\n".join(content), encoding="utf-8")
        return path

    def _enrich_trace_evidence(self, event_payload: dict[str, object]) -> dict[str, object]:
        evidence = event_payload.get("evidence", {})
        if not isinstance(evidence, dict):
            evidence = {}

        enriched = dict(evidence)
        if "analysis_method" not in enriched:
            enriched["analysis_method"] = self._infer_trace_analysis_method(event_payload, enriched)

        sources = self._extract_trace_evidence_sources(enriched)
        if "evidence_sources" not in enriched:
            enriched["evidence_sources"] = sources
        elif isinstance(enriched.get("evidence_sources"), list):
            merged = list(enriched.get("evidence_sources", [])) + sources
            enriched["evidence_sources"] = sorted({str(item).strip() for item in merged if str(item).strip()})
        else:
            enriched["evidence_sources"] = sources
        return enriched

    def _infer_trace_analysis_method(self, event_payload: dict[str, object], evidence: dict[str, object]) -> str:
        agent = str(event_payload.get("agent", "")).strip().lower()
        if agent in {"surveyor", "hydrologist"}:
            return "static"
        if agent == "semanticist":
            usage = evidence.get("model_usage_counts", {})
            if isinstance(usage, dict) and usage:
                total_calls = 0
                for value in usage.values():
                    try:
                        total_calls += int(value)
                    except (TypeError, ValueError):
                        continue
                if total_calls > 0:
                    return "hybrid_llm_static"
            return "static"
        if agent == "orchestrator":
            return "orchestration"
        return "unknown"

    def _extract_trace_evidence_sources(self, evidence: dict[str, object], limit: int = 50) -> list[str]:
        sources: list[str] = []

        def add(value: object) -> None:
            text = str(value or "").strip()
            if not text:
                return
            sources.append(text)

        def walk(obj: object) -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    key_norm = str(key).strip().lower()
                    if key_norm in {"source_file", "file", "path", "source"}:
                        add(value)
                        continue
                    if key_norm in {"source_files", "paths", "files", "failed_files"}:
                        walk(value)
                        continue
                    # recurse into nested evidence payloads
                    if isinstance(value, (dict, list)):
                        walk(value)
                return
            if isinstance(obj, list):
                for item in obj:
                    walk(item)
                return
            # ignore scalar values outside keyed context

        walk(evidence)
        seen: set[str] = set()
        ordered: list[str] = []
        for item in sources:
            lowered = item.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            ordered.append(item)
            if len(ordered) >= max(1, limit):
                break
        return ordered

    def _codebase_architecture_overview(
        self,
        modules: dict[str, ModuleNode],
        top_modules: list[str],
        source_items: list[str],
        sink_items: list[str],
    ) -> str:
        profile = self._repo_profile_for_codebase(modules)
        total_modules = len(modules)
        dominant_dirs = self._dominant_module_directories(modules, limit=2)
        critical_preview = ", ".join(top_modules[:2]) if top_modules else ""
        base_sentence: str
        if profile == "sql_heavy":
            base_sentence = (
                "This repository is a SQL-heavy transformation project where source datasets flow through staged models into reporting outputs."
            )
        elif profile == "python_heavy":
            base_sentence = (
                "This repository is a Python-heavy orchestration platform where runtime modules coordinate ingestion, execution, and stateful pipeline operations."
            )
        else:
            base_sentence = (
                "This repository combines orchestration code and transformation logic to move data from upstream sources into downstream analytical outputs."
            )
        context_parts: list[str] = [f"The current analysis mapped {total_modules} modules"]
        if dominant_dirs:
            context_parts.append(f"with most activity concentrated in {', '.join(dominant_dirs)}")
        if source_items or sink_items:
            context_parts.append(
                f"and identified {len(source_items)} source-side entities and {len(sink_items)} sink-side entities"
            )
        context_sentence = " ".join(context_parts).strip() + "."
        if critical_preview:
            return f"{base_sentence} {context_sentence} Critical execution path starts with {critical_preview}."
        return f"{base_sentence} {context_sentence}"

    def _repo_profile_for_codebase(self, modules: dict[str, ModuleNode]) -> str:
        if not modules:
            return "mixed"
        sql = 0
        py = 0
        for module in modules.values():
            suffix = Path(module.path).suffix.lower()
            if suffix == ".sql":
                sql += 1
            elif suffix == ".py":
                py += 1
        total = sql + py
        if total == 0:
            return "mixed"
        if (sql / total) >= 0.6:
            return "sql_heavy"
        if (py / total) >= 0.6:
            return "python_heavy"
        return "mixed"

    def _dominant_module_directories(self, modules: dict[str, ModuleNode], limit: int = 2) -> list[str]:
        counts: dict[str, int] = {}
        for module in modules.values():
            directory = str(Path(module.path).parent).replace("\\", "/").strip()
            if not directory or directory == ".":
                continue
            counts[directory] = counts.get(directory, 0) + 1
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return [directory for directory, _ in ranked[:limit]]

    def _prepare_codebase_lineage_entities(self, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for raw in values:
            label = self._clean_lineage_entity_label(raw)
            if not label:
                continue
            lowered = label.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            cleaned.append(label)
        if cleaned:
            return cleaned
        return ["No clear entities detected from static lineage analysis."]

    def _clean_lineage_entity_label(self, raw: str) -> str:
        value = str(raw or "").strip()
        if not value:
            return ""
        lowered = value.lower()
        if "dynamic reference, cannot resolve" in lowered:
            return ""
        if value.startswith("dataset::"):
            value = value.replace("dataset::", "", 1)
        elif value.startswith("transform::"):
            value = value.replace("transform::", "", 1)
            if value:
                value = f"{value} (transformation)"
        value = value.strip()
        if not value:
            return ""
        return value

    def generate_onboarding_brief(self, day_one_answers: dict[str, DayOneAnswer]) -> Path:
        path = self.out_dir / "onboarding_brief.md"
        q1 = day_one_answers.get("q1_primary_ingestion", DayOneAnswer(question_id="q1", answer=""))
        q2 = day_one_answers.get("q2_critical_outputs", DayOneAnswer(question_id="q2", answer=""))
        q3 = day_one_answers.get("q3_blast_radius", DayOneAnswer(question_id="q3", answer=""))
        q4 = day_one_answers.get("q4_logic_concentration", DayOneAnswer(question_id="q4", answer=""))
        q5 = day_one_answers.get("q5_change_velocity", DayOneAnswer(question_id="q5", answer=""))
        repo_profile = self._infer_repo_profile([q1, q2, q3, q4, q5])
        lines = ["# FDE Day-One Brief", ""]
        lines.extend(self._render_day_one_section("1) Primary Data Ingestion Path", q1, repo_profile))
        lines.extend(self._render_day_one_section("2) Critical Output Datasets/Endpoints", q2, repo_profile))
        lines.extend(self._render_day_one_section("3) Blast Radius of Critical Module Failure", q3, repo_profile))
        lines.extend(self._render_day_one_section("4) Business Logic Concentration", q4, repo_profile))
        lines.extend(self._render_day_one_section("5) Onboarding-Relevant High-Velocity Areas", q5, repo_profile))
        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def _render_day_one_section(self, heading: str, answer: DayOneAnswer, repo_profile: str) -> list[str]:
        entities = self._section_entities(answer)
        explanation = self._section_explanation(
            answer.question_id,
            entities,
            answer.answer,
            answer.confidence,
            repo_profile,
            answer.evidence,
        )
        entity_label = self._section_entity_label(answer.question_id)
        section_lines = [
            f"## {heading}",
            explanation,
            "",
            f"{entity_label}:",
        ]
        if entities:
            section_lines.extend([f"- {entity}" for entity in entities])
        else:
            section_lines.append("- Not clearly detected from current static analysis.")
        section_lines.extend(
            [
                "",
                self._confidence_line(answer),
                self._confidence_label_line(answer),
                self._confidence_factors_line(answer),
                self._confidence_reason_line(answer),
                "",
                "Evidence:",
                f"- {json.dumps(answer.evidence)}",
                "",
            ]
        )
        return section_lines

    def _confidence_line(self, answer: DayOneAnswer) -> str:
        score = float(answer.confidence_score)
        label = self._resolved_confidence_label(answer)
        return f"Confidence: {label} (score: {score:.2f})"

    def _confidence_label_line(self, answer: DayOneAnswer) -> str:
        label = self._resolved_confidence_label(answer)
        return f"Confidence label: {label}"

    def _resolved_confidence_label(self, answer: DayOneAnswer) -> str:
        legacy = str(answer.confidence or "medium").strip().lower()
        label = str(answer.confidence_label or legacy or "medium").strip().lower()
        if label == "medium" and legacy in {"low", "high"}:
            return legacy
        if label in {"low", "medium", "high"}:
            return label
        if legacy in {"low", "medium", "high"}:
            return legacy
        return "medium"

    def _confidence_factors_line(self, answer: DayOneAnswer) -> str:
        factors = answer.confidence_factors or {}
        evidence_count = float(factors.get("evidence_count", 0.0))
        evidence_diversity = float(factors.get("evidence_diversity", 0.0))
        graph_coverage = float(factors.get("graph_coverage", 0.0))
        heuristic_reliability = float(factors.get("heuristic_reliability", 0.0))
        signal_agreement = float(factors.get("signal_agreement", 0.0))
        repo_type_fit = float(factors.get("repo_type_fit", 0.0))
        return (
            "Confidence factors: "
            f"evidence_count={evidence_count:.2f}, "
            f"evidence_diversity={evidence_diversity:.2f}, "
            f"graph_coverage={graph_coverage:.2f}, "
            f"heuristic_reliability={heuristic_reliability:.2f}, "
            f"signal_agreement={signal_agreement:.2f}, "
            f"repo_type_fit={repo_type_fit:.2f}"
        )

    def _confidence_reason_line(self, answer: DayOneAnswer) -> str:
        reason = str(answer.confidence_reason or "").strip()
        if not reason:
            reason = "Confidence reason unavailable from current analysis context."
        return f"Confidence reason: {reason}"

    def _section_entity_label(self, question_id: str) -> str:
        labels = {
            "q1_primary_ingestion": "Key sources",
            "q2_critical_outputs": "Key outputs",
            "q3_blast_radius": "Key modules",
            "q4_logic_concentration": "Key files",
            "q5_change_velocity": "Onboarding-relevant files",
        }
        return labels.get(question_id, "Key entities")

    def _section_entities(self, answer: DayOneAnswer, limit: int = 5) -> list[str]:
        if answer.question_id in {"q1_primary_ingestion", "q2_critical_outputs"}:
            candidates = self._entities_from_answer(answer.answer)
        else:
            candidates = self._entities_from_evidence(answer.evidence)
            if not candidates:
                candidates = self._entities_from_answer(answer.answer)
        out: list[str] = []
        seen: set[str] = set()
        for entity in candidates:
            normalized = entity.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
            if len(out) >= limit:
                break
        return out

    def _entities_from_answer(self, answer: str) -> list[str]:
        parts = re.split(r"[,;\n]", answer)
        cleaned: list[str] = []
        for part in parts:
            entity = self._clean_entity_label(part)
            if entity:
                cleaned.append(entity)
        return cleaned

    def _entities_from_evidence(self, evidence: list[dict[str, object]]) -> list[str]:
        cleaned: list[str] = []
        for item in evidence:
            source_file = self._clean_entity_label(str(item.get("source_file", "")))
            if source_file:
                cleaned.append(source_file)
        return cleaned

    def _clean_entity_label(self, text: str) -> str:
        value = text.strip().lstrip("-").strip().strip("`'\"")
        if not value:
            return ""
        value = re.sub(r"^(dataset|pipeline|config|transform)::", "", value, flags=re.IGNORECASE)
        lowered = value.lower().strip()
        if not lowered:
            return ""
        if "dynamic reference, cannot resolve" in lowered:
            return ""
        if lowered.startswith(("call ", "install ", "checkpoint", "load ", "vacuum", "pragma ", "set ", "use ")):
            return ""
        if lowered.startswith("if "):
            return ""
        value = re.sub(r"\s+", " ", value).strip()
        if len(value) > 180:
            return ""
        return value

    def _section_explanation(
        self,
        question_id: str,
        entities: list[str],
        answer_text: str,
        confidence: str,
        repo_profile: str,
        evidence: list[dict[str, object]],
    ) -> str:
        confidence_level = str(confidence or "").strip().lower()
        if question_id == "q1_primary_ingestion":
            source_preview = self._entity_preview(entities, limit=2)
            module_mode = self._is_module_entrypoint_ingestion(entities, evidence)
            flow_phrase = (
                "through staging, intermediate, dimensional, and reporting models"
                if repo_profile == "sql_heavy"
                else "through orchestration jobs and transformation steps"
                if repo_profile == "python_heavy"
                else "through staging and transformation layers"
            )
            if module_mode:
                if confidence_level == "high":
                    if source_preview:
                        return (
                            f"Data ingestion is initiated by entrypoint modules such as {source_preview}, then proceeds "
                            f"{flow_phrase} into downstream operational and reporting artifacts."
                        )
                    return (
                        "Data ingestion is initiated by orchestration entrypoint modules, then proceeds "
                        f"{flow_phrase} into downstream operational and reporting artifacts."
                    )
                if confidence_level == "medium":
                    if source_preview:
                        return (
                            f"Ingestion likely starts from entrypoint modules such as {source_preview} and then proceeds "
                            f"{flow_phrase}."
                        )
                    return f"Ingestion likely starts from orchestration entrypoint modules and then proceeds {flow_phrase}."
                return (
                    "Available analysis signals indicate ingestion through entrypoint modules before downstream "
                    "orchestration and transformation steps."
                )
            if confidence_level == "high":
                if source_preview:
                    return (
                        f"Data enters through source-aligned datasets such as {source_preview}, then flows {flow_phrase} "
                        "into analytics-ready reporting tables."
                    )
                return (
                    "Data enters through upstream source systems, then flows "
                    f"{flow_phrase} into analytics-ready reporting tables."
                )
            if confidence_level == "medium":
                if source_preview:
                    return (
                        f"Data flow starts at datasets such as {source_preview} and moves {flow_phrase} into reporting tables."
                    )
                return f"Data flow starts at upstream source datasets and moves {flow_phrase} into reporting tables."
            return f"Available analysis signals indicate a flow from upstream sources {flow_phrase} into reporting tables."
        if question_id == "q2_critical_outputs":
            output_preview = self._entity_preview(entities, limit=2)
            if confidence_level == "low" and repo_profile == "python_heavy":
                return (
                    "No strong terminal dataset sinks were detected from static lineage. For this orchestration-heavy "
                    "repository, downstream outputs are likely operational artifacts such as plugin runs, state, and job metadata."
                )
            if confidence_level == "high":
                if output_preview:
                    return (
                        f"Critical outputs are analytics/reporting datasets such as {output_preview}, which serve "
                        "dashboards and recurring business reporting."
                    )
                return "Critical outputs are terminal analytics/reporting datasets consumed by dashboards and business reporting."
            if confidence_level == "medium":
                if output_preview:
                    return (
                        f"Detected outputs include analytics/reporting datasets such as {output_preview}, used by "
                        "downstream dashboards and reporting."
                    )
                return "Detected outputs are terminal analytics/reporting datasets consumed by downstream reporting."
            return "Available lineage signals identify terminal analytics/reporting datasets used by downstream reporting."
        if question_id == "q3_blast_radius":
            count = self._extract_downstream_count(answer_text)
            module_name = self._extract_blast_module(answer_text, entities)
            if count == 0 and module_name:
                return (
                    f"If {module_name} fails, the current dependency graph shows 0 downstream modules. "
                    "This usually indicates limited graph coverage or an entrypoint mismatch, so operational impact may be higher."
                )
            if count == 0:
                return (
                    "The current dependency graph shows 0 downstream modules for the selected target. "
                    "This usually indicates limited graph coverage or an entrypoint mismatch, so operational impact may be higher."
                )
            if count is not None and module_name:
                return (
                    f"If {module_name} fails, at least {count} downstream modules are in the dependency path "
                    "based on the module dependency graph."
                )
            if count is not None:
                return (
                    f"Failure of the selected critical module affects at least {count} downstream modules in the "
                    "module dependency graph."
                )
            return "Blast radius is estimated from the module dependency graph by counting downstream reachable nodes."
        if question_id == "q4_logic_concentration":
            interpretation = (
                "These files define warehouse transformations and reporting model logic."
                if repo_profile == "sql_heavy"
                else "These files drive orchestration behavior, connector execution, and pipeline control flow."
                if repo_profile == "python_heavy"
                else "These files implement core transformation rules, aggregations, and reporting dataset definitions."
            )
            if entities:
                focus_preview = self._entity_preview(entities, limit=2)
                return (
                    f"Business logic is concentrated in files such as {focus_preview}. {interpretation}"
                )
            return (
                f"Business logic is concentrated in transformation and orchestration files. {interpretation}"
            )
        if question_id == "q5_change_velocity":
            return (
                "This view re-ranks raw git-history velocity into onboarding-relevant areas so new engineers can focus "
                "on fast-changing runtime paths without losing traceability to commit history."
            )
        return "This section summarizes the strongest signals detected for day-one onboarding."

    def _raw_git_velocity_lines(
        self,
        modules: dict[str, ModuleNode],
        snapshot: GitVelocitySnapshot | None,
        limit: int = 10,
    ) -> list[str]:
        if snapshot is not None:
            rows = [item for item in snapshot.files if int(item.commit_count) > 0][:limit]
            if rows:
                lines = [
                    f"- Git history status: {snapshot.history_status} ({snapshot.history_note})",
                    f"- Time window: {snapshot.time_window_days} days",
                ]
                lines.extend(
                    [
                        (
                            f"- {item.path} "
                            f"(analysis_method=git_log_frequency, commit_count={item.commit_count}, "
                            f"time_window_days={snapshot.time_window_days}, "
                            f"last_commit_timestamp={item.last_commit_timestamp or 'unknown'})"
                        )
                        for item in rows
                    ]
                )
                return lines
            return [
                f"- Git history status: {snapshot.history_status} ({snapshot.history_note})",
                f"- Time window: {snapshot.time_window_days} days",
                "- No file commits found in this window.",
            ]

        fallback = sorted(modules.values(), key=lambda m: m.change_velocity_30d, reverse=True)
        fallback = [module for module in fallback if int(module.change_velocity_30d) > 0][:limit]
        if not fallback:
            return [
                "- Git history status: unavailable (Velocity could not be fully computed because git metadata is missing.)",
                "- No file commits found in this window.",
            ]
        lines = [
            "- Git history status: complete (Velocity derived from module-level git metrics.)",
            "- Time window: 30 days",
        ]
        lines.extend(
            [
                (
                    f"- {module.path} (analysis_method=git_log_frequency, commit_count={int(module.change_velocity_30d)}, "
                    f"time_window_days=30, last_commit_timestamp={str(getattr(module, 'git_velocity_last_commit_timestamp', '') or 'unknown')})"
                )
                for module in fallback
            ]
        )
        return lines

    def _entity_preview(self, entities: list[str], limit: int = 2) -> str:
        if not entities:
            return ""
        preview = [entity for entity in entities[:limit] if entity.strip()]
        return ", ".join(preview)

    def _extract_downstream_count(self, answer_text: str) -> int | None:
        lowered = str(answer_text or "").lower()
        match = re.search(r"at\s+least\s+(\d+)\s+downstream\s+(?:nodes|modules)", lowered)
        if not match:
            match = re.search(r"\b(\d+)\s+downstream\s+(?:nodes|modules)", lowered)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    def _extract_blast_module(self, answer_text: str, entities: list[str]) -> str:
        text = str(answer_text or "").strip()
        fail_match = re.search(r"If\s+(.+?)\s+fails", text)
        if fail_match:
            return fail_match.group(1).strip().strip("`")
        return entities[0] if entities else ""

    def _is_module_entrypoint_ingestion(
        self,
        entities: list[str],
        evidence: list[dict[str, object]],
    ) -> bool:
        if not entities and not evidence:
            return False
        methods = [str(item.get("analysis_method", "")).strip().lower() for item in evidence]
        if methods and all(method == "module_entrypoint_ingestion_heuristic" for method in methods):
            return True
        module_like = 0
        for entity in entities:
            lowered = str(entity).strip().lower()
            if not lowered:
                continue
            if "/" in lowered or lowered.endswith((".py", ".ts", ".js", ".java", ".scala")):
                module_like += 1
        return module_like >= max(1, len(entities) // 2)

    def _infer_repo_profile(self, answers: list[DayOneAnswer]) -> str:
        sql_files = 0
        py_files = 0
        for answer in answers:
            for item in answer.evidence:
                source_file = str(item.get("source_file", "")).strip().lower()
                if source_file.endswith(".sql"):
                    sql_files += 1
                elif source_file.endswith(".py"):
                    py_files += 1
        total = sql_files + py_files
        if total <= 0:
            return "mixed"
        sql_ratio = sql_files / total
        py_ratio = py_files / total
        if sql_ratio >= 0.6:
            return "sql_heavy"
        if py_ratio >= 0.6:
            return "python_heavy"
        return "mixed"
