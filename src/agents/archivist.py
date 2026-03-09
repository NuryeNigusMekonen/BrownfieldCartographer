from __future__ import annotations

import json
from pathlib import Path

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
                f.write(json.dumps(event.model_dump()) + "\n")
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
    ) -> Path:
        path = self.out_dir / "CODEBASE.md"
        by_velocity = sorted(modules.values(), key=lambda m: m.change_velocity_30d, reverse=True)[:10]
        debt_lines = [f"- Circular dependency: {', '.join(comp)}" for comp in scc]
        doc_drift = [m.path for m in modules.values() if "Documentation Drift" in m.purpose_statement]
        debt_lines.extend([f"- Documentation drift: {d}" for d in doc_drift[:10]])
        if not debt_lines:
            debt_lines = ["- No major structural debt flags detected."]

        content = [
            "# CODEBASE",
            "",
            "## Architecture Overview",
            "This repository was analyzed by Brownfield Cartographer to produce a structural module map and mixed-lineage view.",
            "",
            "## Critical Path",
            *[f"- {m}" for m in top_modules[:5]],
            "",
            "## Data Sources",
            *[f"- {s}" for s in sources[:20]],
            "",
            "## Data Sinks",
            *[f"- {s}" for s in sinks[:20]],
            "",
            "## Known Debt",
            *debt_lines,
            "",
            "## High-Velocity Files",
            *[f"- {m.path} ({m.change_velocity_30d} commits/30d)" for m in by_velocity],
            "",
            "## Module Purpose Index",
            *[f"- {m.path}: {m.purpose_statement}" for m in list(modules.values())[:200]],
        ]
        path.write_text("\n".join(content), encoding="utf-8")
        return path

    def generate_onboarding_brief(self, day_one_answers: dict[str, DayOneAnswer]) -> Path:
        path = self.out_dir / "onboarding_brief.md"
        q1 = day_one_answers.get("q1_primary_ingestion", DayOneAnswer(question_id="q1", answer=""))
        q2 = day_one_answers.get("q2_critical_outputs", DayOneAnswer(question_id="q2", answer=""))
        q3 = day_one_answers.get("q3_blast_radius", DayOneAnswer(question_id="q3", answer=""))
        q4 = day_one_answers.get("q4_logic_concentration", DayOneAnswer(question_id="q4", answer=""))
        q5 = day_one_answers.get("q5_change_velocity", DayOneAnswer(question_id="q5", answer=""))
        lines = [
            "# FDE Day-One Brief",
            "",
            "## 1) Primary Data Ingestion Path",
            q1.answer,
            "",
            "Evidence:",
            f"- {json.dumps(q1.evidence)}",
            "",
            "## 2) Critical Output Datasets/Endpoints",
            q2.answer,
            "",
            "Evidence:",
            f"- {json.dumps(q2.evidence)}",
            "",
            "## 3) Blast Radius of Critical Module Failure",
            q3.answer,
            "",
            "Evidence:",
            f"- {json.dumps(q3.evidence)}",
            "",
            "## 4) Business Logic Concentration",
            q4.answer,
            "",
            "Evidence:",
            f"- {json.dumps(q4.evidence)}",
            "",
            "## 5) Change Velocity (Last 90-ish Days Proxy)",
            q5.answer,
            "",
            "Evidence:",
            f"- {json.dumps(q5.evidence)}",
        ]
        path.write_text("\n".join(lines), encoding="utf-8")
        return path
