from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pytest

from src.orchestrator import CartographyOrchestrator


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
GOLDEN = ROOT / "tests" / "golden"


@pytest.fixture
def jaffle_repo_copy(tmp_path: Path) -> Path:
    src = ROOT / "test_repos" / "jaffle_shop"
    dst = tmp_path / "jaffle_shop"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns(".cartography", ".git"))
    return dst


@pytest.fixture
def jaffle_shop_hyphen_repo_copy(tmp_path: Path) -> Path:
    src = ROOT / "test_repos" / "jaffle-shop"
    dst = tmp_path / "jaffle-shop"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns(".cartography", ".git"))
    return dst


@pytest.fixture
def mini_repo_copy(tmp_path: Path) -> Path:
    src = FIXTURES / "mini_repo"
    dst = tmp_path / "mini_repo"
    shutil.copytree(src, dst)
    return dst


@pytest.fixture
def deterministic_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.agents.semanticist.SemanticistAgent._ollama_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr("src.agents.semanticist.SemanticistAgent._embed_texts", lambda *args, **kwargs: None)


def run_analysis(repo_path: Path) -> Path:
    out_dir = repo_path / ".cartography"
    orchestrator = CartographyOrchestrator(repo_path=repo_path, out_dir=out_dir)
    orchestrator.analyze(incremental=False)
    return out_dir


def normalized_artifact_summary(out_dir: Path) -> dict:
    module_graph = json.loads((out_dir / "module_graph.json").read_text(encoding="utf-8"))
    lineage_graph = json.loads((out_dir / "lineage_graph.json").read_text(encoding="utf-8"))
    codebase = (out_dir / "CODEBASE.md").read_text(encoding="utf-8")
    onboarding = (out_dir / "onboarding_brief.md").read_text(encoding="utf-8")
    semantic_lines = [
        json.loads(line)
        for line in (out_dir / "semantic_index" / "module_purpose_index.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    trace_lines = [
        json.loads(line)
        for line in (out_dir / "cartography_trace.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    normalized_nodes = []
    for node in module_graph["nodes"]:
        if node.get("node_type") != "module":
            continue
        normalized_nodes.append(
            {
                "id": node["id"],
                "language": node.get("language"),
                "imports": sorted(node.get("imports", [])),
                "public_functions": sorted(node.get("public_functions", [])),
                "classes": sorted(node.get("classes", [])),
                "high_velocity": bool(node.get("is_high_velocity_core", False)),
            }
        )

    normalized_lineage_edges = []
    for edge in lineage_graph["edges"]:
        source = re.sub(r"transform::([^:]+)::\d+", r"transform::\1::HASH", edge["source"])
        target = re.sub(r"transform::([^:]+)::\d+", r"transform::\1::HASH", edge["target"])
        normalized_lineage_edges.append(
            {
                "source": source,
                "target": target,
                "edge_type": edge.get("edge_type"),
                "analysis_method": edge.get("analysis_method"),
                "source_file": edge.get("source_file", ""),
            }
        )

    return {
        "module_nodes": sorted(normalized_nodes, key=lambda item: item["id"]),
        "lineage_edges": sorted(
            normalized_lineage_edges,
            key=lambda item: (item["source"], item["target"], item["edge_type"] or "", item["analysis_method"] or ""),
        ),
        "codebase_sections": [
            line.strip()
            for line in codebase.splitlines()
            if line.startswith("## ")
        ],
        "onboarding_sections": [
            line.strip()
            for line in onboarding.splitlines()
            if line.startswith("## ")
        ],
        "semantic_index_paths": sorted([entry["path"] for entry in semantic_lines]),
        "trace_actions": [f"{entry['agent']}:{entry['action']}" for entry in trace_lines],
    }
