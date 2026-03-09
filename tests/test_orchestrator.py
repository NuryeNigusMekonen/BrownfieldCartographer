from __future__ import annotations

import time
from pathlib import Path

from src.orchestrator import CartographyOrchestrator


def test_orchestrator_incremental_detects_changed_files(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    orchestrator.analyze(incremental=False)

    time.sleep(0.01)
    target = mini_repo_copy / "helpers.py"
    target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    changed = orchestrator.changed_files_since_last_run()
    assert "helpers.py" in changed


def test_orchestrator_incremental_updates_trace(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    orchestrator = CartographyOrchestrator(repo_path=mini_repo_copy, out_dir=mini_repo_copy / ".cartography")
    orchestrator.analyze(incremental=False)

    time.sleep(0.01)
    target = mini_repo_copy / "pipeline.py"
    target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    orchestrator.analyze(incremental=True)
    trace_text = (mini_repo_copy / ".cartography" / "cartography_trace.jsonl").read_text(encoding="utf-8")
    assert '"action": "incremental_update"' in trace_text
