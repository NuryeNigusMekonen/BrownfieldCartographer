from __future__ import annotations

import json
from pathlib import Path

from tests.conftest import GOLDEN, normalized_artifact_summary, run_analysis


def test_jaffle_shop_artifact_snapshot(jaffle_repo_copy: Path, deterministic_semantics: None) -> None:
    out_dir = run_analysis(jaffle_repo_copy)
    observed = normalized_artifact_summary(out_dir)
    expected = json.loads((GOLDEN / "jaffle_shop_artifact_summary.json").read_text(encoding="utf-8"))
    assert observed == expected


def test_mini_repo_artifact_snapshot(mini_repo_copy: Path, deterministic_semantics: None) -> None:
    out_dir = run_analysis(mini_repo_copy)
    observed = normalized_artifact_summary(out_dir)
    expected = json.loads((GOLDEN / "mini_repo_artifact_summary.json").read_text(encoding="utf-8"))
    assert observed == expected
