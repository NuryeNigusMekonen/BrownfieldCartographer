# Brownfield Cartographer Final Report

## 1. Reconnaissance Summary

Primary target: `test_repos/https-github.com-meltano-meltano.git-38a4d2db` (Meltano).

Recon basis:
- Manual inspection of core runtime paths:
  - `src/meltano/cli/__init__.py`
  - `src/meltano/cli/cli.py`
  - `src/meltano/cli/run.py`
  - `src/meltano/core/block/block_parser.py`
  - `src/meltano/core/block/extract_load.py`
  - `src/meltano/core/runner/singer.py`
  - `src/meltano/core/state_service.py`
  - `src/meltano/core/state_store/base.py`
- Automated artifacts generated under:
  - `test_repos/https-github.com-meltano-meltano.git-38a4d2db/.cartography/`

Scale observed in this run (from trace):
- modules analyzed: 469
- functions indexed: 530
- module graph edges: 733
- lineage graph nodes/edges: 30/29
- unresolved dynamic lineage references: 47

## 2. Final Architecture

Pipeline implemented:

1. Surveyor (structural graph + complexity + velocity)
2. Hydrologist (lineage DAG from SQL/Python/YAML/notebooks)
3. Semanticist (purpose, drift, domain clusters, Day-One synthesis)
4. Archivist (living artifacts + trace)
5. Navigator (tool-based query interface)

Delivered artifacts for Meltano:
- `module_graph.json`
- `lineage_graph.json`
- `semantic_index/module_purpose_index.jsonl`
- `CODEBASE.md`
- `onboarding_brief.md`
- `cartography_trace.jsonl`
- `state.json`

## 3. Accuracy Analysis

### Correct Day-One Outputs

- The system correctly identified a plugin/CLI-driven ingestion model (not static SQL-table ingestion), with relevant evidence in:
  - `src/meltano/cli/config.py`
  - `src/meltano/cli/state.py`
  - `src/meltano/core/block/extract_load.py`
  - `src/meltano/core/db.py`
- The lineage build successfully crossed Python and config files at scale (316 Python + 71 YAML files processed).
- Evidence payloads were attached in `onboarding_brief.md` and `cartography_trace.jsonl`.

### Incorrect/Partial Outputs

- Critical output detection is weak for Meltano: reported sink is mostly `dataset::dynamic reference, cannot resolve`.
- Critical path ranking is skewed toward repo meta/config files (for example `.grype.yaml`, CI files) rather than runtime core modules.
- Blast-radius answer in onboarding brief is misleading for this target (`.grype.yaml` as critical module).
- Velocity signals are low-confidence due to shallow clone history.

### Root Causes

- Dynamic references in Python dataflow (f-strings, runtime-constructed values, plugin-mediated IO).
- Structural scoring currently does not filter non-runtime files (CI/workflow/meta/docs) from "critical path" ranking.
- Import edge resolution is not fully normalized for symbol-qualified imports, reducing module dependency fidelity.
- Shallow git history (`--depth 1`) degrades churn/velocity ranking accuracy.

## 4. Limitations

- Dynamic SQL and runtime-constructed IO remain partially unresolved.
- Column-level lineage is not implemented.
- Function/class-level semantic indexing is not fully delivered (current output is module-level semantic index).
- Generated visual system map is not yet first-class auto-artifact (manual `SYSTEM_MAP.md` currently used).
- LLM synthesis depends on local Ollama availability; fallback heuristics reduce semantic depth.

## 5. FDE Applicability

For a brownfield FDE onboarding, the tool is already useful for:
- fast structural inventory of large repos,
- mixed-source lineage extraction,
- evidence-attached onboarding briefs.

However, on platform-style repos like Meltano, it needs runtime-centric ranking and stronger dynamic-resolution handling before treating "critical path" answers as high-confidence.

## 6. Self-Audit

- Target used: `test_repos/https-github.com-meltano-meltano.git-38a4d2db`
- Baseline used:
  - manual recon in `RECONNAISSANCE.md`
  - generated artifacts in target `.cartography/`
- Key discrepancies found:
  - onboarding brief over-prioritized config/meta files for critical path,
  - dynamic lineage resolution produced unresolved sink output,
  - velocity interpretation was constrained by shallow clone history.

Priority fixes for production quality:
1. Runtime-focused criticality scoring (exclude/meta-weight docs+CI paths).
2. Better dependency graph normalization for imports and symbol paths.
3. Stronger dynamic-reference handling in Python dataflow.
4. Auto-generate visual `SYSTEM_MAP` as a formal artifact.
