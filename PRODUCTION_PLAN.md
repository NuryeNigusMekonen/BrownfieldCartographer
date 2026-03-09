# Brownfield Cartographer Production Plan

## Objective

Move Brownfield Cartographer from challenge-complete prototype to production-grade internal tooling for ongoing FDE engagements.

Production-grade in this context means:

- deterministic enough to trust in daily use
- observable enough to debug when wrong
- fast enough to run on real brownfield repositories repeatedly
- explicit about confidence, limitations, and failure modes
- operable with local Ollama models and without paid API dependencies

## Current State

What is already in place:

- end-to-end pipeline across Surveyor, Hydrologist, Semanticist, Archivist, and Navigator
- structural graph, lineage graph, semantic summaries, CODEBASE context, onboarding brief, and trace log
- incremental mode based on git diff with mtime fallback
- local Ollama-based semantic path
- LangGraph-backed Navigator workflow

What still blocks production use:

- limited automated test coverage
- weak parser regression protection for real-world repo edge cases
- minimal structured observability outside `cartography_trace.jsonl`
- incremental mode is file-based, not dependency-aware
- semantic outputs do not yet enforce strong evidence quality guarantees
- large-repo performance and correctness are not benchmarked against real targets

## P0

P0 is the minimum bar before calling the tool production-ready for internal use.

### 1. Test Harness and Golden Fixtures

Deliverables:

- add unit tests for each analyzer and agent
- add golden test fixtures for:
- Python imports, classes, inheritance, decorated functions
- SQL CTEs, dbt refs/sources, insert/create/merge statements
- Airflow DAG edges and dbt schema dependencies
- Python dataflow edge cases: f-strings, variable references, unresolved dynamic refs
- add end-to-end snapshot tests for generated artifacts on at least two repos

Exit criteria:

- `pytest` passes locally and in CI
- artifact snapshots only change intentionally
- regressions in graphs or semantic outputs are caught automatically

Suggested code areas:

- `src/analyzers/`
- `src/agents/`
- `src/orchestrator.py`

### 2. Structured Error Handling and Failure Reporting

Deliverables:

- define parser/semantic/orchestration exception types
- surface partial-failure summaries in trace output
- never fail whole runs because one file cannot be parsed
- attach explicit failure evidence per file: parser, reason, confidence impact

Exit criteria:

- malformed files degrade gracefully
- failed files appear in trace and summary artifacts
- no silent parser fallbacks without evidence

Suggested code areas:

- `src/analyzers/tree_sitter_analyzer.py`
- `src/analyzers/sql_lineage.py`
- `src/analyzers/python_dataflow.py`
- `src/agents/semanticist.py`

### 3. Observability Baseline

Deliverables:

- add structured application logs in addition to `cartography_trace.jsonl`
- emit counters for:
- files analyzed
- files skipped
- parse failures
- unresolved dynamic references
- Ollama call failures
- semantic fallback rate
- total runtime per agent

Exit criteria:

- each run can answer “what failed?”, “where?”, and “how much degraded?”
- a large-repo run can be profiled from logs alone

Suggested code areas:

- `src/orchestrator.py`
- `src/agents/*.py`

### 4. Ollama Runtime Validation

Deliverables:

- validate Ollama host and required models at startup
- add a `doctor` or `healthcheck` CLI command
- fail fast when configured models are unavailable
- make fallback behavior explicit and visible in trace output

Exit criteria:

- users can verify environment readiness before running analysis
- missing local models do not produce ambiguous semantic quality

Suggested code areas:

- `src/cli.py`
- `src/agents/semanticist.py`

## P1

P1 makes the system robust on larger and messier brownfield repositories.

### 5. Dependency-Aware Incremental Analysis

Deliverables:

- extend incremental mode to re-analyze impacted dependents, not only changed files
- when a SQL model changes, re-analyze downstream lineage dependents
- when a Python module changes, re-analyze import neighbors and affected semantic summaries
- cache previous per-file analysis outputs to avoid full regeneration

Exit criteria:

- incremental runs are materially faster than full runs on large repos
- changed-file analysis does not leave stale downstream artifacts

Suggested code areas:

- `src/orchestrator.py`
- `src/graph/knowledge_graph.py`

### 6. Real-Repo Benchmark Suite

Deliverables:

- benchmark on at least three real repos:
- Apache Airflow
- Meltano
- a medium-sized dbt repo
- record runtime, memory, unresolved refs, parse failure rate, and graph sizes
- capture qualitative false positives/false negatives

Exit criteria:

- benchmark results checked into repo docs
- performance and correctness bottlenecks are measurable, not anecdotal

Suggested output:

- `benchmarks/README.md`
- `benchmarks/results/*.json`

### 7. Evidence Quality Hardening

Deliverables:

- require all Navigator answers to carry evidence from graph edges or explicit semantic synthesis
- distinguish evidence classes:
- static analysis
- configuration parsing
- lineage inference
- LLM synthesis
- enforce file path + line range presence where technically available

Exit criteria:

- no user-facing answer returns bare claims without provenance
- trust level is obvious from the response payload

Suggested code areas:

- `src/agents/navigator.py`
- `src/agents/semanticist.py`
- `src/agents/archivist.py`

### 8. Semantic Evaluation Loop

Deliverables:

- build a small evaluation set for:
- module purpose quality
- doc drift precision
- Day-One answer usefulness
- compare Ollama model variants for fast vs synth roles
- store evaluation artifacts and scoring notes

Exit criteria:

- model selection is justified with repo-local evidence
- semantic regressions are visible over time

## P2

P2 is polish, scale, and platform maturity.

### 9. Packaging and Release Discipline

Deliverables:

- add CI for test, lint, type-check, and packaging
- pin production dependency strategy
- publish repeatable install/run instructions
- add versioned release notes

Exit criteria:

- a clean environment can install and run the tool reproducibly
- releases are auditable

### 10. Config System

Deliverables:

- support project config file for:
- ignore paths
- enabled analyzers
- Ollama models
- clustering `k`
- trace verbosity
- repo-specific parser overrides

Exit criteria:

- users do not need to edit code for normal operational customization

### 11. Scale and Caching

Deliverables:

- cache embeddings and semantic summaries
- cache tree-sitter parse outputs where safe
- parallelize analysis where correctness allows
- optimize serialization for large graphs

Exit criteria:

- large repositories complete in predictable time
- repeated runs avoid recomputing stable work

### 12. Security and Multi-Tenancy Readiness

Deliverables:

- sanitize logged content
- bound file sizes and prompt sizes
- define safe handling for secrets accidentally present in repos
- document workspace and model-access assumptions

Exit criteria:

- trace/log outputs do not become accidental data leaks
- local-model usage has clear operating boundaries

## Milestones

### Milestone A

Goal:
- internal beta on real repos

Requires:
- all P0 items complete

### Milestone B

Goal:
- reliable repeated use across multiple brownfield engagements

Requires:
- all P1 items complete

### Milestone C

Goal:
- production-grade maintained toolchain

Requires:
- core P2 items complete

## Recommended Implementation Order

1. test harness and golden fixtures
2. structured error handling
3. observability baseline
4. Ollama runtime validation
5. dependency-aware incremental updates
6. evidence quality hardening
7. benchmark suite
8. semantic evaluation
9. packaging/config/caching/security work

## Definition of Done

You can call Brownfield Cartographer production-grade when all of the following are true:

- end-to-end tests pass on multiple real repositories
- incremental mode is dependency-aware and benchmarked
- all user-facing answers include provenance with confidence semantics
- parse failures and semantic fallbacks are observable and non-silent
- Ollama runtime requirements are validated before execution
- the system has documented operational limits and reproducible setup
