# RECONNAISSANCE

## Phase 0 Target Selection

- Primary target: `test_repos/https-github.com-meltano-meltano.git-38a4d2db` (Meltano OSS codebase).
- Why this target: larger and more realistic brownfield surface than `jaffle_shop`, with mixed orchestration/state/plugin concerns.
- Repository size observed:
  - tracked files: 1033 (`git ls-files | wc -l`)
  - files on disk: 1070 (`find ... -type f | wc -l`)
- Dominant artifact types (top): Python (316), YAML/YML (71), JS (82), Markdown (74), plus lock/docs/assets.
- Important constraint: clone was `--depth 1`, so commit-history/velocity analysis is low-confidence.

## Manual Recon Window

- Approximate manual exploration duration: 45+ minutes.
- Files inspected by hand:
  - `README.md`
  - `src/meltano/cli/__init__.py`
  - `src/meltano/cli/cli.py`
  - `src/meltano/cli/run.py`
  - `src/meltano/core/block/block_parser.py`
  - `src/meltano/core/block/extract_load.py`
  - `src/meltano/core/runner/singer.py`
  - `src/meltano/core/state_service.py`
  - `src/meltano/core/state_store/base.py`
  - recent git log metadata

## Five FDE Day-One Questions (Manual Answers)

1. What is the primary ingestion path?
- In this codebase, ingestion is command-driven EL execution:
  - CLI entrypoint registers `run` (`src/meltano/cli/__init__.py`).
  - top-level CLI resolves project context/environment (`src/meltano/cli/cli.py`).
  - `meltano run` parses blocks/jobs into executable block sets (`src/meltano/cli/run.py`, `src/meltano/core/block/block_parser.py`).
  - `ExtractLoadBlocks` orchestrates extractor/loader execution and state behavior (`src/meltano/core/block/extract_load.py`).
  - `SingerRunner` streams extractor output into loader stdin (`src/meltano/core/runner/singer.py`).
- Practical interpretation: the ingestion boundary is plugin-based (Singer taps, loaders, and command blocks), not a fixed in-repo table list.

2. What are the 3-5 most critical outputs?
- `meltano run` execution outcomes (success/failure of pipeline block sets) via CLI run path (`src/meltano/cli/run.py`).
- Persisted run state used for incremental processing and recovery (`src/meltano/core/state_service.py`).
- State backend writes across configured stores (filesystem/system DB/cloud backends) via the state-store abstraction (`src/meltano/core/state_store/base.py` and backend modules).
- Plugin installation side effects and runnable environments (venv/install path) from plugin install service (`src/meltano/core/plugin_install_service.py`).
- Structured logs/telemetry and command lifecycle events (tracking/logging flows through CLI/core services).

3. If one critical module fails, what is the blast radius?
- If `src/meltano/core/block/extract_load.py` fails, the blast radius is high:
  - EL block execution fails for `meltano run`.
  - Singer tap->target orchestration may not execute or terminate correctly.
  - state updates/checkpoint behavior can become inconsistent.
  - downstream user commands depending on successful runs (schedules/jobs/state ops) degrade.
- If `src/meltano/core/state_service.py` or state backend manager fails:
  - incremental/stateful run guarantees degrade.
  - replay/recovery behavior is impacted across environments.

4. Is business logic concentrated or distributed?
- Distributed with a few concentration hubs:
  - CLI orchestration and command semantics in `src/meltano/cli/*`.
  - block parsing/execution and runner orchestration in `src/meltano/core/block/*` + `src/meltano/core/runner/*`.
  - plugin lifecycle in `src/meltano/core/plugin_*` and `src/meltano/core/plugin/*`.
  - state and persistence strategy in `src/meltano/core/state_*`.
- This is a platform-style codebase; logic is spread across services rather than concentrated in one or two transform files.

5. What files change most often (90-day velocity map)?
- Most recent commit in local clone: `a6821df` on `2026-03-09` (`chore: add stbiadmin as a contributor for code (#9897)`).
- 90-day velocity output is currently not trustworthy for ranking hotspots because clone depth is 1, so many files appear with count `1`.
- Confidence note: for production-grade velocity maps, fetch full history (or at least a deep enough window) before using churn as a risk signal.

## Manual Difficulty Analysis

## Hardest to infer manually

- Distinguishing framework internals from user-facing operational critical paths in a large platform repository.
- Mapping "data lineage" expectations to plugin-mediated runtime flows (rather than static SQL DAGs).
- Isolating meaningful churn signals with shallow git history.

## Where I got lost

- Initial ambiguity between Meltano-the-platform code and example-library/integration project configs in the same repo.
- Early analyzer outputs over-emphasized high-churn config files (`.grype.yaml`, CI YAML) over runtime core modules, requiring manual correction.
- The generated lineage graph contained many dynamic unresolved references, so manual interpretation was necessary.

## Architecture priorities implied by this recon

- Prioritize robust Python call/import graph quality for large multi-package repos before trusting "critical path" ranking.
- Improve runtime-centric filtering (exclude CI/docs scaffolding by default for critical-path scoring).
- Strengthen plugin/runtime lineage extraction:
  - command -> block parser -> block executor -> runner -> state store
  - include plugin type semantics (extractor/loader/mapper/utility) in lineage nodes.
- Attach confidence metadata to velocity and lineage when history is shallow or dynamic references are unresolved.
