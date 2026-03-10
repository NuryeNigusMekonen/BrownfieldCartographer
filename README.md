# Brownfield Cartographer

Engineering codebase intelligence for rapid brownfield FDE onboarding. 

## Features

- Surveyor agent: structural module map, import graph, function nodes, call edges, complexity, git velocity.
- Hydrologist agent: mixed lineage extraction across Python, SQL, YAML, and notebooks.
- Semanticist agent: purpose statements, doc drift checks, domain clustering, Day-One synthesis.
- Archivist agent: `CODEBASE.md`, `onboarding_brief.md`, `module_graph.json`, `lineage_graph.json`, semantic index, trace log.
- Navigator agent: `find_implementation`, `trace_lineage`, `blast_radius`, `explain_module` tools with evidence.
- Incremental mode: changed-file re-analysis with git diff fallback to filesystem mtime.
- Repository source support: local path or GitHub URL.

## Install

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install networkx pydantic pyyaml sqlglot typer rich scikit-learn
```

## Analyze

Local repo:

```bash
. .venv/bin/activate
python -m src.cli analyze /path/to/repo --output .cartography
```

GitHub repo:

```bash
. .venv/bin/activate
python -m src.cli analyze https://github.com/dbt-labs/jaffle_shop --checkout-root /tmp/cartographer_repos
```

## Query

```bash
. .venv/bin/activate
python -m src.cli query /path/to/repo explain_module src/orchestrator.py
python -m src.cli query /path/to/repo find_implementation revenue
python -m src.cli query /path/to/repo trace_lineage dataset::orders --direction upstream
python -m src.cli query /path/to/repo blast_radius src/transforms/revenue.py
```

## Output Artifacts

`.cartography/` contains:

- `module_graph.json`
- `lineage_graph.json`
- `semantic_index/module_purpose_index.jsonl`
- `CODEBASE.md`
- `onboarding_brief.md`
- `cartography_trace.jsonl`
- `state.json`

## Optional LLM Integration (Local Ollama)

Set environment variables to use local Ollama models (no paid API key required):

```bash
export OLLAMA_HOST=http://127.0.0.1:11434
export CARTOGRAPHER_MODEL_FAST=llama3.2:3b
export CARTOGRAPHER_MODEL_SYNTH=llama3.1:8b
export CARTOGRAPHER_EMBED_MODEL=nomic-embed-text
```

If Ollama/model availability is missing, the Semanticist falls back to deterministic local heuristics.
