# Brownfield Cartographer Final Report

## 1. Reconnaissance Summary

Source: `RECONNAISSANCE.md`

## 2. Final Architecture

Pipeline:

1. Surveyor (structural graph + complexity + velocity)
2. Hydrologist (lineage DAG from SQL/Python/YAML/notebooks)
3. Semanticist (purpose, drift, domain clusters, Day-One synthesis)
4. Archivist (living artifacts + trace)
5. Navigator (tool-based query interface)

## 3. Accuracy Analysis

### Correct Day-One Outputs

- [Fill after running on jaffle_shop and Airflow examples]

### Incorrect/Partial Outputs

- [Fill with concrete misses]

### Root Causes

- [Parser limitation, dynamic references, unsupported framework constructs, etc.]

## 4. Limitations

- Dynamic SQL and string-built paths remain partially unresolved.
- Column-level lineage is not fully implemented.
- LLM synthesis quality depends on local Ollama model availability and model choice.

## 5. FDE Applicability

This system is deployable on Day 1 of a brownfield engagement to bootstrap architectural context, identify high-risk modules, and answer immediate data lineage questions with explicit evidence traces.

## 6. Self-Audit

- Target: Week 1 repo (path)
- Comparison baseline: `ARCHITECTURE_NOTES.md`
- Key discrepancies:
- [Fill with real observed mismatches]
