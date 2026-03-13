# FDE Day-One Brief

## 1) Primary Data Ingestion Path
Available analysis signals indicate ingestion through entrypoint modules before downstream orchestration and transformation steps.

Key sources:
- src/meltano/cli/config.py
- src/meltano/cli/state.py
- src/meltano/cli/elt.py
- src/meltano/cli/job.py
- src/meltano/cli/select_entities.py

Confidence: low (score: 0.44)
Confidence label: low
Confidence factors: evidence_count=0.44, evidence_diversity=0.29, graph_coverage=0.55, heuristic_reliability=0.35, signal_agreement=0.30, repo_type_fit=0.85
Confidence reason: Confidence is low (0.44) because ingestion is inferred from CLI/orchestration entrypoint heuristics with limited lineage cross-validation.

Evidence:
- [{"analysis_method": "module_entrypoint_ingestion_heuristic", "source_file": "src/meltano/cli/config.py", "line_range": [1, 1]}, {"analysis_method": "module_entrypoint_ingestion_heuristic", "source_file": "src/meltano/cli/state.py", "line_range": [1, 1]}, {"analysis_method": "module_entrypoint_ingestion_heuristic", "source_file": "src/meltano/cli/elt.py", "line_range": [1, 1]}, {"analysis_method": "module_entrypoint_ingestion_heuristic", "source_file": "src/meltano/cli/job.py", "line_range": [1, 1]}, {"analysis_method": "module_entrypoint_ingestion_heuristic", "source_file": "src/meltano/cli/select_entities.py", "line_range": [1, 1]}]

## 2) Critical Output Datasets/Endpoints
Critical outputs are analytics/reporting datasets such as No obvious output dataset detected., which serve dashboards and recurring business reporting.

Key outputs:
- No obvious output dataset detected.

Confidence: high (score: 0.91)
Confidence label: high
Confidence factors: evidence_count=0.95, evidence_diversity=0.85, graph_coverage=0.95, heuristic_reliability=0.90, signal_agreement=0.95, repo_type_fit=0.80
Confidence reason: Confidence is high (0.91) because lineage graph coverage is complete and consistently shows no terminal output datasets.

Evidence:
- [{"analysis_method": "lineage_graph_sinks", "source_file": "", "line_range": [0, 0]}]

## 3) Blast Radius of Critical Module Failure
If src/meltano/core/plugin_invoker.py fails, at least 147 downstream modules are in the dependency path based on the module dependency graph.

Key modules:
- src/meltano/core/plugin_invoker.py

Confidence: medium (score: 0.69)
Confidence label: medium
Confidence factors: evidence_count=0.29, evidence_diversity=0.49, graph_coverage=1.00, heuristic_reliability=0.86, signal_agreement=1.00, repo_type_fit=0.90
Confidence reason: Confidence is medium (0.69) because graph_coverage provides the strongest support while evidence_count is the primary limiting factor.

Evidence:
- [{"analysis_method": "module_graph_descendants", "source_file": "src/meltano/core/plugin_invoker.py", "line_range": [1, 1]}]

## 4) Business Logic Concentration
Business logic is concentrated in files such as src/meltano/core/settings_store.py, src/meltano/core/plugin/singer/tap.py. These files drive orchestration behavior, connector execution, and pipeline control flow.

Key files:
- src/meltano/core/settings_store.py
- src/meltano/core/plugin/singer/tap.py
- src/meltano/core/plugin/singer/catalog.py
- src/meltano/cli/config.py
- src/meltano/core/block/extract_load.py

Confidence: high (score: 0.91)
Confidence label: high
Confidence factors: evidence_count=1.00, evidence_diversity=0.70, graph_coverage=1.00, heuristic_reliability=0.88, signal_agreement=1.00, repo_type_fit=0.90
Confidence reason: Confidence is high (0.91) because evidence_count provides the strongest support while evidence_diversity is the primary limiting factor.

Evidence:
- [{"analysis_method": "complexity_and_velocity_signals", "source_file": "src/meltano/core/settings_store.py", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/meltano/core/plugin/singer/tap.py", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/meltano/core/plugin/singer/catalog.py", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/meltano/cli/config.py", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/meltano/core/block/extract_load.py", "line_range": [1, 1]}]

## 5) Recent High Velocity Areas
Recent development activity is concentrated in these files, making them the fastest-moving areas for onboarding and change-risk monitoring.

Key files:
- src/meltano/core/settings_store.py
- src/meltano/core/plugin/singer/tap.py
- src/meltano/cli/config.py
- src/meltano/core/plugin/singer/catalog.py
- src/meltano/core/block/extract_load.py

Confidence: medium (score: 0.64)
Confidence label: medium
Confidence factors: evidence_count=0.82, evidence_diversity=0.41, graph_coverage=0.58, heuristic_reliability=0.55, signal_agreement=0.60, repo_type_fit=0.90
Confidence reason: Confidence is medium (0.64) because the result is derived from git-frequency signals only, with limited corroborating structural evidence.

Evidence:
- [{"analysis_method": "git_log_frequency", "source_file": "src/meltano/core/settings_store.py", "line_range": [1, 1]}, {"analysis_method": "git_log_frequency", "source_file": "src/meltano/core/plugin/singer/tap.py", "line_range": [1, 1]}, {"analysis_method": "git_log_frequency", "source_file": "src/meltano/cli/config.py", "line_range": [1, 1]}, {"analysis_method": "git_log_frequency", "source_file": "src/meltano/core/plugin/singer/catalog.py", "line_range": [1, 1]}, {"analysis_method": "git_log_frequency", "source_file": "src/meltano/core/block/extract_load.py", "line_range": [1, 1]}]
