# FDE Day-One Brief

## 1) Primary Data Ingestion Path
Data enters through source-aligned datasets such as ol_warehouse_raw_data.raw__edxorg__s3__tracking_logs, ol_warehouse_raw_data.raw__edxorg__s3__mitx_course_run, then flows through staging, intermediate, dimensional, and reporting models into analytics-ready reporting tables.

Key sources:
- ol_warehouse_raw_data.raw__edxorg__s3__tracking_logs
- ol_warehouse_raw_data.raw__edxorg__s3__mitx_course_run
- ol_warehouse_raw_data.raw__edxorg__s3__mitx_course
- ol_warehouse_raw_data.raw__mitxonline__openedx__tracking_logs
- ol_warehouse_raw_data.raw__emeritus__bigquery__api_enrollments

Confidence: high (score: 0.86)
Confidence label: high
Confidence factors: evidence_count=1.00, evidence_diversity=0.52, graph_coverage=1.00, heuristic_reliability=0.92, signal_agreement=0.70, repo_type_fit=1.00
Confidence reason: Confidence is high (0.86) because evidence_count provides the strongest support while evidence_diversity is the primary limiting factor.

Evidence:
- [{"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/staging/mitxonline/stg__mitxonline__openedx__tracking_logs__user_activity.sql", "line_range": [1, 56]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/staging/edxorg/stg__edxorg__s3__tracking_logs__user_activity.sql", "line_range": [1, 52]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/staging/edxorg/stg__edxorg__api__course.sql", "line_range": [1, 29]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/staging/edxorg/stg__edxorg__api__courserun.sql", "line_range": [1, 56]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/staging/mitxpro/stg__emeritus__api__bigquery__user_enrollments.sql", "line_range": [1, 59]}]

## 2) Critical Output Datasets/Endpoints
Critical outputs are analytics/reporting datasets such as instructor_module_report, program_enrollment_with_user_report, which serve dashboards and recurring business reporting.

Key outputs:
- instructor_module_report
- program_enrollment_with_user_report
- enrollment_detail_report
- student_risk_probability_report
- Enrollment_Activity_Counts_Dataset

Confidence: high (score: 0.84)
Confidence label: high
Confidence factors: evidence_count=1.00, evidence_diversity=0.52, graph_coverage=0.72, heuristic_reliability=0.92, signal_agreement=1.00, repo_type_fit=1.00
Confidence reason: Confidence is high (0.84) because evidence_count provides the strongest support while evidence_diversity is the primary limiting factor.

Evidence:
- [{"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/reporting/instructor_module_report.sql", "line_range": [1, 235]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/reporting/student_risk_probability_report.sql", "line_range": [1, 32]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/reporting/enrollment_detail_report.sql", "line_range": [1, 95]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/reporting/program_enrollment_with_user_report.sql", "line_range": [1, 115]}, {"analysis_method": "sqlglot", "source_file": "src/ol_dbt/models/reporting/Enrollment_Activity_Counts_Dataset.sql", "line_range": [1, 141]}]

## 3) Blast Radius of Critical Module Failure
If dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/dagster_assets.py fails, at least 2 downstream modules are in the dependency path based on the module dependency graph.

Key modules:
- dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/dagster_assets.py

Confidence: medium (score: 0.58)
Confidence label: medium
Confidence factors: evidence_count=0.29, evidence_diversity=0.49, graph_coverage=0.57, heuristic_reliability=0.86, signal_agreement=1.00, repo_type_fit=0.70
Confidence reason: Confidence is medium (0.58) because signal_agreement provides the strongest support while evidence_count is the primary limiting factor.

Evidence:
- [{"analysis_method": "module_graph_descendants", "source_file": "dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/dagster_assets.py", "line_range": [1, 1]}]

## 4) Business Logic Concentration
Business logic is concentrated in files such as src/ol_dbt/models/marts/combined/marts__combined_program_enrollment_detail.sql, src/ol_dbt/models/marts/combined/marts__combined_course_enrollment_detail.sql. These files define warehouse transformations and reporting model logic.

Key files:
- src/ol_dbt/models/marts/combined/marts__combined_program_enrollment_detail.sql
- src/ol_dbt/models/marts/combined/marts__combined_course_enrollment_detail.sql
- src/ol_dbt/models/dimensional/dim_user.sql
- bin/dbt-local-dev.py
- src/ol_dbt/models/reporting/organization_administration_report.sql

Confidence: high (score: 0.88)
Confidence label: high
Confidence factors: evidence_count=1.00, evidence_diversity=0.70, graph_coverage=0.82, heuristic_reliability=0.88, signal_agreement=1.00, repo_type_fit=0.91
Confidence reason: Confidence is high (0.88) because evidence_count provides the strongest support while evidence_diversity is the primary limiting factor.

Evidence:
- [{"analysis_method": "complexity_and_velocity_signals", "source_file": "src/ol_dbt/models/marts/combined/marts__combined_program_enrollment_detail.sql", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/ol_dbt/models/marts/combined/marts__combined_course_enrollment_detail.sql", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/ol_dbt/models/dimensional/dim_user.sql", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "bin/dbt-local-dev.py", "line_range": [1, 1]}, {"analysis_method": "complexity_and_velocity_signals", "source_file": "src/ol_dbt/models/reporting/organization_administration_report.sql", "line_range": [1, 1]}]

## 5) Onboarding-Relevant High-Velocity Areas
This view re-ranks raw git-history velocity into onboarding-relevant areas so new engineers can focus on fast-changing runtime paths without losing traceability to commit history.

Onboarding-relevant files:
- src/ol_dbt/models/staging/mitxpro/_mitxpro__sources.yml
- src/ol_dbt/models/staging/mitxonline/_mitxonline__sources.yml
- src/ol_dbt/models/intermediate/mitxonline/_int_mitxonline__models.yml
- src/ol_dbt/models/intermediate/mitxpro/_int_mitxpro__models.yml
- src/ol_dbt/models/staging/mitxpro/_stg_mitxpro__models.yml

Confidence: medium (score: 0.54)
Confidence label: medium
Confidence factors: evidence_count=0.82, evidence_diversity=0.45, graph_coverage=0.44, heuristic_reliability=0.42, signal_agreement=0.55, repo_type_fit=0.70
Confidence reason: Confidence is medium (0.54) because velocity is derived from git-frequency signals over 90 days but clone history is shallow.

Evidence:
- [{"analysis_method": "git_log_frequency", "source_file": "src/ol_dbt/models/staging/mitxpro/_mitxpro__sources.yml", "line_range": [1, 1], "commit_count": 1, "time_window_days": 90, "last_commit_timestamp": "2026-03-10T15:39:55-04:00", "history_status": "shallow"}, {"analysis_method": "git_log_frequency", "source_file": "src/ol_dbt/models/staging/mitxonline/_mitxonline__sources.yml", "line_range": [1, 1], "commit_count": 1, "time_window_days": 90, "last_commit_timestamp": "2026-03-10T15:39:55-04:00", "history_status": "shallow"}, {"analysis_method": "git_log_frequency", "source_file": "src/ol_dbt/models/intermediate/mitxonline/_int_mitxonline__models.yml", "line_range": [1, 1], "commit_count": 2, "time_window_days": 90, "last_commit_timestamp": "2026-03-11T14:05:42-04:00", "history_status": "shallow"}, {"analysis_method": "git_log_frequency", "source_file": "src/ol_dbt/models/intermediate/mitxpro/_int_mitxpro__models.yml", "line_range": [1, 1], "commit_count": 1, "time_window_days": 90, "last_commit_timestamp": "2026-03-10T15:39:55-04:00", "history_status": "shallow"}, {"analysis_method": "git_log_frequency", "source_file": "src/ol_dbt/models/staging/mitxpro/_stg_mitxpro__models.yml", "line_range": [1, 1], "commit_count": 1, "time_window_days": 90, "last_commit_timestamp": "2026-03-10T15:39:55-04:00", "history_status": "shallow"}]
