# RECONNAISSANCE

## Phase 0 Target Selection

- Primary target: `test_repos/jaffle_shop` (dbt Labs open-source demo/legacy project).
- Reason selected: explicit recommended candidate in assignment prompt; local git clone available for manual recon.
- Constraint encountered: attempted to switch to Apache Airflow for stricter brownfield scale, but network clone in this environment produced empty repos only.
- File count observed in local target directory: 54 files total on disk (including `.git` internals), 19 tracked project files.
- Languages/artifact types observed: SQL, YAML, Markdown, CSV, PNG, text.

## Manual Recon Window

- Approximate manual exploration duration: 30+ minutes.
- Files inspected by hand: `README.md`, `dbt_project.yml`, all model SQL files, `models/schema.yml`, `models/staging/schema.yml`, docs markdown files, and recent git history.

## Five FDE Day-One Questions (Manual Answers)

1. What is the primary ingestion path?
- Raw data enters via dbt seeds in `seeds/raw_customers.csv`, `seeds/raw_orders.csv`, and `seeds/raw_payments.csv`.
- `dbt seed` materializes those CSVs; staging models (`models/staging/stg_*.sql`) ingest via `{{ ref('raw_*') }}`.
- Mart models (`models/orders.sql`, `models/customers.sql`) ingest from staging via `{{ ref('stg_*') }}`.

2. What are the 3-5 most critical outputs?
- Warehouse relation for `orders` model (`models/orders.sql`) with payment-method splits and total `amount`.
- Warehouse relation for `customers` model (`models/customers.sql`) with first/most recent order and lifetime value.
- Staging relations (`stg_customers`, `stg_orders`, `stg_payments`) that all downstream marts depend on.
- Data quality test outcomes declared in `models/schema.yml` and `models/staging/schema.yml` (`unique`, `not_null`, `accepted_values`, `relationships`).
- Generated project documentation context from `models/docs.md` + `models/overview.md` + schema metadata (`dbt docs generate` path).

3. If one critical module fails, what is the blast radius?
- If `models/staging/stg_orders.sql` fails or produces bad keys, both marts are impacted:
- `models/orders.sql` breaks directly (depends on `stg_orders`).
- `models/customers.sql` breaks/derives incorrect metrics (joins orders and payments through order keys).
- Relationship and not-null tests for downstream models become invalid/noisy.

4. Is business logic concentrated or distributed?
- Mostly concentrated in two mart files:
- `models/orders.sql`: payment method pivots + order-level amount rollups.
- `models/customers.sql`: customer-level aggregations (first order, most recent order, number of orders, CLV).
- Staging files are thin normalization/renaming layers, not heavy business-rule hubs.

5. What files change most often (90-day velocity map)?
- 90-day git window: no commits found (`git log --since='90 days ago'` returned empty).
- Most recent commit: `fd7bfac` on 2024-04-18 (`README` disclaimer update).
- Historical hotspots (all-time commit touch count):
- `models/customers.sql` (12)
- `models/orders.sql` (8)
- `dbt_project.yml` (5)
- `README.md` (4)

## Manual Difficulty Analysis

## Hardest to infer manually

- Determining operational criticality from a small codebase without runtime metadata (e.g., which model truly breaks business reporting first).
- Separating “demo simplifications” from real architectural patterns (README explicitly says this project keeps anti-patterns for simplicity).
- Estimating change velocity from a repository with no recent commits.

## Where I got lost

- Initial ambiguity over whether to treat seed-driven ingestion as “real” ingestion vs warehouse source ingestion.
- Ambiguity around “production system” requirement vs prompt-recommended `jaffle_shop` candidate.

## Architecture priorities implied by this recon

- Prioritize reliable lineage extraction from `ref()` graphs first; that gave the fastest path to blast-radius reasoning.
- Always pair structural lineage with test metadata (schema tests are key risk signals in dbt projects).
- Include velocity confidence labels (for example, “no recent activity”) so downstream automation does not overstate certainty.
