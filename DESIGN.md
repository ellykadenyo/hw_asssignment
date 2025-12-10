
---

### 12) `DESIGN.md`
```markdown
# DESIGN.md — dbt + debugging notes

## 1) Staging → Intermediate → Fact models (5–7 bullets)
1. **stg_employees**: Raw ingestion; parse dates, normalize columns, initial dedupe, parse notes into JSON columns.
2. **stg_plans**: Raw plans; cast to dates, standardize carrier names, ensure interval validity.
3. **stg_claims**: Raw claims; cast service_date, coerce amounts to numeric, flag outliers.
4. **int_company**: Enriched company-level attributes: EIN, domain, enrichment fields (industry, revenue).
5. **fct_employee_roster**: Aggregated active roster per company and period (use last_known flag).
6. **fct_claims_rolling**: Pre-aggregated rolling windows (90-day) per company used for spike detection.
7. **mart_plan_coverage**: Merged plan intervals normalized and merged per company/plan_type; gap detection model.

## 2) Incremental merge logic
- Use unique key per model (e.g., employee_id, company_ein + plan_type + interval_key).
- For incremental loads, implement MERGE semantics:
  - Insert new rows
  - Update rows where updated_at > existing.updated_at
  - Soft-delete via valid_until columns for historical records
- Use dbt incremental materialization with `unique_key` and `is_incremental()` guard.

## 3) Schema & column tests
- Not null constraints on core ids (company_ein, employee_id)
- Relationship tests: every claim.company_ein must exist in stg_company
- Freshness tests on ingestion timestamps
- Acceptable range tests for amounts (non-negative), rolling window results sanity checks

## 4) Freshness & anomaly checks
- Set freshness window for ingestion tables (<= 24h)
- Anomaly detection: sudden row count increases > 3x baseline triggers alert
- Spike detection rule configured as an alertable dbt test (e.g., when pct_change > 300%)

## 5) Metadata columns
- `ingested_at`, `source_file`, `record_hash`, `inserted_at`, `updated_at`, `valid_from`, `valid_to`

## 6) Safe model deprecation strategy
- Mark model as `deprecated` in catalog and add redirectors from model to new model
- Keep deprecated materialization for X weeks (depending on SLA), with log of downstream dependencies using `dbt docs` and `dag` analysis.

## 7) Operational guardrails & monitoring
- Enforce job runtime SLAs. If models increase runtime beyond thresholds, auto-scale compute or fail fast and raise incident.
- Query-level timeouts, partition pruning, and clustering on date/company_ein for large feeds.

## 8) Debugging & incident response (30-minute plan)
- **Triage (0-5m):** Check alert, gather basic metrics: ingestion rate, row counts, runtime changes, recent deploys.
- **Isolate (5-15m):** Re-run failing model on small partition, profile queries (EXPLAIN), check cardinality explosion keys.
- **Mitigate (15-30m):** Rollback recent data or model changes; disable suspect upstream feed ingestion; enact throttling or sampling.
- **Follow-up:** Add regression tests, add more robust partitioning, and increase observability (profile dashboards, explain-as-code).

## Likely causes for regression described
- Key explosion due to new feed adding 20M rows/day -> joins that are not partitioned cause large cross joins.
- Duplicate keys or missing dedupe leading to inflated downstream counts.
- Freshness tests pass because they check ingestion time but not content correctness (needs delta checks).