
# DESIGN.md — System Design, Trade‑offs & Operational Thinking

This document outlines the **architectural design**, **operational considerations**, and **scalability strategy** for the implemented solution, framed from a long‑term platform data engineering perspective.

---

## 1. End‑to‑End Logical Design

```text
Raw CSV / JSON
      |
      v
+------------------+
|   Staging (SQL)  |
|  • Type coercion |
|  • Deduping      |
+------------------+
      |
      v
+---------------------------+
| Intermediate Aggregation |
| • Interval normalization |
| • Rolling windows        |
| • Company joins          |
+---------------------------+
      |
      v
+------------------+
|   Fact / Marts   |
| • Gaps           |
| • Cost spikes    |
| • Roster checks  |
+------------------+
      |
      v
Outputs (CSV / Parquet)
```

This flow mirrors how the logic would be encoded in a modern warehouse/dbt‑based stack.

---

## 2. dbt‑Style Model Design (Conceptual)

1. **stg_employees**  
   - Raw ingestion, type normalization, email/domain parsing, light dedupe.

2. **stg_plans**  
   - Plan interval validation, carrier normalization.

3. **stg_claims**  
   - Date coercion, numeric validation, outlier flagging.

4. **int_company_enriched**  
   - EIN/domain resolution, enrichment attributes.

5. **fct_employee_roster**  
   - Active employee counts per company.

6. **fct_claims_rolling_90d**  
   - Rolling 90‑day claim aggregates.

7. **mart_plan_coverage_gaps**  
   - Fully stitched plan intervals with explicit gap representation.

---

## 3. Incremental & State Management

Current implementation:
- Local high‑water‑mark store to demonstrate incremental thinking.

In production:
- Warehouse‑native MERGE semantics (`unique_key + updated_at`)
- Partitioning by `company_ein` and event date
- Idempotent backfills and reruns

---

## 4. What Has Been Accomplished

- Reproducible Dockerized pipeline
- SQL analytics with validated outputs
- Data validation & error isolation
- Local enrichment with caching & retries
- Incremental processing pattern
- Operational logging
- Clear separation of concerns

---

## 5. What Could Be Polished With More Time

- Full dbt project scaffolding and tests
- CI pipeline for SQL + ETL validation
- Warehouse‑scale benchmarking
- Stronger anomaly detection beyond thresholds
- Richer schema contracts and data expectations

---

## 6. Scaling the System

### Data Volume
- Partition & cluster by company and date
- Pre‑aggregate rolling windows
- Avoid fan‑out joins using surrogate keys

### Compute
- Separate ingestion, transformation, and serving layers
- Autoscale warehouse compute based on SLA

### Reliability
- SLA‑aware freshness checks
- Row‑count deltas and distribution drift monitoring
- Defensive fail‑fast on anomalous volume changes

---

## 7. Incident Scenario (20M rows/day)

**Likely causes**
- Key explosion in joins
- Missing dedupe with increased feed volume
- Partition pruning regression

**Response**
1. Stop downstream propagation
2. Profile cardinality and query plans
3. Roll back models or limit ingestion
4. Add regression and cost guardrails

---

## Closing Note

This solution prioritizes **sound engineering judgment over exhaustiveness**.  
The same patterns demonstrated here — observability, reproducibility, incrementalism, and clear trade‑off communication — scale directly to production‑grade data platforms.
