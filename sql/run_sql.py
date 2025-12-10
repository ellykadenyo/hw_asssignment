"""
Run three SQL analyses against provided CSVs using DuckDB and write outputs/*.csv
- sql_gaps.csv
- sql_spikes.csv
- sql_roster.csv
"""
import duckdb
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT
OUT = ROOT / "outputs"
OUT.mkdir(exist_ok=True)

conn = duckdb.connect(database=":memory:")

# Register CSVs as DuckDB tables using read_csv_auto
conn.execute(f"""
CREATE TABLE claims AS SELECT * FROM read_csv_auto('{DATA_DIR / "claims_raw.csv"}', AUTO_DETECT=TRUE);
CREATE TABLE plans AS SELECT * FROM read_csv_auto('{DATA_DIR / "plans_raw.csv"}', AUTO_DETECT=TRUE);
CREATE TABLE employees AS SELECT * FROM read_csv_auto('{DATA_DIR / "employees_raw.csv"}', AUTO_DETECT=TRUE);
""")

# 1) Plan Gap Detection
# Normalize overlapping intervals per company_ein + plan_type. Stitch adjacent intervals (adjacent = no gap days).
# Find gaps >7 days
plan_gaps_sql = """
WITH normalized AS (
    SELECT
        company_ein,
        plan_type,
        carrier_name,
        CAST(start_date AS DATE) AS start_date,
        CAST(end_date AS DATE) AS end_date
    FROM plans
),
-- Expand intervals by sorting and merging overlapping/adjacent intervals per company/plan_type
ordered AS (
    SELECT company_ein, plan_type, carrier_name, start_date, end_date
    FROM normalized
    ORDER BY company_ein, plan_type, start_date
),
merged AS (
    SELECT
        company_ein,
        plan_type,
        min(start_date) AS start_date,
        max(end_date) AS end_date
    FROM (
        SELECT
            company_ein,
            plan_type,
            start_date,
            end_date,
            SUM(is_new_group) OVER (PARTITION BY company_ein, plan_type ORDER BY start_date, end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grp
        FROM (
            SELECT
                company_ein,
                plan_type,
                start_date,
                end_date,
                CASE
                    WHEN LAG(end_date) OVER (PARTITION BY company_ein, plan_type ORDER BY start_date) IS NULL THEN 1
                    WHEN DATE_DIFF('day', LAG(end_date) OVER (PARTITION BY company_ein, plan_type ORDER BY start_date), start_date) <= 1 THEN 0
                    WHEN DATE_DIFF('day', LAG(end_date) OVER (PARTITION BY company_ein, plan_type ORDER BY start_date), start_date) <= 0 THEN 0
                    ELSE 1
                END AS is_new_group
            FROM (
                SELECT company_ein, plan_type, MIN(start_date) AS start_date, MAX(end_date) AS end_date
                FROM normalized
                GROUP BY company_ein, plan_type, start_date, end_date
                ORDER BY company_ein, plan_type, start_date
            )
        )
    )
    GROUP BY company_ein, plan_type, grp
),
-- Now find gaps between merged intervals per company and plan_type, and include previous/next carrier from original data
gaps AS (
    SELECT
        m.company_ein,
        m.plan_type,
        prev_end + INTERVAL '1 day' AS gap_start,
        curr_start - INTERVAL '1 day' AS gap_end,
        DATE_DIFF('day', curr_start - INTERVAL '1 day', prev_end + INTERVAL '1 day') AS gap_length_days,
        prev_carrier,
        next_carrier
    FROM (
        SELECT
            company_ein,
            plan_type,
            LAG(end_date) OVER (PARTITION BY company_ein, plan_type ORDER BY start_date) as prev_end,
            start_date as curr_start,
            LAG(carrier_name) OVER (PARTITION BY company_ein, plan_type ORDER BY start_date) as prev_carrier,
            carrier_name as next_carrier
        FROM (
            SELECT company_ein, plan_type, start_date, end_date, carrier_name
            FROM normalized
            ORDER BY company_ein, plan_type, start_date
        )
    ) m
    WHERE prev_end IS NOT NULL
    AND DATE_DIFF('day', prev_end, curr_start) > 7
)
SELECT
    COALESCE(company_ein, '') AS company_name,
    gap_start,
    gap_end,
    gap_length_days,
    prev_carrier AS previous_carrier,
    next_carrier AS next_carrier
FROM gaps
ORDER BY company_name, gap_start
;
"""

try:
    conn.execute(plan_gaps_sql)
    df = conn.sql(plan_gaps_sql).df()
    df.to_csv(OUT / "sql_gaps.csv", index=False)
    print(f"Wrote {OUT / 'sql_gaps.csv'}")
except Exception as e:
    print("Plan gaps SQL failed:", e, file=sys.stderr)

# 2) Claims cost spike: rolling 90-day claim costs by company ordered by service_date; flag >200% spikes.
claims_spikes_sql = """
WITH c AS (
    SELECT company_ein AS company_name, CAST(service_date AS DATE) AS service_date, amount
    FROM claims
),
agg AS (
    SELECT
        company_name,
        service_date,
        SUM(amount) OVER (PARTITION BY company_name ORDER BY service_date
            RANGE BETWEEN INTERVAL '90' DAY PRECEDING AND INTERVAL '1' DAY PRECEDING) AS prev_90d_cost,
        SUM(amount) OVER (PARTITION BY company_name ORDER BY service_date
            RANGE BETWEEN INTERVAL '89' DAY PRECEDING AND CURRENT ROW) AS current_90d_cost
    FROM c
),
flagged AS (
    SELECT DISTINCT
        company_name,
        DATE_TRUNC('day', service_date - INTERVAL '89 days') AS window_start,
        DATE_TRUNC('day', service_date) AS window_end,
        COALESCE(prev_90d_cost, 0) AS prev_90d_cost,
        COALESCE(current_90d_cost, 0) AS current_90d_cost,
        CASE WHEN prev_90d_cost = 0 AND current_90d_cost > 0 THEN 999.0
             WHEN prev_90d_cost = 0 THEN 0.0
             ELSE (current_90d_cost - prev_90d_cost) / NULLIF(prev_90d_cost,0) * 100.0 END AS pct_change
    FROM agg
)
SELECT
    company_name,
    window_start,
    window_end,
    prev_90d_cost,
    current_90d_cost,
    pct_change
FROM flagged
WHERE (prev_90d_cost = 0 AND current_90d_cost > 0) OR (prev_90d_cost > 0 AND (current_90d_cost / prev_90d_cost) > 3.0)
ORDER BY company_name, window_start;
"""
try:
    conn.execute(claims_spikes_sql)
    df2 = conn.sql(claims_spikes_sql).df()
    df2.to_csv(OUT / "sql_spikes.csv", index=False)
    print(f"Wrote {OUT / 'sql_spikes.csv'}")
except Exception as e:
    print("Claims spikes SQL failed:", e, file=sys.stderr)

# 3) Employee roster mismatch
# expected = distinct active employees from employees table. Observed = employee_count if present; otherwise fallback to mapping
roster_sql = """
WITH expected_cte AS (
    SELECT company_ein AS company_name, COUNT(DISTINCT person_id) AS observed
    FROM employees
    GROUP BY company_ein
),
-- In provided data there is no employee_count column. Use provided mapping assumption if needed.
expected_mapping AS (
    SELECT * FROM (VALUES
        ('11-1111111', 60),
        ('22-2222222', 45),
        ('33-3333333', 40)
    ) AS t(company_name, expected)
)
SELECT
    COALESCE(e.company_name, m.company_name) AS company_name,
    COALESCE(m.expected, 0) as expected,
    COALESCE(e.observed, 0) as observed,
    CASE WHEN COALESCE(m.expected, 0) = 0 THEN NULL
         ELSE ROUND(ABS(COALESCE(e.observed, 0) - m.expected) * 100.0 / m.expected, 2) END AS pct_diff,
    CASE
        WHEN COALESCE(m.expected,0) = 0 THEN 'Unknown'
        WHEN ABS(COALESCE(e.observed,0) - m.expected) < 0.2 * m.expected THEN 'Low'
        WHEN ABS(COALESCE(e.observed,0) - m.expected) < 0.5 * m.expected THEN 'Medium'
        WHEN ABS(COALESCE(e.observed,0) - m.expected) <= 1.0 * m.expected THEN 'High'
        ELSE 'Critical'
    END AS severity
FROM expected_cte e
FULL OUTER JOIN expected_mapping m ON e.company_name = m.company_name
ORDER BY company_name;
"""
try:
    conn.execute(roster_sql)
    df3 = conn.sql(roster_sql).df()
    df3.to_csv(OUT / "sql_roster.csv", index=False)
    print(f"Wrote {OUT / 'sql_roster.csv'}")
except Exception as e:
    print("Roster SQL failed:", e, file=sys.stderr)

conn.close()