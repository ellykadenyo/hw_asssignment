#!/usr/bin/env bash
set -euo pipefail

mkdir -p outputs logs

echo "===== 1) Run SQL jobs (DuckDB) ====="
python -u sql/run_sql.py | tee logs/sql_run.log

echo "===== 2) Run ETL ====="
python -m etl.run_etl | tee logs/etl_run.log

echo "===== 3) Tests (optional, non-blocking) ====="
#pytest -q || true
PYTHONPATH=/app pytest tests/ || true

echo "All done. Outputs in outputs/ and logs in logs/"
# Keep container open for inspection
exec /bin/bash