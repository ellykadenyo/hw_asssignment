# Assignment: PostgreSQL-style data engineering take-home (reproducible)

This repository contains a reproducible solution for the take-home assignment. It runs SQL analyses (DuckDB) and a Python ETL pipeline and writes outputs into `outputs/`.

## Project layout (key files)
- `claims_raw.csv`, `employees_raw.csv`, `plans_raw.csv` — input samples (place in repo root)
- `company_lookup.json`, `api_mock.json` — small JSON inputs
- `sql/` — SQL runner for analytical deliverables
- `etl/` — Python ETL code and tests
- `outputs/` — generated CSV and parquet results
- `Dockerfile`, `docker-compose.yml` — reproducible runtime
- `run_all.sh` — single entrypoint used by Docker

## Reproducible run (Docker)
1. Ensure Docker and docker-compose are installed.
2. Build & run:
```bash
docker-compose build
docker-compose run --rm app

### Linux permission note
If `run_all.sh` fails with "permission denied":
```bash
sed -i 's/\r$//' run_all.sh
chmod +x run_all.sh