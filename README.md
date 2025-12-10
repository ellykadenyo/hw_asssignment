
# Data Engineering Take‑Home Assignment (Reproducible via Docker)

This repository contains a **fully reproducible, end‑to‑end data engineering solution** implementing the requirements of the take‑home assignment.  
It covers advanced SQL analytics, a Python‑based ETL pipeline, validation and enrichment, incremental processing concepts, and system‑level design considerations.

The solution is intentionally scoped to balance **correctness, clarity, and reproducibility**, while outlining where additional hardening would be applied in a production environment.

---

## 1. What This Project Does

At a high level, the project:

1. **Runs advanced SQL analytics (DuckDB)** to generate:
   - Plan coverage gaps
   - Rolling claim cost spikes
   - Employee roster mismatches

2. **Executes a Python ETL pipeline** that:
   - Cleans and normalizes raw datasets
   - Validates and separates bad records
   - Enriches company data using a mock API (locally simulated, cached)
   - Produces clean, analytics‑ready parquet output

3. **Produces deterministic outputs** into an `outputs/` directory using a single Docker command.

All logic runs locally without external dependencies, making the solution repeatable on any machine with Docker installed.

---

## 2. Repository Structure

```text
.
├── claims_raw.csv
├── employees_raw.csv
├── plans_raw.csv
├── company_lookup.json
├── api_mock.json
│
├── sql/
│   ├── plan_gaps.sql
│   ├── claims_spikes.sql
│   ├── roster_mismatch.sql
│   └── run_sql.py
│
├── etl/
│   ├── __init__.py
│   ├── run_etl.py
│   ├── etl.py
│   ├── helpers.py
│   └── high_water_mark.sqlite
│
├── tests/
│   └── test_helpers.py
│
├── outputs/
│   ├── sql_gaps.csv
│   ├── sql_spikes.csv
│   ├── sql_roster.csv
│   ├── clean_data.parquet
│   └── validation_errors.csv
│
├── logs/
│   └── etl.log
│
├── Dockerfile
├── docker-compose.yml
├── run_all.sh
├── requirements.txt
├── README.md
└── DESIGN.md
```

---

## 3. How to Clone and Run

### Prerequisites
- Docker (20+)
- Docker Compose v2

### Clone

```bash
git clone https://github.com/ellykadenyo/hw_asssignment.git
cd hw_asssignment/
```

### Set file permissions and create logs directory

```bash
sed -i 's/\r$//' run_all.sh
chmod +x run_all.sh
mkdir logs
```

### Run End‑to‑End (Single Command)

```bash
docker compose build --no-cache
docker compose run --rm app
```

This will:

1. Execute all SQL analytics using DuckDB
2. Run the Python ETL pipeline
3. Write outputs to `outputs/`
4. Write logs to `logs/etl.log`

No additional setup is required.

---

## 4. Outputs

After a successful run, the following will be present in `outputs/`:

- `sql_gaps.csv` — plan coverage gaps >7 days
- `sql_spikes.csv` — >200% 90‑day rolling claim spikes
- `sql_roster.csv` — employee count mismatches by severity
- `clean_data.parquet` — validated and enriched dataset
- `validation_errors.csv` — row‑level validation failures

---

## 5. Notes on Assumptions & Scoping

- SQL dialect uses **DuckDB‑native interval arithmetic** for portability.
- Employee count expectations are derived from an explicit mapping (documented in SQL).
- Incremental processing uses a local high‑water mark store to demonstrate approach.
- Tests are lightweight and intentionally optional, focusing on high‑value helpers.
- External API enrichment is simulated locally to ensure deterministic runs.

Given additional time, these components would be extended for production (see DESIGN.md).

---

## 6. Platform & Reproducibility Notes

- All dependencies are version‑pinned for deterministic builds.
- Docker is the source of truth for runtime behavior.
- All input files shared have been pushed to the git repo to make reproducibility easier.
- Known Linux permission edge cases (`run_all.sh`) are documented and handled.

```bash
sed -i 's/\r$//' run_all.sh
chmod +x run_all.sh
```

---

This repository reflects how I would approach a scoped but production‑oriented data engineering problem: **correct first, observable second, scalable by design**.
