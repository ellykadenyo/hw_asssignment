"""
Entrypoint for ETL: reads raw CSVs, cleans, validates, enriches via local mock per api_mock.json,
writes outputs/validation_errors.csv and outputs/clean_data.parquet.
Implements a simple high-water-mark incremental stub using SQLite.
"""
from pathlib import Path
import logging
import sqlite3
import json
import pandas as pd
import re
from etl.etl import run_etl_pipeline

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "logs" / "etl.log"
OUT = ROOT / "outputs"
OUT.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, filename=LOG, filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
print("Logging to", LOG)

# Load api mock
api_mock_path = ROOT / "api_mock.json"
company_lookup_path = ROOT / "company_lookup.json"

with open(api_mock_path) as f:
    api_mock = json.load(f)

with open(company_lookup_path) as f:
    company_lookup = json.load(f)

# run pipeline
run_etl_pipeline(
    root=ROOT,
    company_lookup=company_lookup,
    api_mock=api_mock,
    out_dir=OUT
)

print("ETL finished. Outputs:", list(OUT.iterdir()))