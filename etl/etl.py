# etl/etl.py
from pathlib import Path
import pandas as pd
import re
import json
import logging
from .helpers import is_valid_email, extract_domain_from_email, normalize_name, simple_retry, load_or_init_hwm
from .cache import MockAPIClient

logger = logging.getLogger(__name__)

DATE_COLS_EMP = ["start_date"]
DATE_COLS_PLANS = ["start_date", "end_date"]
DATE_COLS_CLAIMS = ["service_date"]

def run_etl_pipeline(root: Path, company_lookup: dict, api_mock: dict, out_dir: Path):
    root = Path(root)
    # Read raw files
    employees = pd.read_csv(root / "employees_raw.csv")
    plans = pd.read_csv(root / "plans_raw.csv")
    claims = pd.read_csv(root / "claims_raw.csv")

    # Normalize column names
    employees.columns = [c.strip() for c in employees.columns]
    plans.columns = [c.strip() for c in plans.columns]
    claims.columns = [c.strip() for c in claims.columns]

    # Coerce dates
    for col in DATE_COLS_EMP:
        if col in employees.columns:
            employees[col] = pd.to_datetime(employees[col], errors='coerce')
    for col in DATE_COLS_PLANS:
        if col in plans.columns:
            plans[col] = pd.to_datetime(plans[col], errors='coerce')
    for col in DATE_COLS_CLAIMS:
        if col in claims.columns:
            claims[col] = pd.to_datetime(claims[col], errors='coerce')

    # Basic dedupe
    employees = employees.drop_duplicates()
    plans = plans.drop_duplicates()
    claims = claims.drop_duplicates()

    # Validation framework
    validation_errors = []

    # Emails: drop rows with clearly bad emails (no @ or domain)
    def validate_employees(df):
        rows_out = []
        for i, row in df.iterrows():
            row_id = row.get('person_id', i)
            email = str(row.get('email', '')).strip()
            if not is_valid_email(email):
                validation_errors.append((row_id, 'email', f'bad_email:{email}'))
                continue
            rows_out.append(row)
        return pd.DataFrame(rows_out) if rows_out else pd.DataFrame(columns=df.columns)

    employees_valid = validate_employees(employees)

    # Infer missing EINs from company_lookup by email domain if missing or null
    def infer_ein(row):
        ein = row.get('company_ein')
        if pd.isna(ein) or ein == "":
            domain = extract_domain_from_email(row.get('email', ''))
            if domain and domain in company_lookup:
                return company_lookup[domain]
        return ein

    employees_valid['company_ein'] = employees_valid.apply(infer_ein, axis=1)

    # Carry forward titles where missing grouped by company + full_name (simple fill)
    employees_valid['title'] = employees_valid['title'].fillna(method='ffill')

    # Extract simple structured info from notes (example: "mgr:Bob; team:Core")
    def parse_notes(notes):
        if pd.isna(notes):
            return {}
        result = {}
        parts = re.split(r'[;|,]', str(notes))
        for p in parts:
            if ':' in p:
                k, v = p.split(':', 1)
                result[k.strip()] = v.strip()
        return result

    employees_valid['notes_parsed'] = employees_valid['notes'].apply(parse_notes)

    # Enrichment via local mock API client with caching & retry
    client = MockAPIClient(api_mock=api_mock, cache_path=root / ".api_cache.json")
    # For each unique domain in employees, enrich
    domains = employees_valid['email'].apply(extract_domain_from_email).dropna().unique().tolist()
    enrich_map = {}
    for d in domains:
        resp = client.get(domain=d)
        enrich_map[d] = resp

    # Attach enrichment columns
    def attach_enrichment(row):
        dom = extract_domain_from_email(row.get('email', ''))
        info = enrich_map.get(dom, {})
        row['industry'] = info.get('industry')
        row['revenue'] = info.get('revenue')
        row['headcount'] = info.get('headcount')
        return row

    employees_enriched = employees_valid.apply(attach_enrichment, axis=1)

    # Build clean dataset (simple join example)
    clean = employees_enriched.copy()
    clean = clean.rename(columns={'person_id': 'employee_id'})

    # Final simple validations: start_date present
    for i, row in clean.iterrows():
        if pd.isna(row.get('start_date')):
            validation_errors.append((row.get('employee_id', i), 'start_date', 'missing'))

    # Save validation_errors.csv
    val_df = pd.DataFrame(validation_errors, columns=['row_id', 'field', 'error_reason'])
    val_df.to_csv(out_dir / 'validation_errors.csv', index=False)

    # Save clean_data.parquet
    clean.to_parquet(out_dir / 'clean_data.parquet', index=False)

    # Incremental high-water-mark: store latest start_date processed
    hwm_db = root / "etl_hwm.sqlite"
    conn = load_or_init_hwm(hwm_db)
    conn.execute("CREATE TABLE IF NOT EXISTS hwm (source TEXT PRIMARY KEY, last_ts TEXT)")
    # For demo we upsert the max start_date
    if 'start_date' in clean.columns and not clean['start_date'].isna().all():
        max_ts = str(clean['start_date'].max())
        conn.execute("INSERT OR REPLACE INTO hwm(source, last_ts) VALUES (?, ?)", ("employees", max_ts))
        conn.commit()
    conn.close()

    logging = __import__('logging')
    logging.getLogger().info(f"ETL complete: {len(clean)} rows valid, {len(val_df)} validation errors")