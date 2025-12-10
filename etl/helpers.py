# etl/helpers.py
import re
import sqlite3
from pathlib import Path
import time
import backoff
import json

EMAIL_RE = re.compile(r"[^@]+@[^@]+\.[^@]+")

def is_valid_email(email: str) -> bool:
    if not email or not isinstance(email, str):
        return False
    return EMAIL_RE.match(email.strip()) is not None

def extract_domain_from_email(email: str) -> str:
    if not email or '@' not in email:
        return None
    return email.split('@')[-1].lower().strip()

def normalize_name(name: str) -> str:
    if not name:
        return None
    return " ".join(name.split()).title()

def load_or_init_hwm(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    return conn

# Simple retry wrapper using backoff
def simple_retry(fn):
    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)
    return wrapped

# Minimal assertion test helpers (used by tests)
def assert_valid_email_examples():
    assert is_valid_email("a@b.com")
    assert not is_valid_email("no-at-symbol")