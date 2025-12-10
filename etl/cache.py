# etl/cache.py
from pathlib import Path
import json
import time
from typing import Optional

class MockAPIClient:
    def __init__(self, api_mock: dict, cache_path: Path):
        self.api_mock = api_mock
        self.cache_path = cache_path
        if cache_path.exists():
            try:
                self.cache = json.loads(cache_path.read_text())
            except Exception:
                self.cache = {}
        else:
            self.cache = {}

    def _save(self):
        try:
            self.cache_path.write_text(json.dumps(self.cache))
        except Exception:
            pass

    def get(self, domain: str) -> dict:
        # Simple cache first
        if domain in self.cache:
            return self.cache[domain]
        # Simulate shape
        resp = self.api_mock.get("sample_response", {})
        # Add domain-specific tweak (simulate variance)
        out = dict(resp)
        out['domain'] = domain
        # store
        self.cache[domain] = out
        self._save()
        return out