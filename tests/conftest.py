"""Pytest configuration for market_analysis tests."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"
INGEST_PATH = SRC_PATH / "ingest"

for path in (SRC_PATH, INGEST_PATH):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
