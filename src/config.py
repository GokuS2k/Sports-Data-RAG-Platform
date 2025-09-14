"""Application configuration utilities."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DUCKDB_PATH = DATA_DIR / "duckdb" / "sports.duckdb"
FBREF_CACHE = DATA_DIR / "raw" / "fbref" / "cache"


def get_env(name: str, default: str | None = None) -> str | None:
    """Return an environment variable if set, otherwise default."""
    return os.getenv(name, default)
