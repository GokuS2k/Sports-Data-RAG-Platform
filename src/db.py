"""Database connection utilities."""

from pathlib import Path
import duckdb

from .config import DUCKDB_PATH


def get_connection(db_path: Path = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    """Create a connection to the DuckDB database."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))
