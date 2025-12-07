"""DuckDB connection helper for Sports RAG Platform."""

import duckdb
from pathlib import Path
from contextlib import contextmanager

# Path to the persistent DuckDB database file
DB_PATH = Path(__file__).parent.parent / "db" / "sports_rag.duckdb"


def get_connection():
    """
    Return a DuckDB connection to the persistent database file.
    
    This connection is configured for persistent storage (not in-memory).
    All changes will be automatically saved to the database file.
    
    Returns:
        duckdb.DuckDBPyConnection: Connection to the sports_rag.duckdb database
    """
    # Ensure the db directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to the persistent database file
    # read_only=False ensures we can write data (this is the default)
    # Using a file path (not :memory:) ensures persistence to disk
    conn = duckdb.connect(str(DB_PATH), read_only=False)
    return conn


@contextmanager
def get_db_session():
    """
    Context manager for database sessions with automatic cleanup.
    
    DuckDB auto-commits by default, so changes are automatically persisted.
    
    Usage:
        with get_db_session() as conn:
            conn.execute("INSERT INTO leagues VALUES (1, 'Premier League', 'England', '2023-24')")
            # Changes are automatically committed to disk
    
    Yields:
        duckdb.DuckDBPyConnection: Database connection
    """
    conn = get_connection()
    try:
        yield conn
        # DuckDB auto-commits, so no explicit commit needed
    finally:
        # Always close the connection
        conn.close()


def get_cursor():
    """
    Get a cursor from a new connection.
    
    Returns:
        duckdb.DuckDBPyConnection: A connection that can be used as a cursor
    """
    return get_connection()


def init_database():
    """
    Initialize the database by running the schema SQL file.
    This creates all tables if they don't exist.
    """
    schema_path = DB_PATH.parent / "init_schema.sql"
    
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with get_db_session() as conn:
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        # Execute the schema SQL
        conn.execute(schema_sql)
        print(f"Database initialized successfully at {DB_PATH}")


if __name__ == "__main__":
    # Test the connection and initialize database
    init_database()
    
    with get_db_session() as conn:
        # Verify tables were created
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Tables in database: {tables}")
