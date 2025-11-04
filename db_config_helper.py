"""
DuckDB Configuration Helper for 24GB RAM System
Auto-configures database connection with optimal settings
"""

import duckdb


def configure_duckdb_session(conn: duckdb.DuckDBPyConnection):
    """
    Configure DuckDB session with optimal settings for 24GB RAM system.
    
    Args:
        conn: DuckDB connection object
    """
    config_commands = [
        "SET memory_limit='20GB'",
        "SET threads TO 8",
        "SET enable_progress_bar=true",
        "SET enable_object_cache=true",
        "SET checkpoint_threshold='1GB'",
        "SET temp_directory='/tmp'",
    ]
    
    for cmd in config_commands:
        try:
            conn.execute(cmd)
        except Exception as e:
            print(f"Warning: Could not set {cmd}: {e}")
    
    return conn


def get_optimized_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection with optimal configuration.
    
    Args:
        db_path: Path to DuckDB database file
        
    Returns:
        Configured DuckDB connection
    """
    conn = duckdb.connect(db_path)
    configure_duckdb_session(conn)
    return conn


if __name__ == "__main__":
    # Test configuration
    conn = get_optimized_connection('new_tick_data.db')
    
    settings = conn.execute("""
        SELECT 
            current_setting('memory_limit') as memory,
            current_setting('threads') as threads,
            current_setting('enable_progress_bar') as progress
    """).fetchone()
    
    print(f"✅ Configuration applied:")
    print(f"   Memory: {settings[0]}")
    print(f"   Threads: {settings[1]}")
    print(f"   Progress Bar: {settings[2]}")

