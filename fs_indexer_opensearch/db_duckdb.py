"""DuckDB database layer for file indexer."""

import logging
import duckdb
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd

logger = logging.getLogger(__name__)

def init_database(db_url: str) -> duckdb.DuckDBPyConnection:
    """Initialize the database connection and create tables if they don't exist."""
    # Extract the actual file path from the URL
    db_path = db_url.replace('duckdb:///', '')
    
    conn = duckdb.connect(db_path)
    
    # Check if table exists before dropping
    table_exists = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'lucidlink_files'").fetchone()[0] > 0
    if table_exists:
        conn.execute("DROP TABLE lucidlink_files")
    
    # Create files table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lucidlink_files (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            type VARCHAR,
            size BIGINT,
            creation_time TIMESTAMP,
            update_time TIMESTAMP,
            indexed_at TIMESTAMP,
            error_count INTEGER DEFAULT 0,
            last_error VARCHAR
        );
    """)
    
    return conn

def bulk_upsert_files(conn: duckdb.DuckDBPyConnection, files_batch: List[Dict[str, Any]]) -> int:
    """Perform optimized bulk upsert of files using DuckDB."""
    if not files_batch:
        return 0
        
    try:
        # Create temporary table for batch data
        conn.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_batch (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                type VARCHAR,
                size BIGINT,
                creation_time TIMESTAMP,
                update_time TIMESTAMP,
                indexed_at TIMESTAMP,
                error_count INTEGER,
                last_error VARCHAR
            );
        """)
        
        # Clear any existing data in temp table
        conn.execute("DELETE FROM temp_batch;")
        
        # Insert batch data directly using DuckDB's DataFrame interface
        df = pd.DataFrame(files_batch)
        conn.execute("INSERT INTO temp_batch SELECT * FROM df")
        
        # Perform upsert using REPLACE strategy with conflict target
        conn.execute("""
            INSERT INTO lucidlink_files 
            SELECT * FROM temp_batch 
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                size = EXCLUDED.size,
                creation_time = EXCLUDED.creation_time,
                update_time = EXCLUDED.update_time,
                indexed_at = EXCLUDED.indexed_at,
                error_count = EXCLUDED.error_count,
                last_error = EXCLUDED.last_error;
        """)
        
        # Get number of affected rows
        result = conn.execute("SELECT COUNT(*) FROM temp_batch").fetchone()[0]
        
        # Clean up temporary table
        conn.execute("DROP TABLE temp_batch")
        
        return result
        
    except Exception as e:
        logger.error(f"Bulk upsert failed: {str(e)}")
        raise

def cleanup_missing_files(session: duckdb.DuckDBPyConnection, current_files: List[Dict[str, str]]) -> None:
    """Remove files from the database that no longer exist in the filesystem."""
    try:
        # Convert current files to DataFrame
        df = pd.DataFrame([{'id': f['id']} for f in current_files])
        
        if df.empty:
            logger.warning("No current files provided for cleanup")
            return
            
        # Create temporary table with current file IDs
        session.execute("CREATE TEMP TABLE IF NOT EXISTS current_files (id STRING)")
        session.register("current_files_df", df)
        session.execute("INSERT INTO current_files SELECT id FROM current_files_df")
        
        # Delete files that don't exist in current_files
        session.execute("""
            DELETE FROM lucidlink_files
            WHERE id NOT IN (SELECT id FROM current_files)
        """)
        
        # Drop temporary table
        session.execute("DROP TABLE IF EXISTS current_files")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise

def get_database_stats(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get database statistics."""
    try:
        stats = {}
        
        # Get total rows
        stats['total_rows'] = conn.execute("""
            SELECT COUNT(*) FROM lucidlink_files
        """).fetchone()[0]
        
        # Get total size
        stats['total_size'] = conn.execute("""
            SELECT SUM(size) FROM lucidlink_files
        """).fetchone()[0]
        
        # Get file count by type
        type_counts = conn.execute("""
            SELECT type, COUNT(*) as count
            FROM lucidlink_files
            GROUP BY type
        """).fetchall()
        
        stats['type_counts'] = {t: c for t, c in type_counts}
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get database stats: {str(e)}")
        raise
