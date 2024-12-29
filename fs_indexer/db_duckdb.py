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

def cleanup_missing_files(conn: duckdb.DuckDBPyConnection, current_files: List[Dict[str, Any]]) -> int:
    """Remove files from database that are no longer present in the filesystem."""
    try:
        # Create temporary table for current files
        conn.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS current_files (
                id VARCHAR PRIMARY KEY
            );
        """)
        
        # Clear any existing data
        conn.execute("DELETE FROM current_files;")
        
        # Insert current file IDs
        df = pd.DataFrame([{'id': f['id']} for f in current_files])
        conn.execute("INSERT INTO current_files SELECT * FROM df")
        
        # Delete files not in current_files
        result = conn.execute("""
            WITH to_delete AS (
                SELECT id FROM lucidlink_files 
                WHERE id NOT IN (SELECT id FROM current_files)
            )
            DELETE FROM lucidlink_files 
            WHERE id IN (SELECT id FROM to_delete)
            RETURNING *;
        """).fetchall()
        
        # Clean up temporary table
        conn.execute("DROP TABLE current_files")
        
        return len(result)
        
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
