"""DuckDB database layer for file indexer."""

import logging
import duckdb
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
import pandas as pd
from dateutil import tz
import pytz

logger = logging.getLogger(__name__)

def init_database(db_url: str) -> duckdb.DuckDBPyConnection:
    """Initialize the database connection and create tables if they don't exist."""
    # Extract the actual file path from the URL
    db_path = db_url.replace('duckdb:///', '')
    
    conn = duckdb.connect(db_path)
    
    # Create files table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lucidlink_files (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            relative_path VARCHAR,
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
                relative_path VARCHAR,
                type VARCHAR,
                size BIGINT,
                creation_time TIMESTAMP,
                update_time TIMESTAMP,
                indexed_at TIMESTAMP,
                error_count INTEGER,
                last_error VARCHAR
            );
        """)
        
        # Convert timestamps and prepare data
        now = datetime.now(pytz.utc)
        df = pd.DataFrame([{
            'id': f['id'],
            'name': f['name'],  # Use name from API
            'relative_path': f['name'],  # Use name from API as relative path
            'type': f['type'],
            'size': f.get('size', 0),
            'creation_time': datetime.fromtimestamp(f['creationTime'] / 1e9, tz=pytz.utc),
            'update_time': datetime.fromtimestamp(f['updateTime'] / 1e9, tz=pytz.utc),
            'indexed_at': now,
            'error_count': 0,
            'last_error': None
        } for f in files_batch])
        
        # Register DataFrame and perform upsert
        conn.register("batch_df", df)
        
        conn.execute("""
            INSERT OR REPLACE INTO lucidlink_files 
            SELECT * FROM batch_df
        """)
        
        # Drop temporary table
        conn.execute("DROP TABLE IF EXISTS temp_batch")
        
        return len(files_batch)
        
    except Exception as e:
        logger.error(f"Bulk upsert failed: {str(e)}")
        raise

def cleanup_missing_files(session: duckdb.DuckDBPyConnection, current_files: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    """Remove files from the database that no longer exist in the filesystem.
    Returns a list of tuples (id, path) that were removed."""
    try:
        # Convert current files to DataFrame
        df = pd.DataFrame([{'id': f['id']} for f in current_files])
        
        if df.empty:
            logger.warning("No current files provided for cleanup")
            return []
            
        # Log current state
        total_files = session.execute("SELECT COUNT(*) FROM lucidlink_files").fetchone()[0]
        logger.info(f"Total files in database before cleanup: {total_files}")
        logger.info(f"Current files provided for cleanup: {len(current_files)}")
        
        # Create temporary table with current file IDs
        session.execute("DROP TABLE IF EXISTS current_files")
        session.execute("CREATE TEMP TABLE current_files (id STRING)")
        session.register("current_files_df", df)
        session.execute("INSERT INTO current_files SELECT id FROM current_files_df")
        
        # Get list of files to be removed with their paths
        removed_files = session.execute("""
            SELECT id, name, type FROM lucidlink_files
            WHERE id NOT IN (SELECT id FROM current_files)
        """).fetchall()
        
        if removed_files:
            logger.info(f"Found {len(removed_files)} files to remove:")
            for row in removed_files[:5]:  # Show first 5 for debugging
                logger.info(f"  - {row[1]} (type: {row[2]})")
            if len(removed_files) > 5:
                logger.info(f"  ... and {len(removed_files) - 5} more")
        
        # Delete files that don't exist in current_files
        session.execute("""
            DELETE FROM lucidlink_files
            WHERE id NOT IN (SELECT id FROM current_files)
        """)
        
        # Log final state
        remaining_files = session.execute("SELECT COUNT(*) FROM lucidlink_files").fetchone()[0]
        logger.info(f"Files remaining after cleanup: {remaining_files}")
        
        # Drop temporary table
        session.execute("DROP TABLE IF EXISTS current_files")
        
        # Return list of removed file IDs and paths
        return [(row[0], row[1]) for row in removed_files]
        
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

def needs_schema_update(conn: duckdb.DuckDBPyConnection) -> bool:
    """Check if the database needs a schema update by checking for required columns."""
    try:
        # Get column names from lucidlink_files table
        result = conn.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'lucidlink_files'
        """).fetchall()
        columns = [row[0].lower() for row in result]
        
        # Check if relative_path column exists
        return 'relative_path' not in columns
        
    except Exception as e:
        logger.error(f"Error checking schema: {str(e)}")
        # If there's an error (like table doesn't exist), assume we need an update
        return True

def reset_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop and recreate all tables."""
    try:
        conn.execute("DROP TABLE IF EXISTS lucidlink_files")
        conn.execute("DROP TABLE IF EXISTS temp_batch")
        
        # Recreate tables with new schema
        conn.execute("""
            CREATE TABLE lucidlink_files (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                relative_path VARCHAR,
                type VARCHAR,
                size BIGINT,
                creation_time TIMESTAMP,
                update_time TIMESTAMP,
                indexed_at TIMESTAMP,
                error_count INTEGER DEFAULT 0,
                last_error VARCHAR
            );
        """)
        logger.info("Database tables reset successfully")
        
    except Exception as e:
        logger.error(f"Error resetting database: {str(e)}")
        raise
