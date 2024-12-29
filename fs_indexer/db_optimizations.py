"""Database optimization utilities."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Tuple
import time
import logging

logger = logging.getLogger(__name__)

from sqlalchemy import text, Engine, select, insert, update, delete, case
from sqlalchemy.orm import Session

# Import indexed_files table from schema to avoid circular import
from .schema import indexed_files, lucidlink_files

def optimize_connection_pool(engine: Engine, config: Dict) -> Engine:
    """Configure database connection pool settings."""
    pool_size = config["performance"]["db_pool_size"]
    max_overflow = config["performance"]["db_max_overflow"]
    
    engine.pool._pool.maxsize = pool_size
    engine.pool._max_overflow = max_overflow
    
    return engine

def configure_sqlite(engine: Engine) -> None:
    """Configure SQLite-specific optimizations."""
    with engine.connect() as conn:
        # Set journal mode to WAL for better concurrency
        conn.execute(text("PRAGMA journal_mode=WAL"))
        # Set synchronous mode for better performance
        conn.execute(text("PRAGMA synchronous=NORMAL"))
        # Set cache size to 256MB (value in pages, -256000 = 256MB)
        conn.execute(text("PRAGMA cache_size=-256000"))
        # Enable memory-mapped I/O for better performance
        conn.execute(text("PRAGMA mmap_size=1073741824"))  # 1GB
        # Set temp store to memory for better performance
        conn.execute(text("PRAGMA temp_store=MEMORY"))
        # Enable foreign key support
        conn.execute(text("PRAGMA foreign_keys=ON"))
        conn.commit()

def get_table_statistics(session: Session) -> Dict[str, Any]:
    """Get database table statistics."""
    with session.connection() as conn:
        # Get total rows
        result = conn.execute(text("SELECT COUNT(*) FROM indexed_files")).scalar()
        
        stats = {
            "total_rows": result,
            "table_size": "N/A (SQLite)",
            "index_size": "N/A (SQLite)"
        }
        
        return stats

def bulk_upsert_files(session: Session, files_batch: List[Dict[str, Any]], table='indexed_files') -> int:
    """Perform optimized bulk upsert of files.
    
    Args:
        session: SQLAlchemy session
        files_batch: List of file dictionaries to upsert
        table: Table name to upsert into ('indexed_files' or 'lucidlink_files')
        
    Returns:
        Number of files processed
    """
    if not files_batch:
        return 0
        
    try:
        # Use raw SQL with INSERT OR REPLACE for better performance
        if table == 'indexed_files':
            stmt = text("""
                INSERT OR REPLACE INTO indexed_files 
                (rel_path, size, modified_at, checksum, indexed_at, error_count, last_error, status)
                VALUES (:rel_path, :size, :modified_at, :checksum, :indexed_at, :error_count, :last_error, :status)
            """)
        else:  # lucidlink_files
            stmt = text("""
                INSERT OR REPLACE INTO lucidlink_files 
                (id, name, type, size, creation_time, update_time, indexed_at, error_count, last_error)
                VALUES (:id, :name, :type, :size, :creation_time, :update_time, :indexed_at, :error_count, :last_error)
            """)
            
        # Execute in chunks to avoid SQLite variable limit
        chunk_size = 999  # SQLite default max variables is 999
        processed = 0
        
        for i in range(0, len(files_batch), chunk_size):
            chunk = files_batch[i:i + chunk_size]
            session.execute(stmt, chunk)
            processed += len(chunk)
            
        session.commit()
        return processed
        
    except Exception as e:
        session.rollback()
        logger.error(f"Bulk upsert failed: {str(e)}")
        raise

def check_missing_files(session: Session, root_path: Path) -> Tuple[int, List[str]]:
    """Check for files in database that no longer exist on disk.
    
    Returns:
        Tuple containing:
        - Number of files removed from database
        - List of removed file paths
    """
    # Get all file paths from database
    db_files = session.execute(
        select(indexed_files.c.rel_path)
    ).scalars().all()
    
    missing_files = []
    for rel_path in db_files:
        full_path = root_path / rel_path
        if not full_path.exists():
            missing_files.append(rel_path)
    
    if missing_files:
        # Remove missing files from database
        session.execute(
            delete(indexed_files)
            .where(indexed_files.c.rel_path.in_(missing_files))
        )
        session.commit()
    
    return len(missing_files), missing_files
