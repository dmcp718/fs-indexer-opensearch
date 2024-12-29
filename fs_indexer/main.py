#!/usr/bin/env python3

import logging
import logging.handlers
import os
import sys
import time
import argparse
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Generator
from threading import Lock, Thread, Event
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import duckdb
import asyncio
from fs_indexer.db_duckdb import init_database, bulk_upsert_files, cleanup_missing_files, get_database_stats
from fs_indexer.lucidlink_api import LucidLinkAPI

# Initialize basic logging first
logger = logging.getLogger(__name__)

# Load configuration
def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from file."""
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), 'indexer-config.yaml')
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    # Set default values
    config.setdefault('database', {})
    config['database'].setdefault('connection', {})
    config['database']['connection'].setdefault('url', 'duckdb:///fs_index.duckdb')
    config['database']['connection'].setdefault('options', {})
    
    # Create data directory if needed
    db_path = config['database']['connection']['url'].replace('duckdb:///', '')
    if db_path and os.path.dirname(db_path):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    return config

# Configure logging
def configure_logging(config: Dict[str, Any]) -> None:
    """Configure logging with both file and console handlers."""
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, config["logging"]["level"]))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Add file handler
    log_file = config["logging"]["file"]
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=config["logging"]["max_size_mb"] * 1024 * 1024,  # Convert MB to bytes
        backupCount=config["logging"]["backup_count"],
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    # Add console handler if enabled
    if config["logging"].get("console", True):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)

# Constants
def get_constants(config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "BATCH_SIZE": config["batch_size"],
        "READ_BUFFER_SIZE": config["read_buffer_size"]
    }

class WorkflowStats:
    """Track workflow statistics."""
    def __init__(self):
        self.start_time = time.time()
        self.end_time = None
        self.total_files = 0
        self.total_dirs = 0  # Track number of directories
        self.files_updated = 0
        self.files_skipped = 0
        self.files_removed = 0
        self.removed_paths = []
        self.errors = []
        self.total_size = 0  # Track total size in bytes
        self.lock = Lock()  # Add lock for thread safety
        
    def add_error(self, error: str):
        """Add an error to the stats."""
        self.errors.append(error)
        
    def add_removed_files(self, count: int, paths: List[str]):
        """Add information about removed files."""
        self.files_removed = count
        self.removed_paths = paths
        
    def finish(self):
        """Mark the workflow as finished and set end time."""
        self.end_time = time.time()
        
def should_skip_file(file_path: Path, config: Dict[str, Any]) -> bool:
    """Check if a file should be skipped based on configuration."""
    # Check if file is in a skipped directory
    for skip_dir in config["skip_patterns"]["directories"]:
        try:
            if skip_dir in str(file_path):
                return True
        except Exception:
            continue
    
    # Check if file starts with .
    if file_path.name.startswith("."):
        return True
        
    # Check file extension
    extension = file_path.suffix.lower()
    if extension in config["skip_patterns"]["extensions"]:
        return True
        
    return False

def calculate_checksum(file_path: str, buffer_size: int = 131072) -> str:
    """Calculate xxHash checksum of a file."""
    try:
        if not os.access(file_path, os.R_OK):
            logger.error(f"Permission denied: Unable to access file for checksum calculation: {file_path}")
            raise PermissionError(f"No read access to file: {file_path}")
            
        # hasher = xxhash.xxh64()
        # with open(file_path, "rb") as f:
        #     try:
        #         while True:
        #             data = f.read(buffer_size)
        #             if not data:
        #                 break
        #             hasher.update(data)
        #         return hasher.hexdigest()
        #     except IOError as e:
        #         logger.error(f"Error reading file during checksum calculation: {file_path}: {str(e)}")
        #         raise
    except Exception as e:
        logger.error(f"Failed to calculate checksum for {file_path}: {str(e)}")
        raise

def scan_directory_parallel(root_path: Path, config: Dict[str, Any], max_workers: int = 16) -> Generator[Dict[str, Any], None, None]:
    """Scan directory in parallel for better performance."""
    start_time = time.time()
    scanned_files = 0
    
    def scan_directory(dir_path: Path) -> List[Dict[str, Any]]:
        """Scan a single directory and its immediate files."""
        files = []
        try:
            for entry in os.scandir(dir_path):
                try:
                    path = Path(entry.path)
                    if entry.is_file() and not should_skip_file(path, config):
                        stat = entry.stat()
                        files.append({
                            "rel_path": str(path.relative_to(root_path)),
                            "path": str(path),
                            "size": stat.st_size,
                            "mtime": stat.st_mtime
                        })
                except (OSError, ValueError) as e:
                    logger.error(f"Error scanning {entry.path}: {e}")
        except OSError as e:
            logger.error(f"Error scanning directory {dir_path}: {e}")
        return files

    # Get all directories first
    dirs_to_scan = []
    try:
        for entry in os.scandir(root_path):
            try:
                path = Path(entry.path)
                if entry.is_dir() and not should_skip_file(path, config):
                    dirs_to_scan.append(path)
                elif entry.is_file() and not should_skip_file(path, config):
                    # Process root files immediately
                    stat = entry.stat()
                    scanned_files += 1
                    yield {
                        "rel_path": str(path.relative_to(root_path)),
                        "path": str(path),
                        "size": stat.st_size,
                        "mtime": stat.st_mtime
                    }
            except (OSError, ValueError) as e:
                logger.error(f"Error accessing {entry.path}: {e}")
    except OSError as e:
        logger.error(f"Error scanning root directory {root_path}: {e}")
        return

    # Process directories in parallel chunks
    chunk_size = config["scan_chunk_size"]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while dirs_to_scan:
            # Take next chunk of directories
            chunk = dirs_to_scan[:chunk_size]
            dirs_to_scan = dirs_to_scan[chunk_size:]
            
            # Process current chunk
            for dir_files in executor.map(scan_directory, chunk):
                scanned_files += len(dir_files)
                for file_info in dir_files:
                    yield file_info
            
            # Get subdirectories from processed directories
            for dir_path in chunk:
                try:
                    for entry in os.scandir(dir_path):
                        try:
                            path = Path(entry.path)
                            if entry.is_dir() and not should_skip_file(path, config):
                                dirs_to_scan.append(path)
                        except (OSError, ValueError) as e:
                            logger.error(f"Error accessing {entry.path}: {e}")
                except OSError as e:
                    logger.error(f"Error scanning directory {dir_path}: {e}")
            
            # Log progress
            scan_duration = time.time() - start_time
            scan_rate = scanned_files / scan_duration if scan_duration > 0 else 0
            logger.info(f"Scanned {scanned_files} files so far ({scan_rate:.1f} files/s)")

def process_files_worker(queue: Queue, stop_event: Event, session: duckdb.DuckDBPyConnection, stats: WorkflowStats, config: Dict[str, Any]):
    """Worker thread to process files from queue."""
    files_chunk = []
    chunk_size = config["batch_size"]
    chunk_start_time = time.time()
    
    while not stop_event.is_set() or not queue.empty():
        try:
            file_info = queue.get(timeout=1)  # 1 second timeout
            files_chunk.append(file_info)
            stats.total_size += file_info["size"]  # Add file size to total
            
            # Process chunk when it reaches batch size
            if len(files_chunk) >= chunk_size:
                try:
                    # Bulk upsert the chunk
                    processed = bulk_upsert_files(session, files_chunk)
                    stats.files_updated += processed
                    chunk_duration = time.time() - chunk_start_time
                    chunk_rate = processed / chunk_duration if chunk_duration > 0 else 0
                    logger.info(f"Database batch: {processed} files committed")
                    logger.info(f"Processed {stats.files_updated}/{stats.total_files} files ({chunk_rate:.1f} files/s in last chunk)")
                    
                    # Reset for next chunk
                    files_chunk = []
                    chunk_start_time = time.time()
                except Exception as e:
                    logger.error(f"Error in batch update: {e}")
                    # Keep retrying with smaller batches
                    if len(files_chunk) > 1000:
                        files_chunk = files_chunk[:1000]
                    else:
                        files_chunk = []
                    chunk_start_time = time.time()
                
        except Empty:
            # No files available, wait for more
            if not stop_event.is_set():
                continue
            break
        except Exception as e:
            logger.error(f"Error processing files: {e}")
            break
    
    # Process remaining files
    if files_chunk:
        try:
            processed = bulk_upsert_files(session, files_chunk)
            stats.files_updated += processed
            chunk_duration = time.time() - chunk_start_time
            chunk_rate = processed / chunk_duration if chunk_duration > 0 else 0
            logger.info(f"Database batch: {processed} files committed")
            logger.info(f"Processed {stats.files_updated}/{stats.total_files} files ({chunk_rate:.1f} files/s in last chunk)")
        except Exception as e:
            logger.error(f"Error processing final chunk: {e}")

def batch_update_database(session: duckdb.DuckDBPyConnection, files_batch: List[Dict[str, Any]]) -> None:
    """Update database with a batch of files."""
    try:
        processed = bulk_upsert_files(session, files_batch)
        if processed != len(files_batch):
            logger.warning(f"Processed {processed} files out of {len(files_batch)} in batch")
            
    except Exception as e:
        error_msg = f"Error updating database: {e}"
        logger.error(error_msg)
        raise

def process_files(session: duckdb.DuckDBPyConnection, files: List[Dict], stats: WorkflowStats, config: Dict[str, Any]) -> None:
    """Process a list of files and update the database."""
    try:
        total_files = len(files)
        stats.total_files = total_files
        
        # Process files in batches
        total_processed = 0
        start_time = time.time()
        last_progress_time = start_time
        last_progress_count = 0
        
        for i in range(0, len(files), config["batch_size"]):
            batch = files[i:i + config["batch_size"]]
            processed = bulk_upsert_files(session, batch)
            total_processed += processed
            stats.files_updated += processed
            
            # Calculate and log processing rate
            current_time = time.time()
            elapsed = current_time - last_progress_time
            files_in_chunk = total_processed - last_progress_count
            rate = files_in_chunk / elapsed if elapsed > 0 else 0
            logger.info(f"Processed {total_processed}/{total_files} files ({rate:.1f} files/s in last chunk)")
            last_progress_time = current_time
            last_progress_count = total_processed
            
    except Exception as e:
        error_msg = f"Critical error in process_files: {str(e)}"
        logger.error(error_msg)
        stats.add_error(error_msg)
        raise

def process_lucidlink_files(conn: duckdb.DuckDBPyConnection, stats: WorkflowStats, config: Dict[str, Any]) -> None:
    """Process files using LucidLink API"""
    try:
        print("Indexing started...")
        logger.info("Initializing LucidLink API...")
        
        # Initialize API with parallel workers
        max_workers = config.get('performance', {}).get('max_workers', 10)  # Default to 10 workers if not specified
        port = config.get('lucidlink_filespace', {}).get('port', 8080)  # Default to 8080 if not specified
        logger.info(f"Connecting to LucidLink API on port {port}")
        
        async def process_files_async():
            async with LucidLinkAPI(port, max_workers=max_workers) as api:
                logger.info("Starting filesystem traversal...")
                # Get skip directories from config
                skip_directories = config.get('skip_patterns', {}).get('directories', [])
                
                # Create a batch for bulk database updates
                batch = []
                files_in_batch = 0  # Track actual files in batch
                batch_size = config['performance']['batch_size']
                now = datetime.now(timezone.utc)
                
                async for file_info in api.traverse_filesystem(skip_directories=skip_directories):
                    try:
                        logger.debug(f"Processing file: {file_info.get('name', 'unknown')}")
                        
                        # Convert API response to database format
                        db_entry = {
                            'id': file_info['id'],
                            'name': file_info['name'],  # This is the full path name (primary key)
                            'type': file_info['type'].lower(),  # Normalize type to lowercase
                            'size': file_info['size'],
                            'creation_time': file_info['creation_time'],
                            'update_time': file_info['update_time'],
                            'indexed_at': now,
                            'error_count': 0,
                            'last_error': None
                        }
                        
                        batch.append(db_entry)
                        
                        # Track stats based on normalized type
                        if db_entry['type'] == 'file':
                            stats.total_files += 1
                            stats.total_size += db_entry['size']
                            files_in_batch += 1
                        elif db_entry['type'] == 'directory':
                            stats.total_dirs += 1
                        
                        # Process batch if it reaches the configured size
                        if len(batch) >= batch_size:
                            logger.info(f"Processing batch of {len(batch)} items...")
                            bulk_upsert_files(conn, batch)
                            stats.files_updated += files_in_batch  # Only count actual files
                            batch = []
                            files_in_batch = 0
                            
                    except Exception as e:
                        error_msg = f"Error processing file {file_info.get('name', 'unknown')}: {str(e)}"
                        logger.error(error_msg)
                        stats.add_error(error_msg)
                        continue
                        
                # Process any remaining files in the final batch
                if batch:
                    logger.info(f"Processing final batch of {len(batch)} items...")
                    bulk_upsert_files(conn, batch)
                    stats.files_updated += files_in_batch  # Only count actual files
                    
                # Clean up items that no longer exist
                logger.info("Cleaning up removed files...")
                cleanup_missing_files(conn, api._all_files)  # Pass the full file info
                
        # Run the async function
        asyncio.run(process_files_async())
        print("Indexing complete!")
            
    except Exception as e:
        error_msg = f"Error in LucidLink file processing: {str(e)}"
        logger.error(error_msg)
        stats.add_error(error_msg)
        raise

def log_workflow_summary(stats: WorkflowStats) -> None:
    """Log a summary of the workflow execution."""
    # Set end time if not already set
    if stats.end_time is None:
        stats.finish()
        
    elapsed_time = stats.end_time - stats.start_time
    processing_rate = stats.total_files / elapsed_time if elapsed_time > 0 else 0
    
    logger.info("=" * 80)
    logger.info("Indexer Summary:")
    logger.info(f"Time Elapsed:     {elapsed_time:.2f} seconds")
    logger.info(f"Processing Rate:  {processing_rate:.1f} files/second")
    logger.info(f"Total Size:       {stats.total_size / (1024.0 ** 3):.4f} GB")
    logger.info(f"Total Files:      {stats.total_files}")
    logger.info(f"Total Dirs:       {stats.total_dirs}")
    logger.info(f"Files Updated:    {stats.files_updated}")
    logger.info(f"Files Skipped:    {stats.files_skipped}")
    logger.info(f"Files Removed:    {stats.files_removed}")
    logger.info(f"Total Errors:     {len(stats.errors)}")
    
    if stats.files_removed > 0:
        logger.info("Removed Files:")
        for path in stats.removed_paths:
            logger.info(f"  - {path}")
    
    if stats.errors:
        logger.info("Errors:")
        for error in stats.errors:
            logger.info(f"  - {error}")
    
    logger.info("=" * 80)

def get_database_stats(session: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get database statistics."""
    return get_database_stats(session)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='File System Indexer')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--root-path', help='Root path to index')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    configure_logging(config)
    
    # Override root path if provided via command line
    if args.root_path:
        config['root_path'] = args.root_path
        
    # Initialize database
    db_path = os.path.join(os.path.dirname(__file__), 'fs_index.duckdb')
    conn = init_database(db_path)
    
    try:
        # Initialize stats
        stats = WorkflowStats()
        
        # Process files
        process_lucidlink_files(conn, stats, config)
        
        # Log summary
        stats.finish()
        log_workflow_summary(stats)
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error in main workflow: {str(e)}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()