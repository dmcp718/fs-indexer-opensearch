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
from fs_indexer_opensearch.db_duckdb import init_database, bulk_upsert_files, cleanup_missing_files, get_database_stats
from fs_indexer_opensearch.lucidlink_api import LucidLinkAPI
from fs_indexer_opensearch.opensearch_integration import OpenSearchClient
import urllib.parse
import requests
from opensearchpy import helpers
import pandas as pd
import uuid

# Initialize basic logging first
logger = logging.getLogger(__name__)

# Define UUID namespace for consistent IDs
NAMESPACE_DNS = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

# Load configuration
def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from file."""
    if not config_path:
        # Try locations in order:
        config_locations = [
            'config/indexer-config.yaml',  # Project config directory
            'indexer-config.yaml',         # Current directory
            os.path.join(os.path.dirname(__file__), 'indexer-config.yaml')  # Package directory
        ]
        
        for loc in config_locations:
            if os.path.exists(loc):
                config_path = loc
                break
    
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found in any of the expected locations: {config_locations}")
    
    logger.info(f"Loading configuration from: {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    # Log the loaded configuration
    logger.info(f"Loaded configuration: {config}")
    
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
    
    # Remove any existing handlers from the root logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logger = logging.getLogger(__name__)
    logger.handlers.clear()  # Clear any existing handlers
    logger.setLevel(getattr(logging, config["logging"]["level"]))
    logger.propagate = False  # Prevent propagation to avoid duplicate logs
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(config["logging"]["file"])
    os.makedirs(log_dir, exist_ok=True)
    
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
        
def should_skip_file(file_info: Dict[str, Any], skip_patterns: Dict[str, List[str]]) -> bool:
    """Check if a file should be skipped based on skip patterns."""
    filename = file_info['name'].split('/')[-1]
    full_path = file_info['name']  # Use full path from API
    
    # Debug logging
    logger.debug(f"Checking skip patterns for: {full_path} (type: {file_info['type']})")
    
    # Check directory patterns first
    for dir_pattern in skip_patterns.get('directories', []):
        # Remove leading/trailing slashes for consistent matching
        dir_pattern = dir_pattern.strip('/')
        clean_path = full_path.strip('/')
        
        # Skip if path starts with or equals the pattern
        if clean_path.startswith(dir_pattern + '/') or clean_path == dir_pattern:
            logger.debug(f"Skipping {full_path} due to directory pattern {dir_pattern}")
            return True
    
    # Check file extensions
    for ext in skip_patterns.get('extensions', []):
        # Remove leading dot if present in the extension pattern
        ext = ext.lstrip('.')
        if filename.endswith(f".{ext}") or filename == ext:
            logger.debug(f"Skipping {full_path} due to extension pattern {ext}")
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
                    if entry.is_file() and not should_skip_file(file_info={'name': str(path), 'type': 'file'}, config=config):
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
                if entry.is_dir() and not should_skip_file(file_info={'name': str(path), 'type': 'directory'}, config=config):
                    dirs_to_scan.append(path)
                elif entry.is_file() and not should_skip_file(file_info={'name': str(path), 'type': 'file'}, config=config):
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
                            if entry.is_dir() and not should_skip_file(file_info={'name': str(path), 'type': 'directory'}, config=config):
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
            with stats.lock:
                stats.total_size += file_info["size"]  # Add file size to total
            
            # Process chunk when it reaches batch size
            if len(files_chunk) >= chunk_size:
                try:
                    # Bulk upsert the chunk
                    processed = bulk_upsert_files(session, files_chunk)
                    with stats.lock:
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
            with stats.lock:
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
        with stats.lock:
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
            with stats.lock:
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
        with stats.lock:
            stats.add_error(error_msg)
        raise

def process_lucidlink_files(session: duckdb.DuckDBPyConnection, stats: WorkflowStats, config: Dict[str, Any]) -> None:
    """Process files from LucidLink and index them."""
    try:
        # Initialize LucidLink API
        logger.info("Initializing LucidLink API...")
        logger.info(f"LucidLink config: {config['lucidlink_filespace']}")
        
        # Initialize API with parallel workers
        max_workers = config.get('performance', {}).get('max_workers', 10)
        port = config.get('lucidlink_filespace', {}).get('port', 9778)
        
        async def process_batch(session: duckdb.DuckDBPyConnection, batch: List[Dict], stats: WorkflowStats) -> None:
            """Process a batch of files by inserting them into DuckDB."""
            try:
                processed = bulk_upsert_files(session, batch)
                with stats.lock:
                    stats.files_updated += processed
                    
            except Exception as e:
                error_msg = f"Error processing batch: {str(e)}"
                logger.error(error_msg)
                with stats.lock:
                    stats.add_error(error_msg)

        async def process_files_async():
            async with LucidLinkAPI(port=port, max_workers=max_workers) as api:
                # Check API health first
                if not await api.health_check():
                    raise RuntimeError("LucidLink API is not available")
                
                logger.info("Starting filesystem traversal...")
                
                # Get full and relative root paths
                full_root_path = config.get('root_path', '')
                relative_root_path = str(Path(full_root_path).relative_to('/Volumes/dmpfs/production'))
                logger.info(f"Full root path: {full_root_path}")
                logger.info(f"Relative root path: {relative_root_path}")
                
                # Initialize variables for batch processing
                batch = []
                current_files = []  # Track current files for cleanup
                files_in_batch = 0
                skip_patterns = config.get('skip_patterns', {})
                batch_size = config['performance']['batch_size']
                
                # Initialize OpenSearch client for cleanup if needed
                client = OpenSearchClient(
                    host=config['opensearch']['host'],
                    port=config['opensearch']['port'],
                    username=config['opensearch']['username'],
                    password=config['opensearch']['password']
                )
                
                # Process files from LucidLink API using relative path
                async for file_info in api.traverse_filesystem(relative_root_path):
                    try:
                        # Debug log the file_info
                        logger.debug(f"Processing file_info: {file_info}")
                        
                        # Skip files based on patterns
                        if should_skip_file(file_info, skip_patterns):
                            with stats.lock:
                                stats.files_skipped += 1
                            continue
                        
                        # Add file to batch - ensure we have the required fields
                        if 'name' not in file_info:
                            logger.warning(f"Skipping file_info without name: {file_info}")
                            continue
                            
                        # Copy file_info to avoid modifying the original
                        batch_item = file_info.copy()
                        batch_item['relative_path'] = batch_item['name']
                        batch_item['id'] = str(uuid.uuid5(NAMESPACE_DNS, batch_item['relative_path']))
                        
                        # Update stats
                        with stats.lock:
                            if batch_item['type'] == 'directory':
                                stats.total_dirs += 1
                            else:
                                stats.total_files += 1
                                stats.total_size += batch_item.get('size', 0)
                        
                        batch.append(batch_item)
                        current_files.append({'id': batch_item['id'], 'relative_path': batch_item['relative_path']})
                        files_in_batch += 1
                        
                        # Process batch if it reaches the size limit
                        if files_in_batch >= batch_size:
                            logger.info(f"Processing batch of {files_in_batch} items...")
                            await process_batch(session, batch, stats)
                            batch = []
                            files_in_batch = 0
                            
                    except Exception as e:
                        error_msg = f"Error processing file {file_info.get('name', 'unknown')}: {str(e)}"
                        logger.error(error_msg)
                        with stats.lock:
                            stats.add_error(error_msg)
                        continue
                
                # Process remaining files in the last batch
                if files_in_batch > 0:
                    logger.info(f"Processing batch of {files_in_batch} items...")
                    await process_batch(session, batch, stats)
                
                # Clean up items that no longer exist
                logger.info("Cleaning up removed files...")
                removed_files = cleanup_missing_files(session, current_files)
                if removed_files:
                    # Split IDs and paths
                    removed_ids, removed_paths = zip(*removed_files)
                    with stats.lock:
                        stats.files_removed = len(removed_files)
                        stats.removed_paths = removed_paths
                    logger.info(f"Removed {len(removed_files)} files from DuckDB")
                    
                    # Delete from OpenSearch only if files were removed
                    client = OpenSearchClient(
                        host=config['opensearch']['host'],
                        port=config['opensearch']['port'],
                        username=config['opensearch']['username'],
                        password=config['opensearch']['password']
                    )
                    client.bulk_delete(list(removed_ids))
                
        # Run the async function to complete DuckDB indexing
        asyncio.run(process_files_async())
        
        # Send data to OpenSearch, including deletions
        send_data_to_opensearch(session, config)
        
    except Exception as e:
        error_msg = f"Error in LucidLink file processing: {str(e)}"
        logger.error(error_msg)
        with stats.lock:
            stats.add_error(error_msg)
        raise

def clean_opensearch_records(client: OpenSearchClient, root_path: str) -> None:
    """Clean up OpenSearch records under the given path"""
    try:
        logger.info(f"Cleaning up OpenSearch records under path: {root_path}")
        # Use optimized bulk delete by query
        query = {
            "term": {
                "root_path.keyword": root_path
            }
        }
        client.bulk_delete_by_query(query)
    except Exception as e:
        logger.error(f"Error cleaning up OpenSearch records: {str(e)}")
        raise

def send_data_to_opensearch(session: duckdb.DuckDBPyConnection, config: Dict[str, Any]) -> None:
    """Send file data from DuckDB to OpenSearch."""
    logger.info("DuckDB indexing complete, sending data to OpenSearch...")
    start_time = time.time()
    
    try:
        # Initialize OpenSearch client
        client = OpenSearchClient(
            host=config['opensearch']['host'],
            port=config['opensearch']['port'],
            username=config['opensearch']['username'],
            password=config['opensearch']['password']
        )
        
        # First check total records in DuckDB
        count_query = """
        SELECT COUNT(*) as total, 
               SUM(CASE WHEN type = 'directory' THEN 1 ELSE 0 END) as directories,
               SUM(CASE WHEN type = 'file' THEN 1 ELSE 0 END) as files
        FROM lucidlink_files
        WHERE indexed_at >= (
            SELECT MAX(indexed_at) - INTERVAL 1 MINUTE
            FROM lucidlink_files
        )
        """
        counts = session.execute(count_query).fetchone()
        logger.info(f"DuckDB records - Total: {counts[0]:,}, Directories: {counts[1]:,}, Files: {counts[2]:,}")
        
        # Get all files from DuckDB
        query = """
        SELECT id, name, relative_path, relative_path as full_path, size, 
               CASE WHEN type = 'directory' THEN true ELSE false END as is_directory,
               NULL as checksum, '.' as root_path, update_time as last_modified,
               type
        FROM lucidlink_files
        WHERE indexed_at >= (
            SELECT MAX(indexed_at) - INTERVAL 1 MINUTE
            FROM lucidlink_files
        )
        ORDER BY relative_path
        """
        result = session.execute(query).fetchdf()
        total_docs = len(result)
        
        if total_docs == 0:
            logger.info("No files to index in OpenSearch")
            return
            
        # Log sample of records for debugging
        logger.info(f"Sample of first 5 records:")
        for _, row in result.head().iterrows():
            logger.info(f"  {row['type']}: {row['relative_path']}")
            
        logger.info(f"Preparing to index {total_docs:,} documents to OpenSearch...")
        
        # Convert DataFrame to list of dicts for bulk indexing
        docs = []
        for _, row in result.iterrows():
            # Handle NaN values
            size = row['size']
            if pd.isna(size):
                size = 0
            
            doc = {
                "_index": client.index_name,
                "_id": str(row['id']),  # Ensure ID is string
                "_source": {
                    "file_name": row['name'],
                    "relative_path": row['relative_path'],
                    "full_path": row['full_path'],
                    "size_bytes": int(size),
                    "is_directory": bool(row['is_directory']),
                    "checksum": row['checksum'] if pd.notnull(row['checksum']) else None,
                    "root_path": row['root_path'],
                    "last_modified": row['last_modified'].isoformat() if pd.notnull(row['last_modified']) else None
                }
            }
            docs.append(doc)
            
        # Bulk index in larger batches for better performance
        batch_size = 5000
        total_indexed = 0
        total_success = 0
        total_failed = 0
        
        for i in range(0, len(docs), batch_size):
            batch = docs[i:i + batch_size]
            batch_start = time.time()
            try:
                success_count = 0
                failed_count = 0
                
                # Process bulk response
                response = helpers.bulk(
                    client.client,
                    batch,
                    chunk_size=batch_size,
                    request_timeout=300,
                    raise_on_error=False,
                    raise_on_exception=False
                )
                
                # Response is (success_count, errors_list)
                if isinstance(response, tuple):
                    success_count = response[0]
                    failed_count = len(response[1]) if len(response) > 1 and response[1] else 0
                
                total_success += success_count
                total_failed += failed_count
                total_indexed += len(batch)
                
                batch_time = time.time() - batch_start
                docs_per_sec = len(batch) / batch_time
                progress = (total_indexed / total_docs) * 100
                
                if failed_count > 0:
                    logger.warning(f"Batch indexing completed with errors - Success: {success_count:,}, Failed: {failed_count:,}")
                logger.info(f"Progress: {progress:.1f}% ({total_indexed:,}/{total_docs:,}) - {docs_per_sec:.0f} docs/sec")
                
            except Exception as e:
                logger.error(f"Error indexing batch: {str(e)}")
                total_failed += len(batch)
        
        # Log final statistics
        total_time = time.time() - start_time
        avg_rate = total_docs / total_time
        logger.info(f"OpenSearch indexing complete in {total_time:.1f}s ({avg_rate:.0f} docs/sec)")
        logger.info(f"Total documents: {total_docs:,}, Success: {total_success:,}, Failed: {total_failed:,}")
                
    except Exception as e:
        logger.error(f"Error sending data to OpenSearch: {str(e)}")
        raise

def format_size(size_bytes: int) -> str:
    """Convert size in bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            # Use 2 decimal places, but strip trailing zeros and decimal point if not needed
            return f"{size_bytes:.2f}".rstrip('0').rstrip('.') + f" {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

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
    logger.info(f"Total Size:       {format_size(stats.total_size)}")
    logger.info(f"Total Files:      {stats.total_files:,}")
    logger.info(f"Total Dirs:       {stats.total_dirs:,}")
    logger.info(f"Items Updated:    {stats.files_updated:,}")
    logger.info(f"Files Skipped:    {stats.files_skipped:,}")
    logger.info(f"Files Removed:    {stats.files_removed:,}")
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

# Initialize direct link API client
api_base_url = "http://localhost:9778"

def get_direct_link(path: str, is_directory: bool = False) -> str:
    """Get direct link for file/directory"""
    # Clean up path - handle both absolute and relative paths
    clean_path = path.strip('/')
    if clean_path.startswith('Volumes/dmpfs/production/'):
        clean_path = clean_path[len('Volumes/dmpfs/production/'):].strip('/')
    
    # Skip if path is empty
    if not clean_path:
        return ''
    
    try:
        # Disable proxy for local requests
        session = requests.Session()
        session.trust_env = False  # Don't use environment proxies
        
        # Use proven endpoint and parameters
        endpoint = "fsEntry/direct-link"
        params = {
            "path": clean_path,
            "isDirectory": is_directory
        }
        
        response = session.get(
            f"{api_base_url}/{endpoint}",
            params=params,
            timeout=10
        )
        
        # Handle 404s gracefully
        if response.status_code == 404:
            logger.debug(f"Path not found: {clean_path}")
            return ''
            
        response.raise_for_status()
        
        # Extract direct link from result field
        return response.json().get("result", "")
            
    except Exception as e:
        logger.warning(f"Failed to get direct link for {path}: {str(e)}")
        return ''
    finally:
        session.close()

def main():
    """Main entry point."""
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--config', help='Path to configuration file')
        parser.add_argument('--root-path', help='Root path to start indexing from')
        args = parser.parse_args()

        # Load configuration
        config = load_config(args.config)

        # Override root path if provided
        if args.root_path:
            config['root_path'] = args.root_path

        # Configure logging
        configure_logging(config)

        # Initialize database
        session = init_database(config['database']['connection']['url'])

        # Initialize stats
        stats = WorkflowStats()

        try:
            print("Indexing started...")
            process_lucidlink_files(session, stats, config)
        except RuntimeError as e:
            logger.error(str(e))
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error in main workflow: {str(e)}")
            sys.exit(1)
        finally:
            session.close()
            log_workflow_summary(stats)

    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()