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

# Initialize basic logging first
logger = logging.getLogger(__name__)

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
    
    # Check file extensions
    for ext in skip_patterns.get('extensions', []):
        # Remove leading dot if present in the extension pattern
        ext = ext.lstrip('.')
        if filename.endswith(f".{ext}") or filename == ext:
            return True
            
    # Check directory patterns
    if file_info['type'] == 'directory':
        for dir_pattern in skip_patterns.get('directories', []):
            if filename == dir_pattern or filename.startswith(f"{dir_pattern}/"):
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
                now = datetime.now(timezone.utc)
                
                # Initialize OpenSearch client for deletions
                client = OpenSearchClient(
                    host=config['opensearch']['host'],
                    port=config['opensearch']['port'],
                    username=config['opensearch']['username'],
                    password=config['opensearch']['password']
                )
                
                # Delete all documents under the root path first
                logger.info(f"Cleaning up OpenSearch records under path: {relative_root_path}")
                client.delete_by_path_prefix(relative_root_path)
                
                async for file_info in api.traverse_filesystem(root_path=relative_root_path, skip_directories=skip_patterns.get('directories', [])):
                    try:
                        logger.debug(f"Processing file info: {file_info}")
                        
                        # Track current files for cleanup
                        current_files.append({'id': file_info['id']})
                        
                        # Update stats based on type
                        with stats.lock:
                            if file_info.get('type') == 'directory':
                                stats.total_dirs += 1
                            else:
                                stats.total_files += 1
                                stats.total_size += file_info.get('size', 0)
                        
                        # Add to batch
                        batch.append(file_info)
                        files_in_batch += 1
                        
                        # Process batch if full
                        if files_in_batch >= batch_size:
                            logger.info(f"Processing batch of {files_in_batch} items...")
                            bulk_upsert_files(session, batch)
                            with stats.lock:
                                stats.files_updated += files_in_batch
                            batch = []
                            files_in_batch = 0
                    except Exception as e:
                        error_msg = f"Error processing file {file_info.get('name', 'unknown')}: {str(e)}"
                        logger.error(error_msg)
                        with stats.lock:
                            stats.add_error(error_msg)
                        continue
                
                # Process any remaining files in the final batch
                if batch:
                    bulk_upsert_files(session, batch)
                    with stats.lock:
                        stats.files_updated += files_in_batch
                
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
                    
                    # Delete from OpenSearch
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

def send_data_to_opensearch(session: duckdb.DuckDBPyConnection, config: Dict[str, Any]) -> None:
    """Send file data from DuckDB to OpenSearch."""
    logger.info("DuckDB indexing complete, sending data to OpenSearch...")
    
    from fs_indexer_opensearch.opensearch_integration import OpenSearchClient
    
    client = OpenSearchClient(
        host=config['opensearch']['host'],
        port=config['opensearch']['port'],
        username=config['opensearch']['username'],
        password=config['opensearch']['password']
    )

    # Initialize direct link API client
    lucidlink_port = config['lucidlink_filespace'].get('port', 9778)
    api_base_url = f"http://127.0.0.1:{lucidlink_port}"
    
    def get_direct_link(filepath: str, is_directory: bool = False) -> Optional[str]:
        """Generate a direct link for a file using the LucidLink API."""
        try:
            # Remove leading slash and ensure proper path format
            rel_path = filepath.lstrip("/")
            
            # API endpoint and parameters
            endpoint = "fsEntry/direct-link"
            params = {
                "path": rel_path,
                "isDirectory": is_directory
            }
            
            # Make API request
            response = requests.get(
                f"{api_base_url}/{endpoint}",
                params=params,
                timeout=10
            )
            
            # Raise for non-200 responses
            response.raise_for_status()
            
            # Extract direct link from response
            return response.json().get("result")
            
        except Exception as e:
            logger.error(f"Error generating direct link for {filepath}: {str(e)}")
            return None
    
    # First, get list of IDs to delete
    delete_query = """
        SELECT id FROM lucidlink_files
        WHERE indexed_at < (
            SELECT MAX(indexed_at)
            FROM lucidlink_files
        )
    """
    to_delete = session.execute(delete_query).fetchall()
    if to_delete:
        delete_ids = [row[0] for row in to_delete]
        logger.info(f"Deleting {len(delete_ids)} files from OpenSearch")
        client.bulk_delete(delete_ids)
    
    # Read files from DuckDB in batches
    batch_size = config['performance']['batch_size']
    offset = 0
    
    while True:
        # Get a batch of files from DuckDB
        query = f"""
            SELECT id, name, relative_path, type, size, creation_time, update_time
            FROM lucidlink_files
            ORDER BY id
            LIMIT {batch_size} OFFSET {offset}
        """
        results = session.execute(query).fetchall()
        
        if not results:
            break
            
        # Prepare bulk data for OpenSearch
        bulk_data = []
        for row in results:
            # Generate direct link for the file
            is_directory = row[3] == 'directory'  # row[3] is the type
            direct_link = get_direct_link(row[2], is_directory)  # row[2] is the relative_path
            
            # Map database fields to OpenSearch fields
            doc = {
                "filepath": row[2],  # relative_path
                "name": os.path.basename(row[2]),  # Use basename of relative_path
                "size_bytes": row[4],
                "size": format_size(row[4]),  # Human readable size
                "creation_time": row[5].isoformat(),
                "modified_time": row[6].isoformat(),
                "type": row[3],  # Use type instead of file_type
                "indexed_time": datetime.now(timezone.utc).isoformat(),
                "direct_link": direct_link
            }
            
            # Add the index action and document
            bulk_data.extend([
                {"index": {"_index": "filesystem", "_id": row[0]}},  # Use LucidLink ID
                doc
            ])
        
        if bulk_data:
            # Send batch to OpenSearch
            client.send_data(bulk_data)
            logger.info(f"Sent batch of {len(results)} items to OpenSearch")
        
        offset += batch_size

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