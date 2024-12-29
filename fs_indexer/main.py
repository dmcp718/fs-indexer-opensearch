#!/usr/bin/env python3

import logging
import logging.handlers
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import xxhash
import yaml
import subprocess
import atexit
import signal
import sys
import redis
import argparse
from queue import Queue, Empty
from threading import Thread, Event, Lock
from sqlalchemy import create_engine, select, delete
from sqlalchemy.orm import sessionmaker, Session
from fs_indexer.schema import metadata, indexed_files
from fs_indexer.db_optimizations import (
    optimize_connection_pool,
    configure_sqlite,
    get_table_statistics,
    bulk_upsert_files,
    check_missing_files
)

# Initialize basic logging first
logger = logging.getLogger(__name__)

# Load configuration
def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    # Get the base path for the application
    if getattr(sys, 'frozen', False):
        # Running as compiled executable
        base_path = os.path.dirname(sys.executable)
    else:
        # Running as script
        base_path = os.path.dirname(os.path.abspath(__file__))
    
    if config_path is None:
        config_path = os.path.join(base_path, 'indexer-config.yaml')
    else:
        config_path = os.path.abspath(config_path)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create logging directory if it doesn't exist
    os.makedirs(config["logging"]["log_dir"], exist_ok=True)
    
    # Convert relative paths to absolute
    if 'root_path' in config and not os.path.isabs(config['root_path']):
        config['root_path'] = os.path.abspath(config['root_path'])
    
    if not os.path.isabs(config["database"]["connection"]["url"]):
        if config["database"]["connection"]["url"].startswith("sqlite:///"):
            # Handle SQLite URLs specially
            db_path = config["database"]["connection"]["url"][10:]
            if not os.path.isabs(db_path):
                abs_db_path = os.path.abspath(db_path)
                config["database"]["connection"]["url"] = f"sqlite:///{abs_db_path}"
    
    # Fix database URL
    db_url = config['database']['connection']['url']
    if db_url.startswith('sqlite:///'):
        # Extract the path part
        db_path = db_url[10:]
        if not os.path.isabs(db_path):
            # Make the path absolute relative to base_path
            abs_db_path = os.path.abspath(os.path.join(base_path, db_path))
            config['database']['connection']['url'] = f'sqlite:///{abs_db_path}'
    
    # Fix log directory
    if not os.path.isabs(config['logging']['log_dir']):
        config['logging']['log_dir'] = os.path.abspath(os.path.join(base_path, config['logging']['log_dir']))
    
    # Create necessary directories
    os.makedirs(os.path.dirname(config['database']['connection']['url'][10:]), exist_ok=True)
    
    # Set defaults for optional fields
    config.setdefault("batch_size", 10000)
    config.setdefault("max_workers", 8)
    config.setdefault("read_buffer_size", 131072)
    config.setdefault("skip_hidden", True)
    config.setdefault("enable_checksum", False)  # Default to not calculate checksums
    config.setdefault("checksum_mode", "sync")  # Default to sync checksum calculation
    config.setdefault("scan_chunk_size", 1000)  # Default scan chunk size
    
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
    log_file = os.path.join(config["logging"]["log_dir"], config["logging"]["filename"])
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=config["logging"]["rotation"]["max_bytes"],
        backupCount=config["logging"]["rotation"]["backup_count"],
    )
    file_handler.setFormatter(logging.Formatter(config["logging"]["format"]))
    logger.addHandler(file_handler)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(config["logging"]["format"]))
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
        self.total_files = 0
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
            
        hasher = xxhash.xxh64()
        with open(file_path, "rb") as f:
            try:
                while True:
                    data = f.read(buffer_size)
                    if not data:
                        break
                    hasher.update(data)
                return hasher.hexdigest()
            except IOError as e:
                logger.error(f"Error reading file during checksum calculation: {file_path}: {str(e)}")
                raise
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

def process_files_worker(queue: Queue, stop_event: Event, session: Session, stats: WorkflowStats, config: Dict[str, Any]):
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
                    session.commit()
                    
                    # Update stats
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
            session.commit()
            stats.files_updated += processed
            chunk_duration = time.time() - chunk_start_time
            chunk_rate = processed / chunk_duration if chunk_duration > 0 else 0
            logger.info(f"Database batch: {processed} files committed")
            logger.info(f"Processed {stats.files_updated}/{stats.total_files} files ({chunk_rate:.1f} files/s in last chunk)")
        except Exception as e:
            logger.error(f"Error processing final chunk: {e}")

def batch_update_database(session: Session, files_batch: List[Dict[str, Any]]) -> None:
    """Update database with a batch of files."""
    try:
        processed = bulk_upsert_files(session, files_batch)
        if processed != len(files_batch):
            logger.warning(f"Processed {processed} files out of {len(files_batch)} in batch")
            
    except Exception as e:
        error_msg = f"Error updating database: {e}"
        logger.error(error_msg)
        raise

def process_files(session: Session, files: List[Dict], stats: WorkflowStats, config: Dict[str, Any]) -> None:
    """Process a list of files and update the database."""
    try:
        total_files = len(files)
        stats.total_files = total_files
        
        # Process files in batches
        engine = session.get_bind()  # Get the engine from session
        session = Session(bind=engine)  # Create new session bound to engine
        try:
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
            
            # Final commit after all batches
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing files: {e}")
            raise
        finally:
            session.close()
            
    except Exception as e:
        error_msg = f"Critical error in process_files: {str(e)}"
        logger.error(error_msg)
        stats.add_error(error_msg)
        raise

def start_redis_server():
    """Start the local Redis server."""
    try:
        # Check if Redis is already running
        redis_client = redis.Redis(
            host=CONFIG['redis']['host'],
            port=CONFIG['redis']['port'],
            db=CONFIG['redis']['db']
        )
        redis_client.ping()
        logger.info("Redis server is already running")
        return None
    except redis.ConnectionError:
        # Start Redis server if not running
        redis_process = subprocess.Popen(
            ['redis-server', '--port', str(CONFIG['redis']['port'])],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for Redis to start
        max_retries = 5
        retry_interval = 1
        for i in range(max_retries):
            try:
                redis_client = redis.Redis(
                    host=CONFIG['redis']['host'],
                    port=CONFIG['redis']['port'],
                    db=CONFIG['redis']['db']
                )
                redis_client.ping()
                logger.info("Redis server started successfully")
                return redis_process
            except redis.ConnectionError:
                if i < max_retries - 1:
                    time.sleep(retry_interval)
                else:
                    raise Exception("Failed to start Redis server")

def stop_redis_server(redis_process):
    """Stop the Redis server gracefully."""
    if redis_process:
        logger.info("Stopping Redis server...")
        redis_process.terminate()
        try:
            redis_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            redis_process.kill()
        logger.info("Redis server stopped")

def log_workflow_summary(stats: WorkflowStats) -> None:
    """Log a summary of the workflow."""
    logger.info("=" * 80)
    logger.info("Workflow Summary:")
    logger.info(f"Time Elapsed:     {stats.end_time - stats.start_time:.2f} seconds")
    logger.info(f"Processing Rate:  {stats.total_files / (stats.end_time - stats.start_time):.1f} files/second")
    logger.info(f"Total Size:       {stats.total_size / (1024.0 ** 3):.4f} GB")
    logger.info(f"Total Files:      {stats.total_files}")
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

def get_database_stats(session: Session) -> Dict[str, Any]:
    """Get database statistics."""
    return get_table_statistics(session)

def main():
    """Main entry point."""
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='File System Indexer')
        parser.add_argument('--root-path', type=str, help='Root path to start indexing from')
        parser.add_argument('--config', type=str, help='Path to config file')
        args = parser.parse_args()
        
        # Load config and override root_path if provided via command line
        global CONFIG
        CONFIG = load_config(args.config if args.config else None)
        
        if args.root_path:
            CONFIG['root_path'] = args.root_path
        elif 'root_path' not in CONFIG:
            parser.error('Root path must be provided either via --root-path argument or in config file')
        
        # Configure logging with full settings from config
        configure_logging(CONFIG)
        
        logger.info("Starting workflow...")
        logger.info(f"Starting indexing from: {CONFIG['root_path']}")
        logger.info(f"Checksum mode: {CONFIG.get('checksum_mode', 'disabled')}")
        
        stats = WorkflowStats()
        root_path = Path(CONFIG["root_path"])
        
        # Initialize database session
        engine = create_engine(CONFIG["database"]["connection"]["url"])
        engine = optimize_connection_pool(engine, CONFIG)
        
        # Configure SQLite for better performance
        if engine.url.drivername == "sqlite":
            configure_sqlite(engine)
        
        # Create tables and indexes
        metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Initialize queue and worker thread
            file_queue = Queue(maxsize=500000)  # Buffer up to 500K files
            stop_event = Event()
            worker_thread = Thread(
                target=process_files_worker,
                args=(file_queue, stop_event, session, stats, CONFIG)
            )
            worker_thread.start()
            
            # Process files as they're scanned
            stats.total_files = 0
            for file_info in scan_directory_parallel(root_path, CONFIG, max_workers=CONFIG["max_workers"]):
                stats.total_files += 1
                file_queue.put(file_info)
            
            # Signal worker to stop and wait for completion
            stop_event.set()
            worker_thread.join()
            
            # Check for missing files if enabled
            if CONFIG.get("check_missing_files", False):
                logger.info("Checking for missing files...")
                removed_count, removed_paths = check_missing_files(session, root_path)
                stats.add_removed_files(removed_count, removed_paths)
            
            # Log workflow summary
            stats.end_time = time.time()
            log_workflow_summary(stats)
            
        except Exception as e:
            logger.error(f"Error in workflow: {e}")
            raise
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()