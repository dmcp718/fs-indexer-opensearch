"""Async checksum calculation worker using Redis."""
import json
import logging
import time
from pathlib import Path

import redis
from sqlalchemy import create_engine, text

from fs_indexer.config import CONFIG
from fs_indexer.utils import calculate_checksum

logger = logging.getLogger(__name__)

class ChecksumWorker:
    """Worker for processing checksum calculations asynchronously."""
    
    def __init__(self):
        """Initialize the worker with Redis connection and database engine."""
        logger.info("Initializing checksum worker...")
        self._init_redis()
        self.db_engine = create_engine(CONFIG['database']['connection']['url'])
        self.key_prefix = CONFIG['redis']['checksum_key_prefix']
        self.ttl = CONFIG['redis']['checksum_ttl']
        logger.info(f"Worker initialized with Redis prefix: {self.key_prefix}")
    
    def _init_redis(self):
        """Initialize Redis connection with retries."""
        max_retries = 5
        retry_interval = 1
        
        for i in range(max_retries):
            try:
                self.redis = redis.Redis(
                    host=CONFIG['redis']['host'],
                    port=CONFIG['redis']['port'],
                    db=CONFIG['redis']['db'],
                    decode_responses=True
                )
                self.redis.ping()
                logger.info("Connected to Redis successfully")
                return
            except redis.ConnectionError:
                if i < max_retries - 1:
                    logger.warning(f"Failed to connect to Redis, retrying in {retry_interval}s...")
                    time.sleep(retry_interval)
                else:
                    raise Exception("Failed to connect to Redis after multiple attempts")

    def queue_file_for_checksum(self, rel_path: str, abs_path: str):
        """Queue a file for checksum calculation."""
        queue_key = f"{self.key_prefix}:queue"
        task_key = f"{self.key_prefix}:pending:{rel_path}"
        
        # First add to queue
        self.redis.sadd(queue_key, rel_path)
        
        # Then set task data
        task_data = {
            'rel_path': rel_path,
            'abs_path': str(abs_path),
            'status': 'pending',
            'queued_at': time.time()
        }
        self.redis.hmset(task_key, task_data)
        
        logger.info(f"Queued checksum calculation for {rel_path} in {queue_key}")
        logger.debug(f"Queue size after adding: {self.redis.scard(queue_key)}")

    def process_pending_checksums(self, batch_size: int = 100):
        """Process a batch of pending checksum calculations."""
        queue_key = f"{self.key_prefix}:queue"
        logger.info(f"Looking for pending checksums in {queue_key}")
        
        # Check queue size
        queue_size = self.redis.scard(queue_key)
        logger.info(f"Current queue size: {queue_size}")
        
        if queue_size == 0:
            # Check if there are any pending tasks without queue entries
            pending_pattern = f"{self.key_prefix}:pending:*"
            pending_keys = self.redis.keys(pending_pattern)
            if pending_keys:
                logger.warning(f"Found {len(pending_keys)} pending tasks but queue is empty. Requeuing...")
                for key in pending_keys:
                    rel_path = key.split(":")[-1]
                    self.redis.sadd(queue_key, rel_path)
                logger.info(f"Requeued {len(pending_keys)} tasks")
                return
        
        while True:
            # Get a batch of pending files
            pending_files = self.redis.spop(queue_key, batch_size)
            if not pending_files:
                logger.info("No more pending checksums to process")
                break

            logger.info(f"Processing {len(pending_files)} files")
            for rel_path in pending_files:
                task_key = f"{self.key_prefix}:pending:{rel_path}"
                task_data = self.redis.hgetall(task_key)
                
                if not task_data:
                    logger.warning(f"No task data found for {rel_path}")
                    continue

                abs_path = task_data['abs_path']
                logger.info(f"Calculating checksum for {abs_path}")
                
                try:
                    checksum = calculate_checksum(abs_path)
                    
                    # Update the database with the checksum
                    with self.db_engine.connect() as conn:
                        conn.execute(
                            text("UPDATE indexed_files SET checksum = :checksum WHERE rel_path = :rel_path"),
                            {"checksum": checksum, "rel_path": rel_path}
                        )
                        conn.commit()
                    logger.info(f"Updated checksum for {rel_path}: {checksum}")

                    # Update Redis task status
                    task_data.update({
                        'status': 'completed',
                        'checksum': checksum,
                        'completed_at': time.time()
                    })
                except Exception as e:
                    logger.error(f"Failed to process {abs_path}: {e}")
                    task_data.update({
                        'status': 'failed',
                        'error': str(e),
                        'completed_at': time.time()
                    })

                # Update task data and set TTL
                self.redis.hmset(task_key, task_data)
                self.redis.expire(task_key, self.ttl)

def run_worker():
    """Run the checksum worker process."""
    worker = ChecksumWorker()
    logger.info("Starting checksum worker...")
    
    try:
        while True:
            worker.process_pending_checksums()
            time.sleep(1)  # Avoid busy-waiting
    except KeyboardInterrupt:
        logger.info("Shutting down worker...")
