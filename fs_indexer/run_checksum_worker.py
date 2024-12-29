"""Script to run the checksum worker process."""
import atexit
import logging
import signal
import sys

from fs_indexer.checksum_worker import run_worker
from fs_indexer.config import init_logging
from fs_indexer.main import start_redis_server, stop_redis_server

if __name__ == "__main__":
    init_logging()
    logger = logging.getLogger(__name__)
    
    # Start Redis server
    redis_process = start_redis_server()
    
    # Register cleanup handlers
    atexit.register(lambda: stop_redis_server(redis_process))
    signal.signal(signal.SIGINT, lambda sig, frame: (
        stop_redis_server(redis_process),
        sys.exit(0)
    ))
    
    logger.info("Starting checksum worker...")
    run_worker()
