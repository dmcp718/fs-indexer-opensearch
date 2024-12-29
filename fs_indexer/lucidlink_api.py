#!/usr/bin/env python3

import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Generator, Any
from urllib.parse import quote
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class LucidLinkAPI:
    """Handler for LucidLink Filespace API interactions"""
    
    def __init__(self, port: int):
        """Initialize the API handler with the filespace port"""
        self.base_url = f"http://127.0.0.1:{port}/files"
        # Create a session for connection pooling
        self.session = requests.Session()
        # Configure retry strategy
        retry_strategy = requests.adapters.Retry(
            total=3,  # number of retries
            backoff_factor=0.5,  # wait 0.5, 1, 2 seconds between retries
            status_forcelist=[500, 502, 503, 504]  # retry on these status codes
        )
        adapter = requests.adapters.HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # number of connection pools
            pool_maxsize=100  # max size of each pool
        )
        self.session.mount("http://", adapter)
        
    def _convert_timestamp(self, ns_timestamp: int) -> datetime:
        """Convert nanosecond epoch timestamp to datetime object"""
        # Convert nanoseconds to seconds
        seconds = ns_timestamp / 1e9
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
        
    def _make_request(self, path: str = "") -> Dict[str, Any]:
        """Make HTTP request to the API"""
        url = f"{self.base_url}/{quote(path.lstrip('/'))}" if path else self.base_url
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"API response for {path}: {data}")
            return data
        except requests.RequestException as e:
            logger.error(f"API request failed for path {path}: {str(e)}")
            raise
            
    def get_top_level_directories(self) -> List[Dict[str, Any]]:
        """Get list of top-level directories"""
        try:
            data = self._make_request()
            # Process timestamps and normalize types
            for item in data:
                item['creation_time'] = self._convert_timestamp(item['creationTime'])
                item['update_time'] = self._convert_timestamp(item['updateTime'])
                # Ensure type is normalized
                item['type'] = item.get('type', '').lower()
            return data
        except Exception as e:
            logger.error(f"Failed to get top-level directories: {str(e)}")
            raise
            
    def get_directory_contents(self, directory: str) -> List[Dict[str, Any]]:
        """Get contents of a specific directory"""
        try:
            data = self._make_request(directory)
            # Process timestamps and normalize types
            for item in data:
                item['creation_time'] = self._convert_timestamp(item['creationTime'])
                item['update_time'] = self._convert_timestamp(item['updateTime'])
                # Ensure type is normalized
                item['type'] = item.get('type', '').lower()
            return data
        except Exception as e:
            logger.error(f"Failed to get contents of directory {directory}: {str(e)}")
            raise
            
    def _traverse_directory(self, directory: str, skip_directories=None) -> Generator[Dict[str, Any], None, None]:
        """Traverse a single directory tree"""
        stack = [directory]
        while stack:
            current_dir = stack.pop()
            contents = self.get_directory_contents(current_dir)
            
            for item in contents:
                # Skip if it's a directory in the skip list
                if skip_directories and item['type'] == 'directory':
                    dir_name = item['name'].lstrip('/')  # Remove leading slash
                    if dir_name in skip_directories:
                        continue
                        
                yield item
                if item['type'] == 'directory' and (not skip_directories or item['name'].lstrip('/') not in skip_directories):
                    stack.append(item['name'])
                    
    def traverse_filesystem(self, skip_directories=None, max_workers=10) -> Generator[Dict[str, Any], None, None]:
        """Traverse the entire filesystem using the API in parallel"""
        try:
            # Get top-level directories
            top_level = self.get_top_level_directories()
            
            # First yield all top-level entries
            for entry in top_level:
                # Skip if it's a directory in the skip list
                if skip_directories and entry['type'] == 'directory':
                    dir_name = entry['name'].lstrip('/')
                    if dir_name in skip_directories:
                        continue
                yield entry
            
            # Then process directories in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Create tasks for each top-level directory
                futures = []
                for entry in top_level:
                    if entry['type'] == 'directory' and (not skip_directories or entry['name'].lstrip('/') not in skip_directories):
                        future = executor.submit(self._traverse_directory, entry['name'], skip_directories)
                        futures.append(future)
                
                # Process results as they complete
                for future in as_completed(futures):
                    try:
                        for item in future.result():
                            yield item
                    except Exception as e:
                        logger.error(f"Error in parallel traversal: {str(e)}")
                        continue
