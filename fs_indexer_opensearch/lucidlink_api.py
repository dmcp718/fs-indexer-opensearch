#!/usr/bin/env python3

import logging
import aiohttp
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Generator, Any, Set
from urllib.parse import quote
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import time

logger = logging.getLogger(__name__)

class LucidLinkAPI:
    """Handler for LucidLink Filespace API interactions"""
    
    def __init__(self, port: int, max_workers: int = 10):
        """Initialize the API handler with the filespace port"""
        self.base_url = f"http://127.0.0.1:{port}/files"
        self.max_workers = max_workers
        self.session = None
        self._seen_paths = set()  # Track seen paths to avoid duplicates
        self._dir_cache = {}  # Cache for directory contents
        self._cache_ttl = 60  # Cache TTL in seconds
        self._prefetch_depth = 1  # Reduced prefetch depth
        self._request_semaphore = None  # For rate limiting
        self._max_concurrent_requests = 5  # Max concurrent requests
        self._retry_attempts = 3
        self._retry_delay = 1  # seconds
        
    async def __aenter__(self):
        """Async context manager entry"""
        conn = aiohttp.TCPConnector(
            limit=self._max_concurrent_requests,
            ttl_dns_cache=300,
            limit_per_host=self._max_concurrent_requests
        )
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            connector=conn,
            timeout=timeout,
            raise_for_status=True
        )
        self._request_semaphore = asyncio.Semaphore(self._max_concurrent_requests)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            
    def _convert_timestamp(self, ns_timestamp: int) -> datetime:
        """Convert nanosecond epoch timestamp to datetime object"""
        seconds = ns_timestamp / 1e9
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
        
    def _is_cache_valid(self, cache_entry):
        """Check if a cache entry is still valid"""
        if not cache_entry:
            return False
        cache_time, update_time, _ = cache_entry
        current_time = time.time()
        return (current_time - cache_time) < self._cache_ttl
        
    async def _make_request(self, path: str = "") -> Dict[str, Any]:
        """Make async HTTP request to the API with retries and rate limiting"""
        url = f"{self.base_url}/{quote(path.lstrip('/'))}" if path else self.base_url
        
        for attempt in range(self._retry_attempts):
            try:
                async with self._request_semaphore:
                    async with self.session.get(url) as response:
                        data = await response.json()
                        logger.debug(f"API response for {path}: {data}")
                        return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == self._retry_attempts - 1:
                    logger.error(f"API request failed for path {path}: {str(e)}")
                    raise
                await asyncio.sleep(self._retry_delay * (attempt + 1))
                
    async def _prefetch_directories(self, directories: List[str], depth: int):
        """Prefetch directory contents in background with rate limiting"""
        if depth <= 0 or not directories:
            return
            
        # Limit number of directories to prefetch
        directories = directories[:5]  # Only prefetch up to 5 directories at a time
        
        tasks = []
        for directory in directories:
            if directory not in self._dir_cache:
                tasks.append(self.get_directory_contents(directory))
                
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Start prefetching next level
            next_level = []
            for result in results:
                if isinstance(result, Exception):
                    continue
                for item in result:
                    if item['type'] == 'directory':
                        next_level.append(item['name'])
                        
            if next_level:
                await asyncio.sleep(0.1)  # Add small delay between levels
                asyncio.create_task(self._prefetch_directories(next_level[:5], depth - 1))
                
    async def get_directory_contents(self, directory: str) -> List[Dict[str, Any]]:
        """Get contents of a specific directory with caching"""
        # Check cache first
        cache_entry = self._dir_cache.get(directory)
        if self._is_cache_valid(cache_entry):
            _, update_time, contents = cache_entry
            return contents
            
        try:
            data = await self._make_request(directory)
            for item in data:
                item['creation_time'] = self._convert_timestamp(item['creationTime'])
                item['update_time'] = self._convert_timestamp(item['updateTime'])
                item['type'] = item.get('type', '').lower()
                
            # Cache the results
            self._dir_cache[directory] = (time.time(), time.time(), data)
            
            # Start prefetching child directories
            child_dirs = [item['name'] for item in data if item['type'] == 'directory']
            if child_dirs:
                asyncio.create_task(self._prefetch_directories(child_dirs[:5], self._prefetch_depth - 1))
                
            return data
        except Exception as e:
            logger.error(f"Failed to get contents of directory {directory}: {str(e)}")
            raise
            
    async def get_top_level_directories(self) -> List[Dict[str, Any]]:
        """Get list of top-level directories"""
        try:
            data = await self._make_request()
            for item in data:
                item['creation_time'] = self._convert_timestamp(item['creationTime'])
                item['update_time'] = self._convert_timestamp(item['updateTime'])
                item['type'] = item.get('type', '').lower()
            return data
        except Exception as e:
            logger.error(f"Failed to get top-level directories: {str(e)}")
            raise
            
    async def _batch_get_directories(self, directories: List[str], semaphore: asyncio.Semaphore) -> Dict[str, List[Dict[str, Any]]]:
        """Get contents of multiple directories concurrently with rate limiting"""
        results = {}
        tasks = []
        
        for directory in directories:
            if directory in self._seen_paths:
                continue
            self._seen_paths.add(directory)
            
            tasks.append(self._get_directory_with_semaphore(directory, semaphore))
            
        completed = await asyncio.gather(*tasks, return_exceptions=True)
        
        for directory, result in zip(directories, completed):
            if isinstance(result, Exception):
                logger.error(f"Failed to get contents of {directory}: {str(result)}")
                results[directory] = []
            else:
                results[directory] = result
                
        return results
        
    async def _get_directory_with_semaphore(self, directory: str, semaphore: asyncio.Semaphore) -> List[Dict[str, Any]]:
        """Get directory contents with rate limiting"""
        async with semaphore:
            return await self.get_directory_contents(directory)
            
    def _calculate_batch_size(self, depth: int) -> int:
        """Calculate optimal batch size based on directory depth"""
        if depth <= 1:
            return 100  # More parallel at top level
        elif depth <= 3:
            return 50   # Medium parallelism for middle levels
        else:
            return 25   # Less parallelism for deep directories
            
    async def traverse_filesystem(self, root_path: str = None, skip_directories: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Traverse the filesystem and yield file/directory info"""
        skip_directories = skip_directories or []
        
        try:
            # Get initial directory listing from root_path
            data = await self._make_request(root_path if root_path else "")
            if not data:
                logger.error("No entries found in response")
                return
                
            # Process each entry
            for entry in data:
                if entry['type'] == 'directory':
                    if entry['name'] not in skip_directories:
                        # Process directory
                        yield entry
                        # Recursively process contents using relative path
                        next_path = f"{root_path}/{entry['name']}" if root_path else entry['name']
                        async for item in self.traverse_filesystem(next_path, skip_directories):
                            yield item
                else:
                    yield entry
                    
        except Exception as e:
            logger.error(f"Error traversing filesystem: {str(e)}")
            raise
            
    def get_all_files(self) -> List[Dict[str, Any]]:
        """Get all files and directories that were traversed"""
        return self._all_files
