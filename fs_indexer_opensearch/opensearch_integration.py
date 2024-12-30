from opensearchpy import OpenSearch, helpers
import logging
from typing import List, Dict, Any
import urllib3
from datetime import datetime
from dateutil import tz

class OpenSearchClient:
    def __init__(self, host: str, port: int, username: str, password: str, index_name: str = "filesystem"):
        """Initialize OpenSearch client"""
        self.index_name = index_name
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=(username, password),
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=300,  # 5 minutes
            retry_on_timeout=True,
            max_retries=3
        )
        self._ensure_index_exists()
        logging.info(f"Connected to OpenSearch at {host}:{port}")

    def _ensure_index_exists(self):
        """Ensure the index exists with proper mapping."""
        mapping = self._create_index_mapping()
        
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name, body=mapping)
            logging.info(f"Created index {self.index_name} with mapping")

    def _create_index_mapping(self):
        """Create the index mapping for filesystem data."""
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s",
                "analysis": {
                    "analyzer": {
                        "path_analyzer": {
                            "tokenizer": "path_tokenizer",
                            "filter": ["lowercase"]
                        }
                    },
                    "tokenizer": {
                        "path_tokenizer": {
                            "type": "pattern",
                            "pattern": "[/\\\\]"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "filepath": {"type": "text"},
                    "name": {"type": "text"},
                    "extension": {"type": "keyword"},
                    "size_bytes": {"type": "long"},
                    "size": {"type": "text"},
                    "modified_time": {"type": "date"},
                    "creation_time": {"type": "date"},
                    "type": {"type": "keyword"},
                    "indexed_time": {"type": "date"},
                    "direct_link": {"type": "keyword", "null_value": "NULL"}
                }
            }
        }
        return mapping

    def send_data(self, data: list):
        try:
            if not data:
                logging.warning("No data to send to OpenSearch")
                return
                
            response = self.client.bulk(index=self.index_name, body=data)
            if response.get('errors', False):
                logging.error(f"Bulk operation had errors: {response}")
            else:
                logging.info(f"Successfully indexed {len(data)//2} documents")
        except Exception as e:
            logging.error(f"Failed to send data to OpenSearch: {e}")
            raise

    def delete_by_ids(self, ids: List[str]) -> None:
        """Delete documents from OpenSearch by their IDs."""
        if not ids:
            return
            
        # Prepare bulk delete actions
        actions = []
        for doc_id in ids:
            actions.extend([
                {"delete": {"_index": self.index_name, "_id": doc_id}}
            ])
            
        if actions:
            try:
                # Send bulk delete request
                response = self.client.bulk(body=actions, refresh=True)
                
                # Check for errors
                if response.get('errors', False):
                    errors = [item['delete']['error'] for item in response['items'] if 'error' in item['delete']]
                    logging.error(f"Errors during bulk delete: {errors}")
                else:
                    logging.info(f"Successfully deleted {len(ids)} documents from OpenSearch")
                    
            except Exception as e:
                logging.error(f"Failed to delete documents from OpenSearch: {str(e)}")
                raise

    def bulk_delete(self, ids: List[str]) -> None:
        """Delete multiple documents by their IDs."""
        if not ids:
            return
            
        try:
            # Prepare bulk delete actions
            actions = [
                {
                    "_op_type": "delete",
                    "_index": self.index_name,
                    "_id": doc_id
                }
                for doc_id in ids
            ]
            
            # Execute bulk operation using helpers.bulk instead of streaming_bulk
            success, errors = helpers.bulk(
                client=self.client,
                actions=actions,
                chunk_size=500,
                raise_on_error=False,
                stats_only=True
            )
            
            if errors:
                logging.warning(f"Bulk delete completed with errors - Success: {success}, Failed: {errors}")
            else:
                logging.info(f"Bulk delete completed successfully - Deleted: {success} documents")
            
        except Exception as e:
            logging.error(f"Bulk delete failed: {str(e)}")
            raise

    def bulk_delete_files(self, files: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Delete files in bulk from OpenSearch"""
        if not files:
            return

        total_files = len(files)
        success_count = 0
        failed_count = 0
        
        # Process in smaller batches to avoid overwhelming OpenSearch
        for i in range(0, total_files, batch_size):
            batch = files[i:i + batch_size]
            bulk_data = []
            
            for file in batch:
                bulk_data.append({
                    "delete": {
                        "_index": self.index_name,
                        "_id": file.get('id')
                    }
                })
            
            try:
                response = self.client.bulk(body=bulk_data, index=self.index_name)
                if response.get('errors', False):
                    # Count individual successes/failures
                    for item in response['items']:
                        if 'delete' in item and item['delete']['status'] in [200, 201]:
                            success_count += 1
                        else:
                            failed_count += 1
                            logging.warning(f"Failed to delete item: {item}")
                else:
                    success_count += len(batch)
            except Exception as e:
                logging.error(f"Error in bulk delete batch: {str(e)}")
                failed_count += len(batch)
                
            # Log progress
            logging.info(f"Bulk delete progress: {i + len(batch)}/{total_files} files processed")
        
        if failed_count > 0:
            logging.warning(f"Bulk delete completed with some errors - Success: {success_count}, Failed: {failed_count}")
        else:
            logging.info(f"Bulk delete completed successfully - {success_count} files deleted")

    def bulk_delete_by_query(self, query: Dict[str, Any]) -> None:
        """Delete documents matching query in bulk"""
        try:
            # First get count
            count = self.client.count(index=self.index_name, body={"query": query})
            total = count.get('count', 0)
            if total == 0:
                logging.info("No documents to delete")
                return

            logging.info(f"Found {total} documents to delete")
            
            # Use scan to get all document IDs
            docs = helpers.scan(
                client=self.client,
                index=self.index_name,
                query={"query": query},
                _source=False,  # Only get IDs
                size=5000
            )
            
            # Batch delete documents
            batch_size = 5000
            batch = []
            success_count = 0
            failed_count = 0
            
            for doc in docs:
                batch.append({
                    "_op_type": "delete",
                    "_index": self.index_name,
                    "_id": doc.get('_id')
                })
                
                if len(batch) >= batch_size:
                    try:
                        success, failed = helpers.bulk(
                            self.client,
                            batch,
                            chunk_size=batch_size,
                            request_timeout=300,
                            raise_on_error=False,
                            raise_on_exception=False
                        )
                        success_count += success
                        failed_count += failed
                        logging.info(f"Deleted batch of {success} documents, {failed} failed. Progress: {success_count}/{total}")
                    except Exception as e:
                        logging.error(f"Error deleting batch: {str(e)}")
                        failed_count += len(batch)
                    batch = []
            
            # Delete remaining documents
            if batch:
                try:
                    success, failed = helpers.bulk(
                        self.client,
                        batch,
                        chunk_size=batch_size,
                        request_timeout=300,
                        raise_on_error=False,
                        raise_on_exception=False
                    )
                    success_count += success
                    failed_count += failed
                    logging.info(f"Deleted final batch of {success} documents, {failed} failed. Total: {success_count}/{total}")
                except Exception as e:
                    logging.error(f"Error deleting final batch: {str(e)}")
                    failed_count += len(batch)
                    
            if failed_count > 0:
                logging.warning(f"Bulk delete completed with some failures - Success: {success_count}, Failed: {failed_count}")
            else:
                logging.info(f"Bulk delete completed successfully - Total deleted: {success_count}")
                    
        except Exception as e:
            logging.error(f"Bulk delete failed: {str(e)}")
            raise

    def delete_by_path_prefix(self, path_prefix: str) -> None:
        """Delete all documents with filepath starting with the given prefix."""
        try:
            # Build the query
            query = {
                "query": {
                    "prefix": {
                        "filepath": path_prefix
                    }
                }
            }
            
            # Delete by query
            response = self.client.delete_by_query(
                index=self.index_name,
                body=query,
                conflicts="proceed",  # Continue even if there are version conflicts
                refresh=True  # Refresh the index immediately
            )
            
            deleted = response.get('deleted', 0)
            logging.info(f"Deleted {deleted} documents with path prefix '{path_prefix}'")
            
        except Exception as e:
            logging.error(f"Failed to delete documents by path prefix: {str(e)}")
            raise

    def _create_document(self, file_data: Dict) -> Dict:
        """Create an OpenSearch document from file data."""
        doc = {
            "filepath": file_data.get("relative_path"),
            "name": file_data.get("name"),
            "extension": file_data.get("extension"),
            "size_bytes": file_data.get("size", 0),  # Default to 0 if size is missing
            "size": file_data.get("size", 0),  # Default to 0 if size is missing
            "modified_time": datetime.fromtimestamp(
                file_data.get("update_time", 0) / 1e9, tz=tz.tzutc()
            ).isoformat(),
            "creation_time": datetime.fromtimestamp(
                file_data.get("create_time", 0) / 1e9, tz=tz.tzutc()
            ).isoformat(),
            "type": file_data.get("type"),
            "indexed_time": datetime.now(tz=tz.tzutc()).isoformat(),
            "direct_link": file_data.get("direct_link")
        }
        return doc
