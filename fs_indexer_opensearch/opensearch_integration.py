from opensearchpy import OpenSearch
import logging

class OpenSearchClient:
    def __init__(self, host: str, port: int, username: str, password: str, index_name: str = "filesystem"):
        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(username, password),
            use_ssl=False,
            verify_certs=False,
            maxsize=50,
            timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )
        self.index_name = index_name
        self._ensure_index_exists()
        logging.info(f"Connected to OpenSearch at {host}:{port}")

    def _ensure_index_exists(self):
        """Ensure the index exists with proper mapping."""
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s",
                "analysis": {
                    "analyzer": {
                        "path_analyzer": {
                            "tokenizer": "path_hierarchy",
                            "char_filter": ["path_special_chars"],
                            "filter": ["lowercase", "word_delimiter_graph"]
                        },
                        "name_analyzer": {
                            "tokenizer": "standard",
                            "char_filter": ["name_special_chars"],
                            "filter": ["lowercase", "word_delimiter_graph"]
                        }
                    },
                    "char_filter": {
                        "path_special_chars": {
                            "type": "pattern_replace",
                            "pattern": "[_.]",
                            "replacement": " "
                        },
                        "name_special_chars": {
                            "type": "pattern_replace",
                            "pattern": "[_.]",
                            "replacement": " "
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "filepath": {
                        "type": "text",
                        "analyzer": "path_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "name_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "size_bytes": {"type": "long"},
                    "size": {"type": "keyword"},
                    "modified_time": {"type": "date"},
                    "creation_time": {"type": "date"},
                    "type": {"type": "keyword"},
                    "indexed_time": {"type": "date"},
                    "direct_link": {
                        "type": "keyword",
                        "meta": {
                            "fieldType": "string",
                            "openLinkInNewTab": "false",
                            "labelTemplate": "link to asset",
                            "urlTemplate": "{{value}}"
                        }
                    }
                }
            }
        }
        
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name, body=mapping)
            logging.info(f"Created index {self.index_name} with mapping")

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
