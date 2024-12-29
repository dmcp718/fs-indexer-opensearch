import os
import yaml
import logging
import logging.handlers

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'indexer-config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

CONFIG = load_config()

def init_logging():
    """Initialize logging configuration."""
    log_config = CONFIG.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log directory is configured
    if log_config.get('log_dir'):
        os.makedirs(log_config['log_dir'], exist_ok=True)
        log_file = os.path.join(log_config['log_dir'], log_config.get('filename', 'fs_indexer.log'))
        
        # Configure rotating file handler
        rotation_config = log_config.get('rotation', {})
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=rotation_config.get('max_bytes', 10485760),  # 10MB default
            backupCount=rotation_config.get('backup_count', 5)
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
