from sqlalchemy import (
    MetaData, Table, Column, Integer, String, 
    DateTime, Text, func, Float
)
from enum import Enum

metadata = MetaData()

class IndexStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

# Table to store the global root path
global_config = Table(
    'global_config',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('root_path', String, nullable=False),
    Column('updated_at', DateTime(timezone=True), server_default=func.now(), nullable=False)
)

# Main table for indexed files
indexed_files = Table(
    'indexed_files',
    metadata,
    Column('rel_path', String, primary_key=True),
    Column('size', Integer),
    Column('modified_at', DateTime(timezone=True)),
    Column('checksum', String),
    Column('indexed_at', DateTime(timezone=True)),
    Column('error_count', Integer, default=0),
    Column('last_error', Text),
    Column('status', String)
)

# Table for indexing metrics
indexing_metrics = Table(
    'indexing_metrics',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('timestamp', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('total_files', Integer, nullable=False),
    Column('total_size', Integer, nullable=False),
    Column('processed_files', Integer, nullable=False),
    Column('failed_files', Integer, nullable=False),
    Column('processing_time', Integer, nullable=False),  # in milliseconds
    Column('avg_file_size', Integer),
    Column('avg_processing_rate', Float),  # files per second
    Column('batch_id', String, nullable=False)  # unique identifier for each indexing run
)

# Table for detailed error tracking
indexing_errors = Table(
    'indexing_errors',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('timestamp', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('file_path', String, nullable=False),
    Column('error_type', String, nullable=False),
    Column('error_message', String, nullable=False),
    Column('batch_id', String, nullable=False),
    Column('retry_count', Integer, server_default='0', nullable=False)
)

# Table for LucidLink files
lucidlink_files = Table(
    'lucidlink_files',
    metadata,
    Column('id', String, nullable=False),  # LucidLink file/directory ID
    Column('name', String, primary_key=True),  # Full path name
    Column('type', String, nullable=False),  # 'file' or 'directory'
    Column('size', Integer, nullable=False),
    Column('creation_time', DateTime(timezone=True), nullable=False),
    Column('update_time', DateTime(timezone=True), nullable=False),
    Column('indexed_at', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('last_error', Text),
    Column('error_count', Integer, default=0)
)
