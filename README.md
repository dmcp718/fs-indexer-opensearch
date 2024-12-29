# FS Indexer

A high-performance file system indexer with SQLite backend. This tool efficiently scans and indexes file system metadata, supporting both synchronous and asynchronous checksum calculation.

## Features

- Fast file system scanning
- SQLite database for efficient storage and querying
- Optional asynchronous checksum calculation using Redis
- Configurable batch processing
- Progress tracking and performance metrics

## Installation

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

2. Install the package:
```bash
pip install -e .
```

## Configuration

Configure the indexer using `indexer-config.yaml`. Key settings include:
- Source directory to index
- SQLite database path
- Checksum calculation mode (disabled/sync/async)
- Redis configuration for async mode
- Batch sizes and other performance settings

## Usage

### Command Line Arguments

```bash
# Show help
python -m fs_indexer.main --help

# Index a directory
python -m fs_indexer.main --root-path /path/to/index

# Use a custom config file
python -m fs_indexer.main --config /path/to/config.yaml --root-path /path/to/index
```

### Basic indexing (without checksums):
```bash
python -m fs_indexer.main
```

### With async checksum calculation:
   
a. Start the Redis server and worker process:
```bash
python -m fs_indexer.run_checksum_worker
```

b. In another terminal, run the main indexer:
```bash
python -m fs_indexer.main
```

The indexer will display progress and performance metrics during operation, including:
- Files processed per second
- Total size of indexed files
- Number of files updated/skipped/removed
- Any errors encountered

## Building the Application

### Prerequisites
- Python 3.8+
- Virtual environment
- PyInstaller (installed via requirements.txt)

### Platform-Specific Builds

PyInstaller creates executables for the platform it runs on. To create builds for different platforms:

#### macOS Build
On a macOS system:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python build_app.py
```
The macOS executable will be created in `dist/fs-indexer-darwin/`

#### Linux Build
On a Linux system:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python build_app.py
```
The Linux executable will be created in `dist/fs-indexer-linux/`

#### Windows 11 Build
On a Windows 11 system:
```powershell
# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\activate

# Install requirements
pip install -r requirements.txt

# Install pywin32 (Windows-specific requirement)
pip install pywin32

# Run the build script
python build_app.py
```
The Windows executable will be created in `dist\fs-indexer-win\`

Note: On Windows, you may need to:
1. Install Python 3.8+ from the [official Python website](https://www.python.org/downloads/)
2. Add Python to your system PATH during installation
3. Install Visual Studio Build Tools with C++ workload for SQLite compilation
4. Run PowerShell or Command Prompt as Administrator if you encounter permission issues

Note: You must build on each target platform separately. Cross-compilation is not supported by PyInstaller.

### Running the Application

From the `dist/fs-indexer-darwin` directory:

```bash
# Using the executable directly
./fs-indexer --root-path /path/to/index

# Using the helper script
./run-indexer.sh --root-path /path/to/index
```

### Command Line Arguments

- `--root-path PATH`: Specify the root directory to index
- `--config PATH`: Optional path to custom config file

## Building Standalone Executables

To create a self-contained executable for your platform:

1. Install development dependencies:
```bash
pip install -e ".[dev]"
```

2. Run the build script:
```bash
./build_app.py
```

This will create a platform-specific release package in the `dist/fs-indexer-{platform}` directory containing:
- The standalone executable
- Configuration file (indexer-config.yaml)
- Run script (run-indexer.sh)

The executable can be run on any machine of the same platform without requiring Python or any dependencies to be installed.

To run the standalone version:
```bash
cd dist/fs-indexer-{platform}
./run-indexer.sh

```
