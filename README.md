# Fabric Utilities

A Python package providing utilities for working with Microsoft Fabric, including authentication, reading, and writing operations for Delta Lake and Parquet files.

## Features

- Azure authentication utilities
- Delta Lake table reading
- Parquet file operations
- Azure storage integration

## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/mattiasthalen/fabric_utilities.git
```

Or install a specific version/tag:

```bash
pip install git+https://github.com/mattiasthalen/fabric_utilities.git@v0.1.0
```

For development:

```bash
git clone https://github.com/mattiasthalen/fabric_utilities.git
cd fabric_utilities
uv sync
```

## Usage

```python
from fabric_utilities import get_access_token, read_delta

# Get access token
token = get_access_token()

# Read Delta table
df = read_delta("path/to/delta/table")
```

## License

This project is licensed under the GPL-3.0 License - see the LICENSE file for details.