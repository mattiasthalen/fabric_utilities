from .auth import (
    get_access_token,
    get_azure_storage_access_token, 
    get_storage_options
)
from .read import (
    read_delta,
    read_parquet,
    read_parquets
)
from .write import (
    overwrite,
    upsert
)

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("fabric-utilities")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "get_access_token",
    "get_azure_storage_access_token", 
    "get_storage_options",
    "read_delta",
    "read_parquet",
    "overwrite",
    "upsert"
]