import polars as pl

from fabric_utilities.auth import get_storage_options

def read_delta(
        table_uri: str,
        eager: bool = False
) -> pl.DataFrame | pl.LazyFrame:
    """
    Read a Delta Lake table from Fabric or local filesystem.
    
    Supports both eager loading (immediate execution) and lazy loading
    (deferred execution) patterns. Automatically handles Fabric
    authentication when reading from abfss:// URIs.
    
    Args:
        table_uri: URI of the Delta table (e.g., "abfss://..." or local path)
        eager: If True, returns DataFrame with data loaded immediately.
               If False, returns LazyFrame for deferred execution.
               
    Returns:
        DataFrame if eager=True, LazyFrame if eager=False
        
    Raises:
        Various Polars/Delta exceptions for invalid tables or auth failures
    """
    storage_options: dict | None = get_storage_options(table_uri)

    if eager:
        return pl.read_delta(source=table_uri, storage_options=storage_options)

    return pl.scan_delta(source=table_uri, storage_options=storage_options)

def read_parquet(
        table_uri: str,
        eager: bool = False
) -> pl.DataFrame | pl.LazyFrame:
    """
    Read Parquet files from Fabric or local filesystem.
    
    Automatically enables Hive partitioning support for partitioned datasets.
    Supports both eager and lazy loading patterns. Handles Fabric
    authentication automatically for abfss:// URIs.
    
    Args:
        table_uri: URI of the Parquet file(s) (e.g., "abfss://..." or local path)
        eager: If True, returns DataFrame with data loaded immediately.
               If False, returns LazyFrame for deferred execution.
               
    Returns:
        DataFrame if eager=True, LazyFrame if eager=False
        
    Raises:
        Various Polars exceptions for invalid files or auth failures
    """
    storage_options: dict | None = get_storage_options(table_uri)

    if eager:
        return pl.read_parquet(
            source=table_uri, hive_partitioning=True, storage_options=storage_options
        )

    return pl.scan_parquet(
        source=table_uri, hive_partitioning=True, storage_options=storage_options
    )