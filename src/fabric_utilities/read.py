import polars as pl

from fabric_utilities.auth import get_storage_options


def _list_parquet_files(path: str) -> list[str]:
    """
    List all .parquet files in the specified directory path using Fabric's notebookutils.
    
    Uses notebookutils.fs.ls() to get FileInfo objects and filters for .parquet files.
    Excludes directories from the results.
    
    Args:
        path: Directory path to search for parquet files
        
    Returns:
        List of full paths to .parquet files found in the directory
        
    Raises:
        ModuleNotFoundError: If notebookutils is not available (not in Fabric environment)
        Various Fabric exceptions for invalid paths or permission issues
    """
    parquet_files: list[str] = []

    try:
        import notebookutils  # type: ignore
        items = notebookutils.fs.ls(path)

        for item in items:
            if item.isDir:
                continue

            if item.path.endswith(".parquet"):
                parquet_files.append(item.path)

    except (ModuleNotFoundError):
        raise ModuleNotFoundError(
            "This function requires notebookutils in Fabric environments."
        )

    return parquet_files

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
        eager: bool = False,
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

def read_parquets(
    path: str,
    eager: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Read multiple Parquet files from a directory or glob pattern.

    Lists all .parquet files in the specified path and concatenates them with
    diagonal_relaxed strategy to handle schema mismatches. Automatically fills
    missing columns with nulls across files.

    Works in both Fabric notebook environments (using notebookutils.fs.ls())
    and local environments (using os.listdir() or glob).

    Args:
        path: Directory path or glob pattern (e.g., "abfss://...", "/local/path", "*.parquet")
        eager: If True, returns DataFrame with data loaded immediately.
               If False, returns LazyFrame for deferred execution.

    Returns:
        DataFrame if eager=True, LazyFrame if eager=False

    Raises:
        ValueError: If no parquet files are found in the specified path
        Various Polars exceptions for invalid files or auth failures
    """
    parquet_files: list[str] = _list_parquet_files(path)

    lazy_frames: list[pl.LazyFrame] = []
    for file in parquet_files:
        lf = read_parquet(file, eager=False)
        # We know this is a LazyFrame since eager=False
        assert isinstance(lf, pl.LazyFrame)
        lazy_frames.append(lf)

    lf_concat: pl.LazyFrame = pl.concat(lazy_frames, how="diagonal_relaxed")

    if eager:
        return lf_concat.collect()

    return lf_concat
