import polars as pl
import time
import typing as t

from deltalake.exceptions import TableNotFoundError
from fabric_utilities.auth import get_storage_options


def _ensure_dataframe(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """
    Convert LazyFrame to DataFrame if needed.

    >>> import polars as pl
    >>> df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    >>> result = _ensure_dataframe(df)
    >>> isinstance(result, pl.DataFrame)
    True
    >>> lazy_df = pl.LazyFrame({"a": [1, 2], "b": [3, 4]})
    >>> result = _ensure_dataframe(lazy_df)
    >>> isinstance(result, pl.DataFrame)
    True
    """
    if isinstance(df, pl.LazyFrame):
        return df.collect()
    return df


def _normalize_columns(columns: str | list[str] | None) -> list[str]:
    """
    Normalize column specification to list of strings.

    >>> _normalize_columns(None)
    []
    >>> _normalize_columns("single_column")
    ['single_column']
    >>> _normalize_columns(["col1", "col2"])
    ['col1', 'col2']
    >>> _normalize_columns([])
    []
    """
    if columns is None:
        return []
    if isinstance(columns, str):
        return [columns]
    return columns


def _validate_columns_exist(df: pl.DataFrame, columns: list[str], column_type: str) -> None:
    """
    Validate that specified columns exist in the DataFrame.

    >>> import polars as pl
    >>> df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    >>> _validate_columns_exist(df, ["a", "b"], "Test")  # Should not raise
    >>> _validate_columns_exist(df, [], "Test")  # Should not raise (empty list)
    >>> _validate_columns_exist(df, ["a", "missing"], "Test")  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ValueError: Test columns ['missing'] not found in DataFrame...
    """
    if not columns:
        return

    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"{column_type} columns {missing_columns} not found in DataFrame. "
            f"Available columns: {df.columns}"
        )


def _build_merge_predicate(primary_key_columns: list[str]) -> str:
    """
    Build the merge predicate for joining source and target tables.

    >>> _build_merge_predicate(["id"])
    'target.id = source.id'
    >>> _build_merge_predicate(["id", "name"])
    'target.id = source.id AND target.name = source.name'
    >>> _build_merge_predicate([])
    ''
    """
    return " AND ".join(
        [f"target.{column} = source.{column}" for column in primary_key_columns]
    )


def _get_predicate_update_columns(
    df_columns: list[str],
    primary_key_columns: list[str],
    predicate_exclusion_columns: list[str],
    update_exclusion_columns: list[str],
) -> list[str]:
    """
    Get columns to use in update predicate (columns that trigger updates when changed).

    >>> _get_predicate_update_columns(
    ...     ["id", "name", "email", "created_at", "updated_at"],
    ...     ["id"],
    ...     ["created_at"],
    ...     ["updated_at"]
    ... )
    ['name', 'email']
    >>> _get_predicate_update_columns(["a", "b"], ["a"], [], [])
    ['b']
    >>> _get_predicate_update_columns(["a"], ["a"], [], [])
    []
    """
    excluded_columns = primary_key_columns + predicate_exclusion_columns + update_exclusion_columns
    return [column for column in df_columns if column not in excluded_columns]


def _build_update_predicate(predicate_update_columns: list[str]) -> str:
    """
    Build predicate that determines when matched rows should be updated.

    >>> predicate = _build_update_predicate(["name"])
    >>> "target.name != source.name" in predicate
    True
    >>> "target.name IS NULL AND source.name IS NOT NULL" in predicate
    True
    >>> "target.name IS NOT NULL AND source.name IS NULL" in predicate
    True

    >>> predicate = _build_update_predicate(["name", "email"])
    >>> "target.name != source.name" in predicate
    True
    >>> "target.email != source.email" in predicate
    True
    >>> predicate.count("target.name") == 3  # Should appear 3 times (!=, IS NULL, IS NOT NULL)
    True
    >>> predicate.count("target.email") == 3  # Should appear 3 times (!=, IS NULL, IS NOT NULL)
    True

    >>> _build_update_predicate([])
    ''
    """
    column_predicates = [
        f"""
                (
                    1 = 1
                    AND target.{column} != source.{column}
                    OR target.{column} IS NULL AND source.{column} IS NOT NULL
                    OR target.{column} IS NOT NULL AND source.{column} IS NULL
                )
            """
        for column in predicate_update_columns
    ]
    return " AND ".join(column_predicates)


def _get_update_columns(
    df_columns: list[str],
    primary_key_columns: list[str],
    update_exclusion_columns: list[str],
) -> list[str]:
    """
    Get columns that should be updated (excluding primary keys and excluded columns).

    >>> _get_update_columns(
    ...     ["id", "name", "email", "updated_at"],
    ...     ["id"],
    ...     ["updated_at"]
    ... )
    ['name', 'email']
    >>> _get_update_columns(["a", "b", "c"], ["a"], [])
    ['b', 'c']
    >>> _get_update_columns(["a"], ["a"], [])
    []
    """
    excluded_columns = primary_key_columns + update_exclusion_columns
    return [column for column in df_columns if column not in excluded_columns]


def _build_update_mapping(update_columns: list[str]) -> dict[str, str]:
    """
    Build the column mapping for updates.

    >>> _build_update_mapping(["name", "email"])
    {'target.name': 'source.name', 'target.email': 'source.email'}
    >>> _build_update_mapping(["single_col"])
    {'target.single_col': 'source.single_col'}
    >>> _build_update_mapping([])
    {}
    """
    return {f"target.{column}": f"source.{column}" for column in update_columns}


def _build_delta_merge_options(merge_predicate: str) -> dict[str, t.Any]:
    """
    Build delta merge options dictionary.

    >>> options = _build_delta_merge_options("target.id = source.id")
    >>> options["predicate"]
    'target.id = source.id'
    >>> options["source_alias"]
    'source'
    >>> options["target_alias"]
    'target'
    >>> len(options)
    3
    """
    return {
        "predicate": merge_predicate,
        "source_alias": "source",
        "target_alias": "target",
    }


def _execute_delta_merge(
    df: pl.DataFrame,
    table_uri: str,
    storage_options: dict | None,
    delta_merge_options: dict[str, t.Any],
    when_matched_update_predicates: str,
    when_matched_update_columns: dict[str, str],
) -> dict:
    """
    Execute the Delta Lake merge operation with specified conditions.

    Performs a three-way merge operation:
    1. Updates matched rows when predicate conditions are met
    2. Inserts all non-matched rows from source
    3. Leaves unmatched target rows unchanged

    Args:
        df: Source DataFrame to merge
        table_uri: Target Delta table URI
        storage_options: Fabric storage configuration
        delta_merge_options: Merge configuration (predicate, aliases)
        when_matched_update_predicates: SQL conditions for when to update matched rows
        when_matched_update_columns: Column mapping for updates (target -> source)

    Returns:
        Result dictionary from the Delta merge operation

    Raises:
        Various Delta/Polars exceptions for merge failures
    """
    return (
        df.write_delta(
            table_uri,
            mode="merge",
            storage_options=storage_options,
            delta_merge_options=delta_merge_options,
        )
        .when_matched_update(
            predicate=when_matched_update_predicates,
            updates=when_matched_update_columns,
        )
        .when_not_matched_insert_all()
        .execute()
    )


def overwrite(
    table_uri: str,
    df: pl.DataFrame | pl.LazyFrame,
    schema_mode: t.Literal['merge', 'overwrite'] | None = "merge",
    partition_by: str | list[str] | None = None,
) -> dict:
    """
    Overwrite a Delta table with the provided DataFrame.

    Completely replaces the target table with new data. Handles schema
    evolution based on the schema_mode parameter. Automatically manages
    Fabric authentication for abfss:// URIs.

    Args:
        table_uri: URI of the target Delta table (e.g., "abfss://..." or local path)
        df: Source DataFrame or LazyFrame to write
        schema_mode: How to handle schema differences:
                    - "merge": Allow compatible schema changes (default)
                    - "overwrite": Replace schema entirely
                    - None: Use Delta Lake default behavior
        partition_by: Column(s) to partition the table by. Can be string or list of strings.

    Returns:
        Dictionary with operation statistics including number of rows written

    Raises:
        Various Delta/Polars exceptions for write failures or auth issues
    """


    storage_options: dict | None = get_storage_options(table_uri)
    df = _ensure_dataframe(df)

    # Capture statistics
    start_time = time.time()
    num_source_rows = len(df)

    # Perform the overwrite operation (returns None)
    df.write_delta(
        target=table_uri,
        mode="overwrite",
        storage_options=storage_options,
        delta_write_options={
            "schema_mode": schema_mode,
            "partition_by": partition_by,
        },
    )

    end_time = time.time()
    execution_time_ms = int((end_time - start_time) * 1000)

    # Build statistics dictionary similar to merge operations
    result = {
        "num_source_rows": num_source_rows,
        "num_target_rows_inserted": num_source_rows,  # All rows are "inserted" in overwrite
        "num_target_rows_updated": 0,  # No updates in overwrite mode
        "num_target_rows_deleted": 0,  # Delta handles deletions internally
        "num_target_rows_copied": 0,   # No rows copied in overwrite mode
        "num_output_rows": num_source_rows,
        "execution_time_ms": execution_time_ms,
        "operation_mode": "overwrite",
        "schema_mode": schema_mode,
    }

    # Add partition information if provided
    if partition_by:
        result["partition_by"] = partition_by if isinstance(partition_by, list) else [partition_by]

    return result


def upsert(
    table_uri: str,
    df: pl.DataFrame | pl.LazyFrame,
    primary_key_columns: str | list[str],
    update_exclusion_columns: str | list[str] | None = None,
    predicate_exclusion_columns: str | list[str] | None = None,
    partition_by: str | list[str] | None = None,
) -> dict:
    """
    Upsert data into a Delta table using merge operation.

    Args:
        table_uri: URI of the target Delta table
        df: Source DataFrame or LazyFrame
        primary_key_columns: Columns to use for matching records
        update_exclusion_columns: Columns to exclude from updates (but include in inserts)
        predicate_exclusion_columns: Columns to exclude from update predicate logic
        partition_by: Columns to partition by (used if table needs to be created)

    Returns:
        Result dictionary from the delta operation
    """
    storage_options: dict | None = get_storage_options(table_uri)

    # Normalize inputs
    primary_key_columns = _normalize_columns(primary_key_columns)
    update_exclusion_columns = _normalize_columns(update_exclusion_columns)
    predicate_exclusion_columns = _normalize_columns(predicate_exclusion_columns)
    df = _ensure_dataframe(df)

    # Validate inputs
    _validate_columns_exist(df, primary_key_columns, "Primary key")
    all_exclusion_columns: list[str] = update_exclusion_columns + predicate_exclusion_columns
    _validate_columns_exist(df, all_exclusion_columns, "Exclusion")

    # Build merge components
    merge_predicate: str = _build_merge_predicate(primary_key_columns)

    predicate_update_columns: list[str] = _get_predicate_update_columns(
        df.columns, primary_key_columns, predicate_exclusion_columns, update_exclusion_columns
    )
    when_matched_update_predicates: str = _build_update_predicate(predicate_update_columns)

    update_columns: list[str] = _get_update_columns(df.columns, primary_key_columns, update_exclusion_columns)
    when_matched_update_columns: dict[str, str] = _build_update_mapping(update_columns)

    delta_merge_options: dict[str, str] = _build_delta_merge_options(merge_predicate)

    # Execute merge operation
    try:
        result: dict = _execute_delta_merge(
            df=df,
            table_uri=table_uri,
            storage_options=storage_options,
            delta_merge_options=delta_merge_options,
            when_matched_update_predicates=when_matched_update_predicates,
            when_matched_update_columns=when_matched_update_columns,
        )
    except TableNotFoundError:
        # Table doesn't exist, create it with overwrite
        result: dict = overwrite(
            table_uri=table_uri,
            df=df,
            schema_mode="overwrite",
            partition_by=partition_by,
        )

    return result
