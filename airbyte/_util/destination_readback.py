# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Destination readback introspection for smoke tests.

This module provides the ability to read back data written by a destination
connector and produce stats-level reports: table row counts, column names
and types, and per-column null/non-null counts.

The readback leverages PyAirbyte's existing cache implementations to query
the same backends that destinations write to.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airbyte._util.name_normalizers import LowerCaseNormalizer


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.caches.base import CacheBase

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Destination-to-cache mapping
# ---------------------------------------------------------------------------

# Maps destination connector names to cache class import paths.
# We use strings to avoid importing all cache classes at module load time.
_DESTINATION_TO_CACHE_INFO: dict[str, dict[str, str]] = {
    "destination-bigquery": {
        "module": "airbyte.caches.bigquery",
        "class": "BigQueryCache",
    },
    "destination-duckdb": {
        "module": "airbyte.caches.duckdb",
        "class": "DuckDBCache",
    },
    "destination-motherduck": {
        "module": "airbyte.caches.motherduck",
        "class": "MotherDuckCache",
    },
    "destination-postgres": {
        "module": "airbyte.caches.postgres",
        "class": "PostgresCache",
    },
    "destination-snowflake": {
        "module": "airbyte.caches.snowflake",
        "class": "SnowflakeCache",
    },
}

SUPPORTED_DESTINATIONS: frozenset[str] = frozenset(_DESTINATION_TO_CACHE_INFO.keys())
"""Destination connector names that support readback introspection."""


def _get_readback_supported(destination_name: str) -> bool:
    """Return True if readback is supported for the given destination."""
    return destination_name in SUPPORTED_DESTINATIONS


# ---------------------------------------------------------------------------
# Deterministic table name resolution
# ---------------------------------------------------------------------------

# Destinations normalize stream names into SQL table names.  The logic
# differs per destination.  We hard-code the known conventions here so
# that we can *optimistically* compute the expected table name without
# scanning the schema.


def _normalize_table_name_default(stream_name: str) -> str:
    """Default normalizer: lowercase + replace non-alphanumeric with underscores.

    This matches the LowerCaseNormalizer used by most PyAirbyte caches and
    aligns with how the Airbyte Java/Python destinations normalize names.
    """
    return LowerCaseNormalizer.normalize(stream_name)


def _normalize_table_name_snowflake(stream_name: str) -> str:
    """Snowflake normalizer: same as default (lowercase).

    Snowflake destinations use quoted identifiers in lowercase,
    matching the LowerCaseNormalizer behavior.
    """
    return LowerCaseNormalizer.normalize(stream_name)


def _normalize_table_name_bigquery(stream_name: str) -> str:
    """BigQuery normalizer: same as default (lowercase).

    BigQuery destinations use backtick-quoted identifiers in lowercase.
    """
    return LowerCaseNormalizer.normalize(stream_name)


_DESTINATION_TABLE_NORMALIZERS: dict[str, Callable[[str], str]] = {
    "destination-bigquery": _normalize_table_name_bigquery,
    "destination-duckdb": _normalize_table_name_default,
    "destination-motherduck": _normalize_table_name_default,
    "destination-postgres": _normalize_table_name_default,
    "destination-snowflake": _normalize_table_name_snowflake,
}


def _get_table_normalizer(
    destination_name: str,
) -> Callable[[str], str]:
    """Return the table name normalizer for the given destination."""
    return _DESTINATION_TABLE_NORMALIZERS.get(
        destination_name,
        _normalize_table_name_default,
    )


def _resolve_expected_table_name(
    destination_name: str,
    stream_name: str,
) -> str:
    """Deterministically resolve the expected SQL table name for a stream.

    This uses the destination's known naming conventions to predict
    what the table name should be in the backend.
    """
    normalizer = _get_table_normalizer(destination_name)
    return normalizer(stream_name)


# ---------------------------------------------------------------------------
# Column name normalization
# ---------------------------------------------------------------------------


def _normalize_column_name_default(column_name: str) -> str:
    """Default column normalizer: lowercase + replace non-alphanumeric with underscores."""
    return LowerCaseNormalizer.normalize(column_name)


def _resolve_expected_column_name(
    destination_name: str,
    column_name: str,
) -> str:
    """Deterministically resolve the expected SQL column name.

    For now all destinations use the same LowerCaseNormalizer for columns.
    """
    _ = destination_name  # Reserved for per-destination overrides
    return _normalize_column_name_default(column_name)


# ---------------------------------------------------------------------------
# Readback result models
# ---------------------------------------------------------------------------


class ColumnStats(BaseModel):
    """Null/non-null statistics for a single column."""

    column_name: str
    """The column name as found in the destination."""

    null_count: int
    """Number of NULL values in this column."""

    non_null_count: int
    """Number of non-NULL values in this column."""

    total_count: int
    """Total row count (null_count + non_null_count)."""


class ColumnInfo(BaseModel):
    """Column name and type information."""

    column_name: str
    """The column name as found in the destination."""

    column_type: str
    """The SQL data type name as reported by the database."""


class TableInfo(BaseModel):
    """Basic table info: name and row count."""

    table_name: str
    """The table name as found in the destination."""

    row_count: int
    """Number of rows in the table."""

    expected_stream_name: str
    """The original stream name that this table corresponds to."""


class TableReadbackReport(BaseModel):
    """Full readback report for a single table."""

    table_name: str
    """The table name as found in the destination."""

    expected_stream_name: str
    """The original stream name."""

    row_count: int
    """Number of rows found."""

    columns: list[ColumnInfo]
    """Column names and types."""

    column_stats: list[ColumnStats]
    """Per-column null/non-null statistics."""


class DestinationReadbackResult(BaseModel):
    """Result of reading back destination-written data.

    Contains three logical datasets:
    1. tables - list of tables with row counts
    2. columns - per-table column names and types
    3. column_stats - per-table, per-column null/non-null counts
    """

    destination: str
    """The destination connector name."""

    namespace: str
    """The namespace (schema) that was inspected."""

    readback_supported: bool
    """Whether readback was supported for this destination."""

    tables: list[TableInfo]
    """Dataset 1: Tables found with row counts."""

    table_reports: list[TableReadbackReport]
    """Full per-table reports including columns and stats."""

    tables_missing: list[str]
    """Stream names for which the expected table was not found."""

    error: str | None = None
    """Error message if readback failed."""

    def get_tables_summary(self) -> list[dict[str, Any]]:
        """Return dataset 1: tables with row counts as plain dicts."""
        return [t.model_dump() for t in self.tables]

    def get_columns_summary(self) -> list[dict[str, Any]]:
        """Return dataset 2: columns with types, grouped by table."""
        result = []
        for report in self.table_reports:
            result.extend(
                {
                    "table_name": report.table_name,
                    "column_name": col.column_name,
                    "column_type": col.column_type,
                }
                for col in report.columns
            )
        return result

    def get_column_stats_summary(self) -> list[dict[str, Any]]:
        """Return dataset 3: per-column null/non-null counts."""
        result = []
        for report in self.table_reports:
            result.extend(
                {
                    "table_name": report.table_name,
                    "column_name": stat.column_name,
                    "null_count": stat.null_count,
                    "non_null_count": stat.non_null_count,
                    "total_count": stat.total_count,
                }
                for stat in report.column_stats
            )
        return result


# ---------------------------------------------------------------------------
# Cache construction from destination config
# ---------------------------------------------------------------------------


def _build_readback_cache(
    destination_name: str,
    destination_config: dict[str, Any],
    namespace: str,
) -> CacheBase:
    """Construct a cache instance that can query the destination's backend.

    The cache is configured to point at the same backend the destination
    wrote to, using the supplied namespace as the schema.

    Raises:
        NotImplementedError: If the destination is not supported.
    """
    from airbyte.destinations._translate_dest_to_cache import (  # noqa: PLC0415
        destination_to_cache,
    )

    if destination_name not in SUPPORTED_DESTINATIONS:
        raise NotImplementedError(
            f"Readback is not supported for '{destination_name}'. "
            f"Supported destinations: {sorted(SUPPORTED_DESTINATIONS)}"
        )

    # The destination_to_cache function expects the config to have
    # a 'destinationType' field.  We ensure it's present.
    config_with_type = dict(destination_config)
    if "destinationType" not in config_with_type and "DESTINATION_TYPE" not in config_with_type:
        # Infer the type from the destination name
        dest_type = destination_name.replace("destination-", "")
        config_with_type["destinationType"] = dest_type

    cache = destination_to_cache(config_with_type)

    # Override the schema to match the namespace used by the smoke test
    if hasattr(cache, "schema_name"):
        # Use model_copy to create a new instance with updated schema
        cache = cache.model_copy(update={"schema_name": namespace})

    return cache


# ---------------------------------------------------------------------------
# Core readback logic
# ---------------------------------------------------------------------------


def _query_table_row_count(
    cache: CacheBase,
    table_name: str,
) -> int | None:
    """Query the row count for a table. Returns None if the table doesn't exist."""
    try:
        result = cache.run_sql_query(
            f"SELECT COUNT(*) AS row_count FROM {cache.schema_name}.{table_name}",
        )
        if result:
            return int(result[0]["row_count"])
        return 0  # noqa: TRY300
    except Exception:
        logger.debug("Table %s.%s not found or not accessible.", cache.schema_name, table_name)
        return None


def _query_column_info(
    cache: CacheBase,
    table_name: str,
) -> list[ColumnInfo]:
    """Query column names and types for a table.

    Uses a SELECT with LIMIT 0 to get column metadata from the result set,
    avoiding INFORMATION_SCHEMA scanning.
    """
    try:
        # We use the SQLAlchemy engine's inspector for column info
        import sqlalchemy  # noqa: PLC0415

        engine = cache.get_sql_engine()
        inspector = sqlalchemy.inspect(engine)
        columns = inspector.get_columns(table_name, schema=cache.schema_name)
        return [
            ColumnInfo(
                column_name=col["name"],
                column_type=str(col["type"]),
            )
            for col in columns
        ]
    except Exception:
        logger.debug(
            "Could not get column info for %s.%s",
            cache.schema_name,
            table_name,
        )
        return []


def _query_column_stats(
    cache: CacheBase,
    table_name: str,
    columns: list[ColumnInfo],
) -> list[ColumnStats]:
    """Query per-column null/non-null counts."""
    if not columns:
        return []

    # Build a SQL query that computes COUNT(*), COUNT(col) for each column
    # COUNT(*) gives total rows, COUNT(col) gives non-null count
    count_exprs = []
    for col in columns:
        col_name = col.column_name
        # Quote column names to handle special characters and reserved words
        quoted = f'"{col_name}"'
        count_exprs.append(f"COUNT({quoted}) AS non_null_{col_name}")

    count_exprs_str = ", ".join(count_exprs)
    sql = (
        f"SELECT COUNT(*) AS total_rows, {count_exprs_str} "
        f"FROM {cache.schema_name}.{table_name}"
    )

    try:
        result = cache.run_sql_query(sql)
    except Exception:
        logger.debug(
            "Could not query column stats for %s.%s",
            cache.schema_name,
            table_name,
        )
        return []

    if not result:
        return []

    row = result[0]
    total_rows = int(row["total_rows"])

    stats = []
    for col in columns:
        non_null_key = f"non_null_{col.column_name}"
        non_null_count = int(row.get(non_null_key, 0))
        stats.append(
            ColumnStats(
                column_name=col.column_name,
                null_count=total_rows - non_null_count,
                non_null_count=non_null_count,
                total_count=total_rows,
            )
        )

    return stats


def run_destination_readback(
    *,
    destination_name: str,
    destination_config: dict[str, Any],
    namespace: str,
    stream_names: list[str],
) -> DestinationReadbackResult:
    """Read back data from a destination after a smoke test and produce stats.

    This is the main entry point for readback introspection.  It:
    1. Constructs a cache that can query the destination's backend
    2. For each expected stream, resolves the expected table name
    3. Queries row counts, column info, and column stats

    Returns a ``DestinationReadbackResult`` with three datasets:
    - tables: table names + row counts
    - columns: column names + types per table
    - column_stats: null/non-null counts per column per table

    If the destination is not supported for readback, returns a result
    with ``readback_supported=False`` and empty data.
    """
    if not _get_readback_supported(destination_name):
        return DestinationReadbackResult(
            destination=destination_name,
            namespace=namespace,
            readback_supported=False,
            tables=[],
            table_reports=[],
            tables_missing=stream_names,
        )

    try:
        cache = _build_readback_cache(
            destination_name=destination_name,
            destination_config=destination_config,
            namespace=namespace,
        )
    except Exception as ex:
        logger.warning("Failed to build readback cache for %s: %s", destination_name, ex)
        return DestinationReadbackResult(
            destination=destination_name,
            namespace=namespace,
            readback_supported=True,
            tables=[],
            table_reports=[],
            tables_missing=stream_names,
            error=f"Failed to build readback cache: {ex}",
        )

    tables: list[TableInfo] = []
    table_reports: list[TableReadbackReport] = []
    tables_missing: list[str] = []

    for stream_name in stream_names:
        expected_table = _resolve_expected_table_name(destination_name, stream_name)

        # Optimistic: try to query the expected table directly
        row_count = _query_table_row_count(cache, expected_table)

        if row_count is None:
            tables_missing.append(stream_name)
            continue

        tables.append(
            TableInfo(
                table_name=expected_table,
                row_count=row_count,
                expected_stream_name=stream_name,
            )
        )

        # Get column info
        columns = _query_column_info(cache, expected_table)

        # Get column stats
        column_stats = _query_column_stats(cache, expected_table, columns)

        table_reports.append(
            TableReadbackReport(
                table_name=expected_table,
                expected_stream_name=stream_name,
                row_count=row_count,
                columns=columns,
                column_stats=column_stats,
            )
        )

    return DestinationReadbackResult(
        destination=destination_name,
        namespace=namespace,
        readback_supported=True,
        tables=tables,
        table_reports=table_reports,
        tables_missing=tables_missing,
    )
