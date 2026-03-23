# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Destination readback introspection for smoke tests.

This module provides the ability to read back data written by a destination
connector and produce stats-level reports: table row counts, column names
and types, and per-column null/non-null counts.

The readback leverages PyAirbyte's existing cache implementations to query
the same backends that destinations write to.  Table and column name
normalization is delegated to the cache's built-in SQL processor normalizer.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel


if TYPE_CHECKING:
    from airbyte.destinations.base import Destination

logger = logging.getLogger(__name__)


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
# Readback orchestration
# ---------------------------------------------------------------------------


def run_destination_readback(
    *,
    destination: Destination,
    namespace: str,
    stream_names: list[str],
) -> DestinationReadbackResult:
    """Read back data from a destination after a smoke test and produce stats.

    This is the main entry point for readback introspection.  It:
    1. Builds a cache via ``destination.get_sql_cache()`` (same pattern as
       :pymethod:`airbyte.cloud.sync_results.SyncResult.get_sql_cache`)
    2. For each expected stream, resolves the expected table name using the
       cache's built-in processor normalizer
    3. Queries row counts, column info, and column stats via cache methods

    Returns a ``DestinationReadbackResult`` with three datasets:
    - tables: table names + row counts
    - columns: column names + types per table
    - column_stats: null/non-null counts per column per table

    If the destination is not supported for readback, returns a result
    with ``readback_supported=False`` and empty data.
    """
    try:
        cache = destination.get_sql_cache(schema_name=namespace)
    except ValueError:
        # destination_to_cache raises ValueError for unsupported types
        return DestinationReadbackResult(
            destination=destination.name,
            namespace=namespace,
            readback_supported=False,
            tables=[],
            table_reports=[],
            tables_missing=stream_names,
        )
    except Exception as ex:
        logger.warning("Failed to build readback cache for %s: %s", destination.name, ex)
        return DestinationReadbackResult(
            destination=destination.name,
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
        # Use the cache's processor normalizer to resolve the expected table name.
        # This delegates to the same normalizer the cache uses for writing.
        expected_table = cache.processor.get_sql_table_name(stream_name)

        # Optimistic: try to query the expected table directly
        row_count = cache._readback_query_row_count(  # noqa: SLF001
            expected_table,
        )

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

        # Get column info via cache method
        raw_columns = cache._readback_query_column_info(  # noqa: SLF001
            expected_table,
        )
        columns = [
            ColumnInfo(
                column_name=c["column_name"],
                column_type=c["column_type"],
            )
            for c in raw_columns
        ]

        # Get column stats via cache method
        raw_stats = cache._readback_query_column_stats(  # noqa: SLF001
            expected_table,
            raw_columns,
        )
        column_stats = [
            ColumnStats(
                column_name=s["column_name"],
                null_count=s["null_count"],
                non_null_count=s["non_null_count"],
                total_count=s["total_count"],
            )
            for s in raw_stats
        ]

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
        destination=destination.name,
        namespace=namespace,
        readback_supported=True,
        tables=tables,
        table_reports=table_reports,
        tables_missing=tables_missing,
    )
