# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Polars utility functions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl
import pyarrow as pa


@dataclass
class PolarsStreamSchema:
    """A Polars stream schema."""

    expressions: list[pl.Expr] = field(default_factory=list)
    arrow_schema: pa.Schema = field(default_factory=lambda: pa.schema([]))
    polars_schema: pl.Schema = field(default_factory=lambda: pl.Schema([]))

    @classmethod
    def from_json_schema(cls, json_schema: dict[str, Any]) -> PolarsStreamSchema:
        """Create a Polars stream schema from a JSON schema."""
        expressions: list[pl.Expr] = []
        arrow_columns: list[pa.Field] = []
        polars_columns: list[pl.Col] = []

        _json_schema_to_polars(
            json_schema=json_schema,
            expressions=expressions,
            arrow_columns=arrow_columns,
            polars_columns=polars_columns,
        )
        return cls(
            expressions=expressions,
            arrow_schema=pa.schema(arrow_columns),
            polars_schema=pl.Schema(polars_columns),
        )


def _json_schema_to_polars(
    *,
    json_schema: dict[str, Any],
    expressions: list[pl.Expr],
    arrow_columns: list[pa.Field],
    polars_columns: list[pl.Col],
    _breadcrumbs: list[str] | None = None,
) -> None:
    """Get Polars transformations and PyArrow column definitions from the provided JSON schema.

    Recursive operations are tracked with a breadcrumb list.
    """
    _breadcrumbs = _breadcrumbs or []
    json_schema_node = json_schema
    for crumb in _breadcrumbs:
        json_schema_node = json_schema_node["properties"][crumb]

    # Determine the primary type from the schema node
    tidy_type: str | list[str] = json_schema_node["type"]

    # Handle multiple types, focusing on non-nullable types if present
    if isinstance(tidy_type, list):
        if "null" in tidy_type:
            tidy_type.remove("null")
        if len(tidy_type) == 1:
            tidy_type = tidy_type[0]
        else:
            msg = f"Invalid type: {tidy_type}"
            raise ValueError(msg)

    for key, value in json_schema_node.get("properties", {}).items():
        # Handle nested objects recursively
        if tidy_type == "object":
            # Use breadcrumbs to navigate into nested properties
            _json_schema_to_polars(
                json_schema=json_schema,
                expressions=expressions,
                arrow_columns=arrow_columns,
                polars_columns=polars_columns,
                _breadcrumbs=[*_breadcrumbs, key],
            )
            continue

        # Map JSON schema types to Arrow and Polars types
        if tidy_type == "integer":
            expressions.append(pl.col(key).cast(pl.Int64))
            arrow_columns.append(pa.field(key, pa.int64()))
            polars_columns.append(pl.Int64)  # Add corresponding Polars column type
            continue

        if tidy_type == "number":
            expressions.append(pl.col(key).cast(pl.Float64))
            arrow_columns.append(pa.field(key, pa.float64()))
            polars_columns.append(pl.Float64)  # Add corresponding Polars column type
            continue

        if tidy_type == "boolean":
            expressions.append(pl.col(key).cast(pl.Boolean))
            arrow_columns.append(pa.field(key, pa.bool_()))
            polars_columns.append(pl.Boolean)  # Add corresponding Polars column type
            continue

        if tidy_type == "string":
            str_format = value.get("format")
            if str_format == "date-time":
                expressions.append(pl.col(key).cast(pl.Datetime))
                arrow_columns.append(pa.field(key, pa.timestamp("ms")))
                polars_columns.append(pl.Datetime)  # Add corresponding Polars column type
                continue

            if str_format == "date":
                expressions.append(pl.col(key).cast(pl.Date))
                arrow_columns.append(pa.field(key, pa.date32()))
                polars_columns.append(pl.Date)  # Add corresponding Polars column type
                continue

            if str_format == "time":
                expressions.append(pl.col(key).cast(pl.Time))
                arrow_columns.append(pa.field(key, pa.time32("ms")))
                polars_columns.append(pl.Time)  # Add corresponding Polars column type
                continue

            expressions.append(pl.col(key).cast(pl.Utf8))
            arrow_columns.append(pa.field(key, pa.string()))
            polars_columns.append(pl.Utf8)  # Add corresponding Polars column type
            continue

        msg = f"Invalid type: {tidy_type}"
        raise ValueError(msg)
