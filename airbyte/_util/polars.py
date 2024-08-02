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

    @classmethod
    def from_json_schema(cls, json_schema: dict[str, Any]) -> PolarsStreamSchema:
        """Create a Polars stream schema from a JSON schema."""
        expressions: list[pl.Expr] = []
        arrow_columns: list[pa.Field] = []
        _json_schema_to_polars(
            json_schema=json_schema,
            expressions=expressions,
            arrow_columns=arrow_columns,
        )
        return cls(
            expressions=expressions,
            arrow_schema=pa.schema(arrow_columns),
        )


def _json_schema_to_polars(
    *,
    json_schema: dict[str, Any],
    expressions: list[pl.Expr],
    arrow_columns: list[pa.Field],
    _breadcrumbs: list[str] | None = None,
) -> None:
    """Get Polars transformations and PyArrow column definitions from the provided JSON schema.

    Recursive operations are tracked with a breadcrumb list.
    """
    _breadcrumbs = _breadcrumbs or []
    json_schema_node = json_schema
    for crumb in _breadcrumbs:
        json_schema_node = json_schema_node["properties"][crumb]

    tidy_type: str | list[str] = json_schema_node["type"]

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
                _breadcrumbs=[*_breadcrumbs, key],
            )
            continue

        if tidy_type == "integer":
            expressions.append(pl.col(key).cast(pl.Int64))
            arrow_columns.append(pa.field(key, pa.int64()))
            continue

        if tidy_type == "number":
            expressions.append(pl.col(key).cast(pl.Float64))
            arrow_columns.append(pa.field(key, pa.float64()))
            continue

        if tidy_type == "boolean":
            expressions.append(pl.col(key).cast(pl.Boolean))
            arrow_columns.append(pa.field(key, pa.bool_()))
            continue

        if tidy_type == "string":
            str_format = value.get("format")
            if str_format == "date-time":
                expressions.append(pl.col(key).cast(pl.Datetime))
                arrow_columns.append(pa.field(key, pa.timestamp("ms")))
                continue

            if str_format == "date":
                expressions.append(pl.col(key).cast(pl.Date))
                arrow_columns.append(pa.field(key, pa.date32()))
                continue

            if str_format == "time":
                expressions.append(pl.col(key).cast(pl.Time))
                arrow_columns.append(pa.field(key, pa.time32("ms")))
                continue

            expressions.append(pl.col(key).cast(pl.Utf8))
            arrow_columns.append(pa.field(key, pa.string()))
            continue

        msg = f"Invalid type: {tidy_type}"
        raise ValueError(msg)
