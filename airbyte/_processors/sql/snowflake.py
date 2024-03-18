# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Snowflake implementation of the SQL processor."""

from __future__ import annotations

from textwrap import dedent, indent
from typing import TYPE_CHECKING

import sqlalchemy
from overrides import overrides
from snowflake.sqlalchemy import VARIANT

from airbyte._processors.file.jsonl import JsonlWriter
from airbyte._processors.sql.base import SqlProcessorBase
from airbyte.types import SQLTypeConverter


if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.engine import Connection


class SnowflakeTypeConverter(SQLTypeConverter):
    """A class to convert types for Snowflake."""

    @overrides
    def to_sql_type(
        self,
        json_schema_property_def: dict[str, str | dict | list],
    ) -> sqlalchemy.types.TypeEngine:
        """Convert a value to a SQL type.

        We first call the parent class method to get the type. Then if the type JSON, we
        replace it with VARIANT.
        """
        sql_type = super().to_sql_type(json_schema_property_def)
        if isinstance(sql_type, sqlalchemy.types.JSON):
            return VARIANT()

        return sql_type


class SnowflakeSqlProcessor(SqlProcessorBase):
    """A Snowflake implementation of the cache."""

    file_writer_class = JsonlWriter
    type_converter_class = SnowflakeTypeConverter

    @overrides
    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Write files to a new table."""
        temp_table_name = self._create_table_for_loading(
            stream_name=stream_name,
            batch_id=batch_id,
        )
        internal_sf_stage_name = f"@%{temp_table_name}"
        put_files_statements = "\n".join(
            [
                f"PUT 'file://{file_path.absolute()!s}' {internal_sf_stage_name};"
                for file_path in files
            ]
        )
        self._execute_sql(put_files_statements)
        properties_list: list[str] = list(self._get_stream_properties(stream_name).keys())
        columns_list = [
            self._quote_identifier(c)
            for c in list(self._get_sql_column_definitions(stream_name).keys())
        ]
        files_list = ", ".join([f"'{f.name}'" for f in files])
        columns_list_str: str = indent("\n, ".join(columns_list), " " * 12)
        variant_cols_str: str = ("\n" + " " * 21 + ", ").join(
            [f"$1:{col}" for col in properties_list]
        )
        copy_statement = dedent(
            f"""
            COPY INTO {temp_table_name}
            (
                {columns_list_str}
            )
            FROM (
                SELECT {variant_cols_str}
                FROM {internal_sf_stage_name}
            )
            FILES = ( {files_list} )
            FILE_FORMAT = ( TYPE = JSON )
            ;
            """
        )
        self._execute_sql(copy_statement)
        return temp_table_name

    @overrides
    def _init_connection_settings(self, connection: Connection) -> None:
        """We set Snowflake-specific settings for the session.

        This sets QUOTED_IDENTIFIERS_IGNORE_CASE setting to True, which is necessary because
        Snowflake otherwise will treat quoted table and column references as case-sensitive.
        More info: https://docs.snowflake.com/en/sql-reference/identifiers-syntax

        This also sets MULTI_STATEMENT_COUNT to 0, which allows multi-statement commands.
        """
        connection.execute(
            """
            ALTER SESSION SET
            QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE
            MULTI_STATEMENT_COUNT = 0
            """
        )
