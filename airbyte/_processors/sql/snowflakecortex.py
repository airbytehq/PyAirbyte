# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""A Snowflake vector store implementation of the SQL processor."""

from __future__ import annotations

from textwrap import dedent, indent
from typing import TYPE_CHECKING, cast

import sqlalchemy
from overrides import overrides

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStateType,
    ConfiguredAirbyteCatalog,
    Type,
)

from airbyte import exceptions as exc
from airbyte._processors.base import RecordProcessor
from airbyte._processors.sql.snowflake import SnowflakeSqlProcessor, SnowflakeTypeConverter
from airbyte.caches._catalog_manager import CatalogManager
from airbyte.strategies import WriteStrategy


if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from sqlalchemy.engine import Connection, Engine

    from airbyte._processors.file.base import FileWriterBase
    from airbyte.caches.base import CacheBase


class SnowflakeCortexTypeConverter(SnowflakeTypeConverter):
    """A class to convert array type into vector."""

    def __init__(
        self,
        conversion_map: dict | None = None,
        *,
        vector_length: int,
    ) -> None:
        self.vector_length = vector_length
        super().__init__(conversion_map)

    @overrides
    def to_sql_type(
        self,
        json_schema_property_def: dict[str, str | dict | list],
    ) -> sqlalchemy.types.TypeEngine:
        """Convert a value to a SQL type."""
        sql_type = super().to_sql_type(json_schema_property_def)
        if isinstance(sql_type, sqlalchemy.types.ARRAY):
            return f"Vector(Float, {self.vector_length})"

        return sql_type


class SnowflakeCortexSqlProcessor(SnowflakeSqlProcessor):
    """A Snowflake implementation for use with Cortex functions."""

    def __init__(
        self,
        cache: CacheBase,
        catalog: ConfiguredAirbyteCatalog,
        vector_length: int,
        source_name: str,
        stream_names: set[str],
        *,
        file_writer: FileWriterBase | None = None,
    ) -> None:
        """Custom initialization: Initialize type_converter with vector_length."""
        self._catalog = catalog
        # to-do: see if we can get rid of the following assignment
        self.source_catalog = catalog
        self._vector_length = vector_length
        self._engine: Engine | None = None
        self._connection_to_reuse: Connection | None = None

        # call base class to do necessary initialization
        RecordProcessor.__init__(self, cache=cache, catalog_manager=None)
        self._ensure_schema_exists()
        self._catalog_manager = CatalogManager(
            engine=self.get_sql_engine(),
            table_name_resolver=lambda stream_name: self.get_sql_table_name(stream_name),
        )

        # TODO: read streams and source from catalog if not provided

        # initialize catalog manager by registering source
        self.register_source(
            source_name=source_name,
            incoming_source_catalog=self._catalog,
            stream_names=stream_names,
        )
        self.file_writer = file_writer or self.file_writer_class(cache)
        self.type_converter = SnowflakeCortexTypeConverter(vector_length=vector_length)
        self._cached_table_definitions: dict[str, sqlalchemy.Table] = {}

    @overrides
    def process_airbyte_messages(
        self,
        messages: Iterable[AirbyteMessage],
        write_strategy: WriteStrategy,
    ) -> None:
        """Process a stream of Airbyte messages."""
        if not isinstance(write_strategy, WriteStrategy):
            raise exc.PyAirbyteInternalError(
                message="Invalid `write_strategy` argument. Expected instance of WriteStrategy.",
                context={"write_strategy": write_strategy},
            )

        stream_schemas: dict[str, dict] = {}

        # Process messages, writing to batches as we go
        for message in messages:
            if message.type is Type.RECORD:
                record_msg = cast(AirbyteRecordMessage, message.record)
                stream_name = record_msg.stream

                if stream_name not in stream_schemas:
                    stream_schemas[stream_name] = self.cache.processor.get_stream_json_schema(
                        stream_name=stream_name
                    )

                self.process_record_message(
                    record_msg,
                    stream_schema=stream_schemas[stream_name],
                )

            elif message.type is Type.STATE:
                state_msg = cast(AirbyteStateMessage, message.state)
                if state_msg.type in [AirbyteStateType.GLOBAL, AirbyteStateType.LEGACY]:
                    self._pending_state_messages[f"_{state_msg.type}"].append(state_msg)
                else:
                    # stream_state = cast(AirbyteStreamState, state_msg.stream)
                    # to-do: remove hardcoding below
                    stream_name = "myteststream"  # stream_state.stream_descriptor.name
                    self._pending_state_messages[stream_name].append(state_msg)

            else:
                # Ignore unexpected or unhandled message types:
                # Type.LOG, Type.TRACE, Type.CONTROL, etc.
                pass

        self.write_all_stream_data(
            write_strategy=write_strategy,
        )

        # Clean up files, if requested.
        if self.cache.cleanup:
            self.cleanup_all()

    def _get_column_list_from_table(
        self,
        table_name: str,
    ) -> list[str]:
        """Get column names for passed stream."""
        conn: Connection = self.cache.get_database_connection_via_alternate_method()
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE TABLE {table_name};")
        results = cursor.fetchall()
        column_names = [row[0].lower() for row in results]
        cursor.close()
        conn.close()
        return column_names

    @overrides
    def _ensure_compatible_table_schema(
        self,
        stream_name: str,
        *,
        raise_on_error: bool = True,
    ) -> bool:
        """Read the exsting table schema using Snowflake python connector"""
        json_schema = self.get_stream_json_schema(stream_name)
        stream_column_names: list[str] = json_schema["properties"].keys()
        table_column_names: list[str] = self._get_column_list_from_table(stream_name)

        lower_case_table_column_names = self.normalizer.normalize_set(table_column_names)
        missing_columns = [
            stream_col
            for stream_col in stream_column_names
            if self.normalizer.normalize(stream_col) not in lower_case_table_column_names
        ]
        if missing_columns:
            if raise_on_error:
                raise exc.PyAirbyteCacheTableValidationError(
                    violation="Cache table is missing expected columns.",
                    context={
                        "stream_column_names": stream_column_names,
                        "table_column_names": table_column_names,
                        "missing_columns": missing_columns,
                    },
                )
            return False  # Some columns are missing.

        return True  # All columns exist.

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

        def path_str(path: Path) -> str:
            return str(path.absolute()).replace("\\", "\\\\")

        put_files_statements = "\n".join(
            [f"PUT 'file://{path_str(file_path)}' {internal_sf_stage_name};" for file_path in files]
        )
        self._execute_sql(put_files_statements)
        columns_list = [
            self._quote_identifier(c)
            for c in list(self._get_sql_column_definitions(stream_name).keys())
        ]
        files_list = ", ".join([f"'{f.name}'" for f in files])
        columns_list_str: str = indent("\n, ".join(columns_list), " " * 12)

        # to-do: move following into a separate method,
        # then don't need to override the whole method.
        vector_suffix = f"::Vector(Float, {self._vector_length})"
        variant_cols_str: str = ("\n" + " " * 21 + ", ").join(
            [
                f"$1:{self.normalizer.normalize(col)}{vector_suffix if 'embedding' in col else ''}"
                for col in columns_list
            ]
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
