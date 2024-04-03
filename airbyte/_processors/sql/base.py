# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""The base SQL Cache implementation."""

from __future__ import annotations

import contextlib
import enum
from contextlib import contextmanager
from functools import cached_property
from typing import TYPE_CHECKING, final

import pandas as pd
import sqlalchemy
import ulid
from overrides import overrides
from pandas import Index
from sqlalchemy import (
    Column,
    Table,
    and_,
    create_engine,
    insert,
    null,
    select,
    text,
    update,
)
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.elements import TextClause

from airbyte import exceptions as exc
from airbyte._processors.base import RecordProcessor
from airbyte._util.name_normalizers import LowerCaseNormalizer
from airbyte.caches._catalog_manager import CatalogManager
from airbyte.constants import (
    AB_EXTRACTED_AT_COLUMN,
    AB_META_COLUMN,
    AB_RAW_ID_COLUMN,
    DEBUG_MODE,
)
from airbyte.datasets._sql import CachedDataset
from airbyte.progress import progress
from airbyte.strategies import WriteStrategy
from airbyte.types import SQLTypeConverter


if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from sqlalchemy.engine import Connection, Engine
    from sqlalchemy.engine.cursor import CursorResult
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.sql.base import Executable
    from sqlalchemy.sql.type_api import TypeEngine

    from airbyte_protocol.models import (
        AirbyteRecordMessage,
        AirbyteStateMessage,
        ConfiguredAirbyteCatalog,
    )

    from airbyte._batch_handles import BatchHandle
    from airbyte._processors.file.base import FileWriterBase
    from airbyte.caches.base import CacheBase


class RecordDedupeMode(enum.Enum):
    APPEND = "append"
    REPLACE = "replace"


class SQLRuntimeError(Exception):
    """Raised when an SQL operation fails."""


class SqlProcessorBase(RecordProcessor):
    """A base class to be used for SQL Caches."""

    type_converter_class: type[SQLTypeConverter] = SQLTypeConverter
    """The type converter class to use for converting JSON schema types to SQL types."""

    normalizer = LowerCaseNormalizer
    """The name normalizer to user for table and column name normalization."""

    file_writer_class: type[FileWriterBase]
    """The file writer class to use for writing files to the cache."""

    supports_merge_insert = False
    """True if the database supports the MERGE INTO syntax."""

    use_singleton_connection = False  # If true, the same connection is used for all operations.

    # Constructor:

    def __init__(
        self,
        cache: CacheBase,
        file_writer: FileWriterBase | None = None,
    ) -> None:
        self._engine: Engine | None = None
        self._connection_to_reuse: Connection | None = None
        super().__init__(cache, catalog_manager=None)
        self._ensure_schema_exists()
        self._catalog_manager = CatalogManager(
            engine=self.get_sql_engine(),
            table_name_resolver=lambda stream_name: self.get_sql_table_name(stream_name),
        )
        self.file_writer = file_writer or self.file_writer_class(cache)
        self.type_converter = self.type_converter_class()
        self._cached_table_definitions: dict[str, sqlalchemy.Table] = {}

    # Public interface:

    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        return self.cache.get_sql_alchemy_url()

    @final
    @cached_property
    def database_name(self) -> str:
        """Return the name of the database."""
        return self.cache.get_database_name()

    @final
    def get_sql_engine(self) -> Engine:
        """Return a new SQL engine to use."""
        if self._engine:
            return self._engine

        sql_alchemy_url = self.get_sql_alchemy_url()

        execution_options = {"schema_translate_map": {None: self.cache.schema_name}}
        if self.use_singleton_connection:
            if self._connection_to_reuse is None:
                # This temporary bootstrap engine will be created once and is needed to
                # create the long-lived connection object.
                bootstrap_engine = create_engine(
                    sql_alchemy_url,
                )
                self._connection_to_reuse = bootstrap_engine.connect()

            self._engine = create_engine(
                sql_alchemy_url,
                creator=lambda: self._connection_to_reuse,
                poolclass=StaticPool,
                echo=DEBUG_MODE,
                execution_options=execution_options,
                # isolation_level="AUTOCOMMIT",
            )
        else:
            # Regular engine creation for new connections
            self._engine = create_engine(
                sql_alchemy_url,
                echo=DEBUG_MODE,
                execution_options=execution_options,
                # isolation_level="AUTOCOMMIT",
            )

        return self._engine

    @overrides
    def register_source(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        stream_names: set[str],
    ) -> None:
        """Register the source with the cache.

        We use stream_names to determine which streams will receive data, and
        we only register the stream if is expected to receive data.

        This method is called by the source when it is initialized.
        """
        self._source_name = source_name
        self._ensure_schema_exists()
        super().register_source(
            source_name,
            incoming_source_catalog,
            stream_names=stream_names,
        )

    @contextmanager
    def get_sql_connection(self) -> Generator[sqlalchemy.engine.Connection, None, None]:
        """A context manager which returns a new SQL connection for running queries.

        If the connection needs to close, it will be closed automatically.
        """
        if self.use_singleton_connection and self._connection_to_reuse is not None:
            connection = self._connection_to_reuse
            self._init_connection_settings(connection)
            yield connection

        else:
            with self.get_sql_engine().begin() as connection:
                self._init_connection_settings(connection)
                yield connection

        if not self.use_singleton_connection:
            connection.close()
            del connection

    def get_sql_table_name(
        self,
        stream_name: str,
    ) -> str:
        """Return the name of the SQL table for the given stream."""
        table_prefix = self.cache.table_prefix or ""

        # TODO: Add default prefix based on the source name.

        return self.normalizer.normalize(
            f"{table_prefix}{stream_name}{self.cache.table_suffix}",
        )

    @final
    def get_sql_table(
        self,
        stream_name: str,
    ) -> sqlalchemy.Table:
        """Return the main table object for the stream."""
        return self._get_table_by_name(
            self.get_sql_table_name(stream_name),
        )

    # Read methods:

    def get_records(
        self,
        stream_name: str,
    ) -> CachedDataset:
        """Uses SQLAlchemy to select all rows from the table."""
        return CachedDataset(self.cache, stream_name)

    def get_pandas_dataframe(
        self,
        stream_name: str,
    ) -> pd.DataFrame:
        """Return a Pandas data frame with the stream's data."""
        table_name = self.get_sql_table_name(stream_name)
        engine = self.get_sql_engine()
        return pd.read_sql_table(table_name, engine, schema=self.cache.schema_name)

    def process_record_message(
        self,
        record_msg: AirbyteRecordMessage,
        stream_schema: dict,
    ) -> None:
        """Write a record to the cache.

        This method is called for each record message, before the batch is written.

        In most cases, the SQL processor will not perform any action, but will pass this along to to
        the file processor.
        """
        self.file_writer.process_record_message(
            record_msg,
            stream_schema=stream_schema,
        )

    # Protected members (non-public interface):

    def _init_connection_settings(self, connection: Connection) -> None:
        """This is called automatically whenever a new connection is created.

        By default this is a no-op. Subclasses can use this to set connection settings, such as
        timezone, case-sensitivity settings, and other session-level variables.
        """
        pass

    def _get_table_by_name(
        self,
        table_name: str,
        *,
        force_refresh: bool = False,
        shallow_okay: bool = False,
    ) -> sqlalchemy.Table:
        """Return a table object from a table name.

        If 'shallow_okay' is True, the table will be returned without requiring properties to
        be read from the database.

        To prevent unnecessary round-trips to the database, the table is cached after the first
        query. To ignore the cache and force a refresh, set 'force_refresh' to True.
        """
        if force_refresh and shallow_okay:
            raise exc.AirbyteLibInternalError(
                message="Cannot force refresh and use shallow query at the same time."
            )

        if force_refresh or table_name not in self._cached_table_definitions:
            if shallow_okay:
                # Return a shallow instance, without column declarations. Do not cache
                # the table definition in this case.
                return sqlalchemy.Table(
                    table_name,
                    sqlalchemy.MetaData(schema=self.cache.schema_name),
                )

            self._cached_table_definitions[table_name] = sqlalchemy.Table(
                table_name,
                sqlalchemy.MetaData(schema=self.cache.schema_name),
                autoload_with=self.get_sql_engine(),
            )

        return self._cached_table_definitions[table_name]

    def _ensure_schema_exists(
        self,
    ) -> None:
        """Return a new (unique) temporary table name."""
        schema_name = self.cache.schema_name
        if schema_name in self._get_schemas_list():
            return

        sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

        try:
            self._execute_sql(sql)
        except Exception as ex:
            # Ignore schema exists errors.
            if "already exists" not in str(ex):
                raise

        if DEBUG_MODE:
            found_schemas = self._get_schemas_list()
            assert (
                schema_name in found_schemas
            ), f"Schema {schema_name} was not created. Found: {found_schemas}"

    def _quote_identifier(self, identifier: str) -> str:
        """Return the given identifier, quoted."""
        return f'"{identifier}"'

    @final
    def _get_temp_table_name(
        self,
        stream_name: str,
        batch_id: str | None = None,  # ULID of the batch
    ) -> str:
        """Return a new (unique) temporary table name."""
        batch_id = batch_id or str(ulid.ULID())
        return self.normalizer.normalize(f"{stream_name}_{batch_id}")

    def _fully_qualified(
        self,
        table_name: str,
    ) -> str:
        """Return the fully qualified name of the given table."""
        return f"{self.cache.schema_name}.{self._quote_identifier(table_name)}"

    @final
    def _create_table_for_loading(
        self,
        /,
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Create a new table for loading data."""
        temp_table_name = self._get_temp_table_name(stream_name, batch_id)
        column_definition_str = ",\n  ".join(
            f"{self._quote_identifier(column_name)} {sql_type}"
            for column_name, sql_type in self._get_sql_column_definitions(stream_name).items()
        )
        self._create_table(temp_table_name, column_definition_str)

        return temp_table_name

    def _get_tables_list(
        self,
    ) -> list[str]:
        """Return a list of all tables in the database."""
        with self.get_sql_connection() as conn:
            inspector: Inspector = sqlalchemy.inspect(conn)
            return inspector.get_table_names(schema=self.cache.schema_name)

    def _get_schemas_list(
        self,
        database_name: str | None = None,
    ) -> list[str]:
        """Return a list of all tables in the database."""
        inspector: Inspector = sqlalchemy.inspect(self.get_sql_engine())
        database_name = database_name or self.database_name
        found_schemas = inspector.get_schema_names()
        return [
            found_schema.split(".")[-1].strip('"')
            for found_schema in found_schemas
            if "." not in found_schema
            or (found_schema.split(".")[0].lower().strip('"') == database_name.lower())
        ]

    def _ensure_final_table_exists(
        self,
        stream_name: str,
        *,
        create_if_missing: bool = True,
    ) -> str:
        """Create the final table if it doesn't already exist.

        Return the table name.
        """
        table_name = self.get_sql_table_name(stream_name)
        did_exist = self._table_exists(table_name)
        if not did_exist and create_if_missing:
            column_definition_str = ",\n  ".join(
                f"{self._quote_identifier(column_name)} {sql_type}"
                for column_name, sql_type in self._get_sql_column_definitions(
                    stream_name,
                ).items()
            )
            self._create_table(table_name, column_definition_str)

        return table_name

    def _ensure_compatible_table_schema(
        self,
        stream_name: str,
        *,
        raise_on_error: bool = False,
    ) -> bool:
        """Return true if the given table is compatible with the stream's schema.

        If raise_on_error is true, raise an exception if the table is not compatible.

        TODO: Expand this to check for column types and sizes, and to add missing columns.

        Returns true if the table is compatible, false if it is not.
        """
        json_schema = self.get_stream_json_schema(stream_name)
        stream_column_names: list[str] = json_schema["properties"].keys()
        table_column_names: list[str] = self.get_sql_table(stream_name).columns.keys()

        lower_case_table_column_names = self.normalizer.normalize_set(table_column_names)
        missing_columns = [
            stream_col
            for stream_col in stream_column_names
            if self.normalizer.normalize(stream_col) not in lower_case_table_column_names
        ]
        if missing_columns:
            if raise_on_error:
                raise exc.AirbyteLibCacheTableValidationError(
                    violation="Cache table is missing expected columns.",
                    context={
                        "stream_column_names": stream_column_names,
                        "table_column_names": table_column_names,
                        "missing_columns": missing_columns,
                    },
                )
            return False  # Some columns are missing.

        return True  # All columns exist.

    @final
    def _create_table(
        self,
        table_name: str,
        column_definition_str: str,
        primary_keys: list[str] | None = None,
    ) -> None:
        if primary_keys:
            pk_str = ", ".join(primary_keys)
            column_definition_str += f",\n  PRIMARY KEY ({pk_str})"

        cmd = f"""
        CREATE TABLE {self._fully_qualified(table_name)} (
            {column_definition_str}
        )
        """
        _ = self._execute_sql(cmd)

    def get_stream_properties(
        self,
        stream_name: str,
    ) -> dict[str, dict]:
        """Return the names of the top-level properties for the given stream."""
        return self.get_stream_json_schema(stream_name)["properties"]

    @final
    def _get_sql_column_definitions(
        self,
        stream_name: str,
    ) -> dict[str, sqlalchemy.types.TypeEngine]:
        """Return the column definitions for the given stream."""
        columns: dict[str, sqlalchemy.types.TypeEngine] = {}
        properties = self.get_stream_properties(stream_name)
        for property_name, json_schema_property_def in properties.items():
            clean_prop_name = self.normalizer.normalize(property_name)
            columns[clean_prop_name] = self.type_converter.to_sql_type(
                json_schema_property_def,
            )

        columns[AB_RAW_ID_COLUMN] = self.type_converter_class.get_string_type()
        columns[AB_EXTRACTED_AT_COLUMN] = sqlalchemy.TIMESTAMP()
        columns[AB_META_COLUMN] = self.type_converter_class.get_json_type()

        return columns

    @final
    def write_stream_data(
        self,
        stream_name: str,
        write_strategy: WriteStrategy,
    ) -> list[BatchHandle]:
        """Finalize all uncommitted batches.

        This is a generic 'final' SQL implementation, which should not be overridden.

        Returns a mapping of batch IDs to batch handles, for those processed batches.

        TODO: Add a dedupe step here to remove duplicates from the temp table.
              Some sources will send us duplicate records within the same stream,
              although this is a fairly rare edge case we can ignore in V1.
        """
        # Flush any pending writes
        self.file_writer.flush_active_batches()

        with self.finalizing_batches(stream_name) as batches_to_finalize:
            # Make sure the target schema and target table exist.
            self._ensure_schema_exists()
            final_table_name = self._ensure_final_table_exists(
                stream_name,
                create_if_missing=True,
            )
            self._ensure_compatible_table_schema(
                stream_name=stream_name,
                raise_on_error=True,
            )

            if not batches_to_finalize:
                # If there are no batches to finalize, return after ensuring the table exists.
                return []

            files: list[Path] = []
            # Get a list of all files to finalize from all pending batches.
            for batch_handle in batches_to_finalize:
                files += batch_handle.files
            # Use the max batch ID as the batch ID for table names.
            max_batch_id = max([batch.batch_id for batch in batches_to_finalize])

            temp_table_name = self._write_files_to_new_table(
                files=files,
                stream_name=stream_name,
                batch_id=max_batch_id,
            )
            try:
                self._write_temp_table_to_final_table(
                    stream_name=stream_name,
                    temp_table_name=temp_table_name,
                    final_table_name=final_table_name,
                    write_strategy=write_strategy,
                )
            finally:
                self._drop_temp_table(temp_table_name, if_exists=True)

        progress.log_stream_finalized(stream_name)

        # Return the batch handles as measure of work completed.
        return batches_to_finalize

    @final
    def cleanup_all(self) -> None:
        """Clean resources."""
        self.file_writer.cleanup_all()

    # Finalizing context manager

    @final
    @contextlib.contextmanager
    def finalizing_batches(
        self,
        stream_name: str,
    ) -> Generator[list[BatchHandle], str, None]:
        """Context manager to use for finalizing batches, if applicable.

        Returns a mapping of batch IDs to batch handles, for those processed batches.
        """
        batches_to_finalize: list[BatchHandle] = self.file_writer.get_pending_batches(stream_name)
        state_messages_to_finalize: list[AirbyteStateMessage] = self._pending_state_messages[
            stream_name
        ].copy()
        self._pending_state_messages[stream_name].clear()

        progress.log_batches_finalizing(stream_name, len(batches_to_finalize))
        yield batches_to_finalize
        self._finalize_state_messages(stream_name, state_messages_to_finalize)
        progress.log_batches_finalized(stream_name, len(batches_to_finalize))

        for batch_handle in batches_to_finalize:
            batch_handle.finalized = True

        self._finalized_state_messages[stream_name] += state_messages_to_finalize

        if self.cache.cleanup:
            for batch_handle in batches_to_finalize:
                batch_handle.delete_files()

    def _execute_sql(self, sql: str | TextClause | Executable) -> CursorResult:
        """Execute the given SQL statement."""
        if isinstance(sql, str):
            sql = text(sql)
        if isinstance(sql, TextClause):
            sql = sql.execution_options(
                autocommit=True,
            )

        with self.get_sql_connection() as conn:
            try:
                result = conn.execute(sql)
            except (
                sqlalchemy.exc.ProgrammingError,
                sqlalchemy.exc.SQLAlchemyError,
            ) as ex:
                msg = f"Error when executing SQL:\n{sql}\n{type(ex).__name__}{ex!s}"
                raise SQLRuntimeError(msg) from None  # from ex

        return result

    def _drop_temp_table(
        self,
        table_name: str,
        *,
        if_exists: bool = True,
    ) -> None:
        """Drop the given table."""
        exists_str = "IF EXISTS" if if_exists else ""
        self._execute_sql(f"DROP TABLE {exists_str} {self._fully_qualified(table_name)}")

    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Write a file(s) to a new table.

        This is a generic implementation, which can be overridden by subclasses
        to improve performance.
        """
        temp_table_name = self._create_table_for_loading(stream_name, batch_id)
        for file_path in files:
            dataframe = pd.read_json(file_path, lines=True)

            sql_column_definitions: dict[str, TypeEngine] = self._get_sql_column_definitions(
                stream_name
            )

            # Remove fields that are not in the schema
            for col_name in dataframe.columns:
                if col_name not in sql_column_definitions:
                    dataframe = dataframe.drop(columns=col_name)

            # Pandas will auto-create the table if it doesn't exist, which we don't want.
            if not self._table_exists(temp_table_name):
                raise exc.AirbyteLibInternalError(
                    message="Table does not exist after creation.",
                    context={
                        "temp_table_name": temp_table_name,
                    },
                )

            # Normalize all column names to lower case.
            dataframe.columns = Index([self.normalizer.normalize(col) for col in dataframe.columns])

            # Write the data to the table.
            dataframe.to_sql(
                temp_table_name,
                self.get_sql_alchemy_url(),
                schema=self.cache.schema_name,
                if_exists="append",
                index=False,
                dtype=sql_column_definitions,
            )
        return temp_table_name

    def _add_column_to_table(
        self,
        table: Table,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Add a column to the given table."""
        self._execute_sql(
            text(
                f"ALTER TABLE {self._fully_qualified(table.name)} "
                f"ADD COLUMN {column_name} {column_type}"
            ),
        )

    def _add_missing_columns_to_table(
        self,
        stream_name: str,
        table_name: str,
    ) -> None:
        """Add missing columns to the table."""
        columns = self._get_sql_column_definitions(stream_name)
        table = self._get_table_by_name(table_name, force_refresh=True)
        for column_name, column_type in columns.items():
            if column_name not in table.columns:
                self._add_column_to_table(table, column_name, column_type)

    @final
    def _write_temp_table_to_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
        write_strategy: WriteStrategy,
    ) -> None:
        """Write the temp table into the final table using the provided write strategy."""
        has_pks: bool = bool(self._get_primary_keys(stream_name))
        has_incremental_key: bool = bool(self._get_incremental_key(stream_name))
        if write_strategy == WriteStrategy.MERGE and not has_pks:
            raise exc.AirbyteLibInputError(
                message="Cannot use merge strategy on a stream with no primary keys.",
                context={
                    "stream_name": stream_name,
                },
            )

        if write_strategy == WriteStrategy.AUTO:
            if has_pks:
                write_strategy = WriteStrategy.MERGE
            elif has_incremental_key:
                write_strategy = WriteStrategy.APPEND
            else:
                write_strategy = WriteStrategy.REPLACE

        if write_strategy == WriteStrategy.REPLACE:
            self._swap_temp_table_with_final_table(
                stream_name=stream_name,
                temp_table_name=temp_table_name,
                final_table_name=final_table_name,
            )
            return

        if write_strategy == WriteStrategy.APPEND:
            self._add_missing_columns_to_table(
                stream_name=stream_name,
                table_name=final_table_name,
            )
            self._append_temp_table_to_final_table(
                stream_name=stream_name,
                temp_table_name=temp_table_name,
                final_table_name=final_table_name,
            )
            return

        if write_strategy == WriteStrategy.MERGE:
            self._add_missing_columns_to_table(
                stream_name=stream_name,
                table_name=final_table_name,
            )
            if not self.supports_merge_insert:
                # Fallback to emulated merge if the database does not support merge natively.
                self._emulated_merge_temp_table_to_final_table(
                    stream_name=stream_name,
                    temp_table_name=temp_table_name,
                    final_table_name=final_table_name,
                )
                return

            self._merge_temp_table_to_final_table(
                stream_name=stream_name,
                temp_table_name=temp_table_name,
                final_table_name=final_table_name,
            )
            return

        raise exc.AirbyteLibInternalError(
            message="Write strategy is not supported.",
            context={
                "write_strategy": write_strategy,
            },
        )

    def _append_temp_table_to_final_table(
        self,
        temp_table_name: str,
        final_table_name: str,
        stream_name: str,
    ) -> None:
        nl = "\n"
        columns = [self._quote_identifier(c) for c in self._get_sql_column_definitions(stream_name)]
        self._execute_sql(
            f"""
            INSERT INTO {self._fully_qualified(final_table_name)} (
            {f',{nl}  '.join(columns)}
            )
            SELECT
            {f',{nl}  '.join(columns)}
            FROM {self._fully_qualified(temp_table_name)}
            """,
        )

    def _get_primary_keys(
        self,
        stream_name: str,
    ) -> list[str]:
        pks = self._get_stream_config(stream_name).primary_key
        if not pks:
            return []

        joined_pks = [".".join(pk) for pk in pks]
        for pk in joined_pks:
            if "." in pk:
                msg = "Nested primary keys are not yet supported. Found: {pk}"
                raise NotImplementedError(msg)

        return joined_pks

    def _get_incremental_key(
        self,
        stream_name: str,
    ) -> str | None:
        return self._get_stream_config(stream_name).cursor_field

    def _swap_temp_table_with_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Merge the temp table into the main one.

        This implementation requires MERGE support in the SQL DB.
        Databases that do not support this syntax can override this method.
        """
        if final_table_name is None:
            raise exc.AirbyteLibInternalError(message="Arg 'final_table_name' cannot be None.")
        if temp_table_name is None:
            raise exc.AirbyteLibInternalError(message="Arg 'temp_table_name' cannot be None.")

        _ = stream_name
        deletion_name = f"{final_table_name}_deleteme"
        commands = "\n".join(
            [
                f"ALTER TABLE {self._fully_qualified(final_table_name)} RENAME "
                f"TO {deletion_name};",
                f"ALTER TABLE {self._fully_qualified(temp_table_name)} RENAME "
                f"TO {final_table_name};",
                f"DROP TABLE {self._fully_qualified(deletion_name)};",
            ]
        )
        self._execute_sql(commands)

    def _merge_temp_table_to_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Merge the temp table into the main one.

        This implementation requires MERGE support in the SQL DB.
        Databases that do not support this syntax can override this method.
        """
        nl = "\n"
        columns = {self._quote_identifier(c) for c in self._get_sql_column_definitions(stream_name)}
        pk_columns = {self._quote_identifier(c) for c in self._get_primary_keys(stream_name)}
        non_pk_columns = columns - pk_columns
        join_clause = f"{nl} AND ".join(f"tmp.{pk_col} = final.{pk_col}" for pk_col in pk_columns)
        set_clause = f"{nl}  , ".join(f"{col} = tmp.{col}" for col in non_pk_columns)
        self._execute_sql(
            f"""
            MERGE INTO {self._fully_qualified(final_table_name)} final
            USING (
            SELECT *
            FROM {self._fully_qualified(temp_table_name)}
            ) AS tmp
            ON {join_clause}
            WHEN MATCHED THEN UPDATE
            SET
                {set_clause}
            WHEN NOT MATCHED THEN INSERT
            (
                {f',{nl}    '.join(columns)}
            )
            VALUES (
                tmp.{f',{nl}    tmp.'.join(columns)}
            );
            """,
        )

    def _get_column_by_name(self, table: str | Table, column_name: str) -> Column:
        """Return the column object for the given column name.

        This method is case-insensitive.
        """
        if isinstance(table, str):
            table = self._get_table_by_name(table)
        try:
            # Try to get the column in a case-insensitive manner
            return next(col for col in table.c if col.name.lower() == column_name.lower())
        except StopIteration:
            raise exc.AirbyteLibInternalError(
                message="Could not find matching column.",
                context={
                    "table": table,
                    "column_name": column_name,
                },
            ) from None

    def _emulated_merge_temp_table_to_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Emulate the merge operation using a series of SQL commands.

        This is a fallback implementation for databases that do not support MERGE.
        """
        final_table = self._get_table_by_name(final_table_name)
        temp_table = self._get_table_by_name(temp_table_name)
        pk_columns = self._get_primary_keys(stream_name)

        columns_to_update: set[str] = self._get_sql_column_definitions(
            stream_name=stream_name
        ).keys() - set(pk_columns)

        # Create a dictionary mapping columns in users_final to users_stage for updating
        update_values = {
            self._get_column_by_name(final_table, column): (
                self._get_column_by_name(temp_table, column)
            )
            for column in columns_to_update
        }

        # Craft the WHERE clause for composite primary keys
        join_conditions = [
            self._get_column_by_name(final_table, pk_column)
            == self._get_column_by_name(temp_table, pk_column)
            for pk_column in pk_columns
        ]
        join_clause = and_(*join_conditions)

        # Craft the UPDATE statement
        update_stmt = update(final_table).values(update_values).where(join_clause)

        # Define a join between temp_table and final_table
        joined_table = temp_table.outerjoin(final_table, join_clause)

        # Define a condition that checks for records in temp_table that do not have a corresponding
        # record in final_table
        where_not_exists_clause = self._get_column_by_name(final_table, pk_columns[0]) == null()

        # Select records from temp_table that are not in final_table
        select_new_records_stmt = (
            select([temp_table]).select_from(joined_table).where(where_not_exists_clause)
        )

        # Craft the INSERT statement using the select statement
        insert_new_records_stmt = insert(final_table).from_select(
            names=[column.name for column in temp_table.columns], select=select_new_records_stmt
        )

        if DEBUG_MODE:
            print(str(update_stmt))
            print(str(insert_new_records_stmt))

        with self.get_sql_connection() as conn:
            conn.execute(update_stmt)
            conn.execute(insert_new_records_stmt)

    def _table_exists(
        self,
        table_name: str,
    ) -> bool:
        """Return true if the given table exists.

        Subclasses may override this method to provide a more efficient implementation.
        """
        return table_name in self._get_tables_list()
