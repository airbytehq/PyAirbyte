# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""SQL Cache implementation."""

from __future__ import annotations

from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, ClassVar, Literal, final

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pydantic import Field, PrivateAttr
from sqlalchemy import text

from airbyte import constants
from airbyte._util.text_util import generate_ulid
from airbyte._writers.base import AirbyteWriterInterface
from airbyte.caches._catalog_backend import CatalogBackendBase, SqlCatalogBackend
from airbyte.caches._state_backend import SqlStateBackend
from airbyte.constants import DEFAULT_ARROW_MAX_CHUNK_SIZE, TEMP_FILE_CLEANUP
from airbyte.datasets._sql import CachedDataset
from airbyte.shared.catalog_providers import CatalogProvider
from airbyte.shared.sql_processor import SqlConfig
from airbyte.shared.state_writers import StdOutStateWriter
from airbyte_protocol.models import ConfiguredAirbyteCatalog


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte._message_iterators import AirbyteMessageIterator
    from airbyte.caches._state_backend_base import StateBackendBase
    from airbyte.lakes import FastLoadResult, FastUnloadResult, LakeStorage
    from airbyte.progress import ProgressTracker
    from airbyte.shared.sql_processor import SqlProcessorBase
    from airbyte.shared.state_providers import StateProviderBase
    from airbyte.shared.state_writers import StateWriterBase
    from airbyte.sources.base import Source
    from airbyte.strategies import WriteStrategy


DEFAULT_LAKE_STORE_OUTPUT_PREFIX: str = "airbyte/lake/output/{stream_name}/batch-{batch_id}/"


class CacheBase(SqlConfig, AirbyteWriterInterface):  # noqa: PLR0904
    """Base configuration for a cache.

    Caches inherit from the matching `SqlConfig` class, which provides the SQL config settings
    and basic connectivity to the SQL database.

    The cache is responsible for managing the state of the data synced to the cache, including the
    stream catalog and stream state. The cache also provides the mechanism to read and write data
    to the SQL backend specified in the `SqlConfig` class.
    """

    cache_dir: Path = Field(default=Path(constants.DEFAULT_CACHE_ROOT))
    """The directory to store the cache in."""

    cleanup: bool = TEMP_FILE_CLEANUP
    """Whether to clean up the cache after use."""

    _name: str = PrivateAttr()

    _sql_processor_class: ClassVar[type[SqlProcessorBase]]
    _read_processor: SqlProcessorBase = PrivateAttr()

    _catalog_backend: CatalogBackendBase = PrivateAttr()
    _state_backend: StateBackendBase = PrivateAttr()

    paired_destination_name: ClassVar[str | None] = None
    paired_destination_config_class: ClassVar[type | None] = None

    @property
    def paired_destination_config(self) -> Any | dict[str, Any]:  # noqa: ANN401  # Allow Any return type
        """Return a dictionary of destination configuration values."""
        raise NotImplementedError(
            f"The type '{type(self).__name__}' does not define an equivalent destination "
            "configuration."
        )

    @final
    def __init__(self, **data: Any) -> None:  # noqa: ANN401
        """Initialize the cache and backends."""
        super().__init__(**data)

        # Create a temporary processor to do the work of ensuring the schema exists
        temp_processor = self._sql_processor_class(
            sql_config=self,
            catalog_provider=CatalogProvider(ConfiguredAirbyteCatalog(streams=[])),
            state_writer=StdOutStateWriter(),
            temp_dir=self.cache_dir,
            temp_file_cleanup=self.cleanup,
        )
        temp_processor._ensure_schema_exists()  # noqa: SLF001  # Accessing non-public member

        # Initialize the catalog and state backends
        self._catalog_backend = SqlCatalogBackend(
            sql_config=self,
            table_prefix=self.table_prefix or "",
        )
        self._state_backend = SqlStateBackend(
            sql_config=self,
            table_prefix=self.table_prefix or "",
        )

        # Now we can create the SQL read processor
        self._read_processor = self._sql_processor_class(
            sql_config=self,
            catalog_provider=self._catalog_backend.get_full_catalog_provider(),
            state_writer=StdOutStateWriter(),  # Shouldn't be needed for the read-only processor
            temp_dir=self.cache_dir,
            temp_file_cleanup=self.cleanup,
        )

    @final
    @property
    def config_hash(self) -> str | None:
        """Return a hash of the cache configuration.

        This is the same as the SQLConfig hash from the superclass.
        """
        return super(SqlConfig, self).config_hash

    @final
    def execute_sql(self, sql: str | list[str]) -> None:
        """Execute one or more SQL statements against the cache's SQL backend.

        If multiple SQL statements are given, they are executed in order,
        within the same transaction.

        This method is useful for creating tables, indexes, and other
        schema objects in the cache. It does not return any results and it
        automatically closes the connection after executing all statements.

        This method is not intended for querying data. For that, use the `get_records`
        method - or for a low-level interface, use the `get_sql_engine` method.

        If any of the statements fail, the transaction is canceled and an exception
        is raised. Most databases will rollback the transaction in this case.
        """
        if isinstance(sql, str):
            # Coerce to a list if a single string is given
            sql = [sql]

        with self.processor.get_sql_connection() as connection:
            for sql_statement in sql:
                connection.execute(text(sql_statement))

    @final
    @property
    def processor(self) -> SqlProcessorBase:
        """Return the SQL processor instance."""
        return self._read_processor

    @final
    def get_record_processor(
        self,
        source_name: str,
        catalog_provider: CatalogProvider,
        state_writer: StateWriterBase | None = None,
    ) -> SqlProcessorBase:
        """Return a record processor for the specified source name and catalog.

        We first register the source and its catalog with the catalog manager. Then we create a new
        SQL processor instance with (only) the given input catalog.

        For the state writer, we use a state writer which stores state in an internal SQL table.
        """
        # First register the source and catalog into durable storage. This is necessary to ensure
        # that we can later retrieve the catalog information.
        self.register_source(
            source_name=source_name,
            incoming_source_catalog=catalog_provider.configured_catalog,
            stream_names=set(catalog_provider.stream_names),
        )

        # Next create a new SQL processor instance with the given catalog - and a state writer
        # that writes state to the internal SQL table and associates with the given source name.
        return self._sql_processor_class(
            sql_config=self,
            catalog_provider=catalog_provider,
            state_writer=state_writer or self.get_state_writer(source_name=source_name),
            temp_dir=self.cache_dir,
            temp_file_cleanup=self.cleanup,
        )

    # Read methods:

    @final
    def get_records(
        self,
        stream_name: str,
    ) -> CachedDataset:
        """Uses SQLAlchemy to select all rows from the table."""
        return CachedDataset(self, stream_name)

    def get_pandas_dataframe(
        self,
        stream_name: str,
    ) -> pd.DataFrame:
        """Return a Pandas data frame with the stream's data."""
        table_name = self._read_processor.get_sql_table_name(stream_name)
        engine = self.get_sql_engine()
        return pd.read_sql_table(table_name, engine, schema=self.schema_name)

    def get_arrow_dataset(
        self,
        stream_name: str,
        *,
        max_chunk_size: int = DEFAULT_ARROW_MAX_CHUNK_SIZE,
    ) -> ds.Dataset:
        """Return an Arrow Dataset with the stream's data."""
        table_name = self._read_processor.get_sql_table_name(stream_name)
        engine = self.get_sql_engine()

        # Read the table in chunks to handle large tables which does not fits in memory
        pandas_chunks = pd.read_sql_table(
            table_name=table_name,
            con=engine,
            schema=self.schema_name,
            chunksize=max_chunk_size,
        )

        arrow_batches_list = []
        arrow_schema = None

        for pandas_chunk in pandas_chunks:
            if arrow_schema is None:
                # Initialize the schema with the first chunk
                arrow_schema = pa.Schema.from_pandas(pandas_chunk)

            # Convert each pandas chunk to an Arrow Table
            arrow_table = pa.RecordBatch.from_pandas(pandas_chunk, schema=arrow_schema)
            arrow_batches_list.append(arrow_table)

        return ds.dataset(arrow_batches_list)

    @final
    @property
    def streams(self) -> dict[str, CachedDataset]:
        """Return a temporary table name."""
        result = {}
        stream_names = set(self._catalog_backend.stream_names)

        for stream_name in stream_names:
            result[stream_name] = CachedDataset(self, stream_name)

        return result

    @final
    def __len__(self) -> int:
        """Gets the number of streams."""
        return len(self._catalog_backend.stream_names)

    @final
    def __bool__(self) -> bool:
        """Always True.

        This is needed so that caches with zero streams are not falsey (None-like).
        """
        return True

    @final
    def get_state_provider(
        self,
        source_name: str,
        *,
        refresh: bool = True,
        destination_name: str | None = None,
    ) -> StateProviderBase:
        """Return a state provider for the specified source name."""
        return self._state_backend.get_state_provider(
            source_name=source_name,
            table_prefix=self.table_prefix or "",
            refresh=refresh,
            destination_name=destination_name,
        )

    @final
    def get_state_writer(
        self,
        source_name: str,
        destination_name: str | None = None,
    ) -> StateWriterBase:
        """Return a state writer for the specified source name.

        If syncing to the cache, `destination_name` should be `None`.
        If syncing to a destination, `destination_name` should be the destination name.
        """
        return self._state_backend.get_state_writer(
            source_name=source_name,
            destination_name=destination_name,
        )

    @final
    def register_source(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        stream_names: set[str],
    ) -> None:
        """Register the source name and catalog."""
        self._catalog_backend.register_source(
            source_name=source_name,
            incoming_source_catalog=incoming_source_catalog,
            incoming_stream_names=stream_names,
        )

    @final
    def create_source_tables(
        self,
        source: Source,
        streams: Literal["*"] | list[str] | None = None,
    ) -> None:
        """Create tables in the cache for the provided source if they do not exist already.

        Tables are created based upon the Source's catalog.

        Args:
            source: The source to create tables for.
            streams: Stream names to create tables for. If None, use the Source's selected_streams
                or "*" if neither is set. If "*", all available streams will be used.
        """
        if streams is None:
            streams = source.get_selected_streams() or "*"

        catalog_provider = CatalogProvider(source.get_configured_catalog(streams=streams))

        # Register the incoming source catalog
        self.register_source(
            source_name=source.name,
            incoming_source_catalog=catalog_provider.configured_catalog,
            stream_names=set(catalog_provider.stream_names),
        )

        # Ensure schema exists
        self.processor._ensure_schema_exists()  # noqa: SLF001  # Accessing non-public member

        # Create tables for each stream if they don't exist
        for stream_name in catalog_provider.stream_names:
            self.processor._ensure_final_table_exists(  # noqa: SLF001
                stream_name=stream_name,
                create_if_missing=True,
            )

    @final
    def __getitem__(self, stream: str) -> CachedDataset:
        """Return a dataset by stream name."""
        return self.streams[stream]

    @final
    def __contains__(self, stream: str) -> bool:
        """Return whether a stream is in the cache."""
        return stream in (self._catalog_backend.stream_names)

    @final
    def __iter__(  # type: ignore [override]  # Overriding Pydantic model method
        self,
    ) -> Iterator[tuple[str, Any]]:
        """Iterate over the streams in the cache."""
        return ((name, dataset) for name, dataset in self.streams.items())

    @final
    def _write_airbyte_message_stream(
        self,
        stdin: IO[str] | AirbyteMessageIterator,
        *,
        catalog_provider: CatalogProvider,
        write_strategy: WriteStrategy,
        state_writer: StateWriterBase | None = None,
        progress_tracker: ProgressTracker,
    ) -> None:
        """Read from the connector and write to the cache."""
        cache_processor = self.get_record_processor(
            source_name=self.name,
            catalog_provider=catalog_provider,
            state_writer=state_writer,
        )
        cache_processor.process_airbyte_messages(
            messages=stdin,
            write_strategy=write_strategy,
            progress_tracker=progress_tracker,
        )
        progress_tracker.log_cache_processing_complete()

    @final
    def _resolve_lake_store_path(
        self,
        lake_store_prefix: str,
        stream_name: str | None = None,
        batch_id: str | None = None,
    ) -> str:
        """Resolve the lake path prefix.

        The string is interpolated with "{stream_name}" and "{batch_id}" if requested.

        If `stream_name` is requested but not provided, it raises a ValueError.
        If `batch_id` is requested but not provided, it defaults to a generated ULID.
        """
        if lake_store_prefix is None:
            raise ValueError(
                "lake_store_prefix must be provided. Use DEFAULT_LAKE_STORE_OUTPUT_PREFIX if needed."
            )

        if "{stream_name}" in lake_store_prefix:
            if stream_name is not None:
                lake_store_prefix = lake_store_prefix.format(stream_name=stream_name)
            else:
                raise ValueError(
                    "stream_name must be provided when lake_store_prefix contains {stream_name}."
                )

        if "{batch_id}" in lake_store_prefix:
            batch_id = batch_id or generate_ulid()
            lake_store_prefix = lake_store_prefix.format(
                batch_id=batch_id,
            )

        return lake_store_prefix

    @final
    def fast_unload_streams(
        self,
        lake_store: LakeStorage,
        *,
        lake_store_prefix: str = DEFAULT_LAKE_STORE_OUTPUT_PREFIX,
        streams: list[str] | Literal["*"] | None = None,
    ) -> list[FastUnloadResult]:
        """Unload the cache to a lake store.

        We dump data directly to parquet files in the lake store.

        Args:
            streams: The streams to unload. If None, unload all streams.
            lake_store: The lake store to unload to. If None, use the default lake store.
        """
        stream_names: list[str]
        if streams == "*" or streams is None:
            stream_names = self._catalog_backend.stream_names
        elif isinstance(streams, list):
            stream_names = streams
        else:
            raise ValueError(
                f"Invalid streams argument: {streams}. Must be '*' or a list of stream names."
            )

        return [
            self.fast_unload_stream(
                stream_name=stream_name,
                lake_store=lake_store,
                lake_store_prefix=lake_store_prefix,
            )
            for stream_name in stream_names
        ]

    @final
    def fast_unload_stream(
        self,
        lake_store: LakeStorage,
        *,
        lake_store_prefix: str = DEFAULT_LAKE_STORE_OUTPUT_PREFIX,
        stream_name: str,
        **kwargs,
    ) -> FastUnloadResult:
        """Unload a single stream to the lake store.

        This generic implementation delegates to `fast_unload_table()`
        which subclasses should override for database-specific fast operations.

        The `lake_store_prefix` arg can be interpolated with {stream_name} to create a unique path
        for each stream.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name

        # Raises NotImplementedError if subclass does not implement this method:
        return self.fast_unload_table(
            lake_store=lake_store,
            lake_store_prefix=lake_store_prefix,
            stream_name=stream_name,
            table_name=table_name,
            **kwargs,
        )

    def fast_unload_table(
        self,
        table_name: str,
        lake_store: LakeStorage,
        *,
        lake_store_prefix: str = DEFAULT_LAKE_STORE_OUTPUT_PREFIX,
        db_name: str | None = None,
        schema_name: str | None = None,
        stream_name: str | None = None,
    ) -> FastUnloadResult:
        """Fast-unload a specific table to the designated lake storage.

        Subclasses should override this method to implement fast unloads.

        Subclasses should also ensure that the `lake_store_prefix` is resolved
        using the `_resolve_lake_store_path` method. E.g.:
        ```python
        lake_store_prefix = self._resolve_lake_store_path(
            lake_store_prefix=lake_store_prefix,
            stream_name=stream_name,
        )
        ```

        The `lake_store_prefix` arg can be interpolated with {stream_name} to create a unique path
        for each stream.
        """
        raise NotImplementedError

    @final
    def fast_load_streams(
        self,
        lake_store: LakeStorage,
        *,
        lake_store_prefix: str,
        streams: list[str],
        zero_copy: bool = False,
    ) -> None:
        """Unload the cache to a lake store.

        We dump data directly to parquet files in the lake store.

        The `lake_store_prefix` arg can be interpolated with {stream_name} to create a unique path
        for each stream.
        """
        for stream_name in streams:
            self.fast_load_stream(
                stream_name=stream_name,
                lake_store=lake_store,
                lake_store_prefix=lake_store_prefix or stream_name,
                zero_copy=zero_copy,
            )

    @final
    def fast_load_stream(
        self,
        lake_store: LakeStorage,
        *,
        stream_name: str,
        lake_store_prefix: str,
        zero_copy: bool = False,
    ) -> FastLoadResult:
        """Load a single stream from the lake store using fast native LOAD operations.

        The `lake_store_prefix` arg can be interpolated with {stream_name} to create a unique path
        for each stream.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name

        if zero_copy:
            raise NotImplementedError("Zero-copy loading is not yet supported in Snowflake.")

        return self.fast_load_table(
            table_name=table_name,
            lake_store=lake_store,
            lake_store_prefix=lake_store_prefix,
            zero_copy=zero_copy,
        )

    def fast_load_table(
        self,
        table_name: str,
        lake_store: LakeStorage,
        lake_store_prefix: str,
        *,
        db_name: str | None = None,
        schema_name: str | None = None,
        zero_copy: bool = False,
    ) -> FastLoadResult:
        """Fast-load a specific table from the designated lake storage.

        Subclasses should override this method to implement fast loads.

        The `lake_store_prefix` arg can be interpolated with {stream_name} to create a unique path
        for each stream.
        """
        raise NotImplementedError

    @final
    def fast_load_stream_from_unload_result(
        self,
        stream_name: str,
        unload_result: FastUnloadResult,
        *,
        zero_copy: bool = False,
    ) -> FastLoadResult:
        """Load the result of a fast unload operation."""
        return self.fast_load_stream(
            stream_name=stream_name,
            lake_store=unload_result.lake_store,
            lake_store_prefix=unload_result.lake_store_prefix,
            zero_copy=zero_copy,
        )

    @final
    def fast_load_table_from_unload_result(
        self,
        table_name: str,
        unload_result: FastUnloadResult,
        *,
        zero_copy: bool = False,
    ) -> FastLoadResult:
        """Load the result of a fast unload operation."""
        return self.fast_load_table(
            table_name=table_name,
            lake_store=unload_result.lake_store,
            lake_store_prefix=unload_result.lake_store_prefix,
            zero_copy=zero_copy,
        )
