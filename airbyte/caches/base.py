# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""SQL Cache implementation."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, final

import pandas as pd
from pydantic import Field, PrivateAttr

from airbyte_protocol.models import ConfiguredAirbyteCatalog

from airbyte._future_cdk.catalog_providers import CatalogProvider
from airbyte._future_cdk.sql_processor import (
    SqlConfig,
    SqlProcessorBase,
)
from airbyte._future_cdk.state_writers import StdOutStateWriter
from airbyte.caches._catalog_backend import CatalogBackendBase, SqlCatalogBackend
from airbyte.caches._state_backend import SqlStateBackend
from airbyte.datasets._sql import CachedDataset


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte._future_cdk.sql_processor import SqlProcessorBase
    from airbyte._future_cdk.state_providers import StateProviderBase
    from airbyte._future_cdk.state_writers import StateWriterBase
    from airbyte.caches._state_backend_base import StateBackendBase
    from airbyte.datasets._base import DatasetBase


class CacheBase(SqlConfig):
    """Base configuration for a cache.

    Caches inherit from the matching `SqlConfig` class, which provides the SQL config settings
    and basic connectivity to the SQL database.

    The cache is responsible for managing the state of the data synced to the cache, including the
    stream catalog and stream state. The cache also provides the mechanism to read and write data
    to the SQL backend specified in the `SqlConfig` class.
    """

    cache_dir: Path = Field(default=Path(".cache"))
    """The directory to store the cache in."""

    cleanup: bool = True
    """Whether to clean up the cache after use."""

    _deployed_api_root: Optional[str] = PrivateAttr(default=None)
    _deployed_workspace_id: Optional[str] = PrivateAttr(default=None)
    _deployed_destination_id: Optional[str] = PrivateAttr(default=None)

    _sql_processor_class: type[SqlProcessorBase] = PrivateAttr()
    _read_processor: SqlProcessorBase = PrivateAttr()

    _catalog_backend: CatalogBackendBase = PrivateAttr()
    _state_backend: StateBackendBase = PrivateAttr()

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
            engine=self.get_sql_engine(),
            table_prefix=self.table_prefix or "",
        )
        self._state_backend = SqlStateBackend(
            engine=self.get_sql_engine(),
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
    def processor(self) -> SqlProcessorBase:
        """Return the SQL processor instance."""
        return self._read_processor

    def get_record_processor(
        self,
        source_name: str,
        catalog_provider: CatalogProvider,
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
            state_writer=self.get_state_writer(source_name=source_name),
            temp_dir=self.cache_dir,
            temp_file_cleanup=self.cleanup,
        )

    # Read methods:

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

    @final
    @property
    def streams(self) -> dict[str, CachedDataset]:
        """Return a temporary table name."""
        result = {}
        stream_names = set(self._catalog_backend.stream_names)

        for stream_name in stream_names:
            result[stream_name] = CachedDataset(self, stream_name)

        return result

    def get_state_provider(
        self,
        source_name: str,
        *,
        refresh: bool = True,
    ) -> StateProviderBase:
        """Return a state provider for the specified source name."""
        return self._state_backend.get_state_provider(
            source_name=source_name,
            table_prefix=self.table_prefix or "",
            refresh=refresh,
        )

    def get_state_writer(
        self,
        source_name: str,
    ) -> StateWriterBase:
        """Return a state writer for the specified source name."""
        return self._state_backend.get_state_writer(source_name=source_name)

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

    def __getitem__(self, stream: str) -> DatasetBase:
        """Return a dataset by stream name."""
        return self.streams[stream]

    def __contains__(self, stream: str) -> bool:
        """Return whether a stream is in the cache."""
        return stream in (self._catalog_backend.stream_names)

    def __iter__(  # type: ignore [override]  # Overriding Pydantic model method
        self,
    ) -> Iterator[tuple[str, Any]]:
        """Iterate over the streams in the cache."""
        return ((name, dataset) for name, dataset in self.streams.items())
