# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""SQL Cache implementation."""

from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast, final

from pydantic import BaseModel, PrivateAttr

from airbyte._catalog_manager import SqlCatalogManager
from airbyte._future_cdk.catalog_manager import CatalogManagerBase
from airbyte._state_backend import SqlStateBackend
from airbyte.datasets._sql import CachedDataset


if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Engine

    from airbyte_protocol.models import ConfiguredAirbyteCatalog

    from airbyte._future_cdk.sql_processor import SqlProcessorBase
    from airbyte._future_cdk.state.state_provider_base import StateProviderBase
    from airbyte._future_cdk.state.state_writer_base import StateWriterBase
    from airbyte.caches._state_backend_base import StateBackendBase
    from airbyte.datasets._base import DatasetBase


# TODO: meta=EnforceOverrides (Pydantic doesn't like it currently.)
class CacheBase(BaseModel):
    """Base configuration for a cache."""

    cache_dir: Path = Path(".cache")
    """The directory to store the cache in."""

    cleanup: bool = True
    """Whether to clean up the cache after use."""

    schema_name: str = "airbyte_raw"
    """The name of the schema to write to."""

    table_prefix: Optional[str] = None
    """ A prefix to add to all table names.
    If 'None', a prefix will be created based on the source name.
    """

    table_suffix: str = ""
    """A suffix to add to all table names."""

    _deployed_api_root: Optional[str] = PrivateAttr(default=None)
    _deployed_workspace_id: Optional[str] = PrivateAttr(default=None)
    _deployed_destination_id: Optional[str] = PrivateAttr(default=None)

    _sql_processor_class: type[SqlProcessorBase] = PrivateAttr()
    _sql_processor: Optional[SqlProcessorBase] = PrivateAttr(default=None)

    _catalog_manager: Optional[CatalogManagerBase] = PrivateAttr(default=None)
    _state_backend: Optional[StateBackendBase] = PrivateAttr(default=None)

    def _initialize_cache(self) -> None:
        """Initialize the SQL cache.

        This method creates the SQL processor and catalog manager.
        """
        if self._sql_processor is None:
            self._sql_processor = self._sql_processor_class(cache=self)

        if self._catalog_manager is None:
            self.processor._ensure_schema_exists()  # noqa: SLF001  # Accessing non-public member
            self._catalog_manager = SqlCatalogManager(
                engine=self._sql_processor.get_sql_engine(),
                table_prefix=self.table_prefix or "",
            )

    @final
    @property
    def processor(self) -> SqlProcessorBase:
        """Return the SQL processor instance."""
        if self._sql_processor is None:
            self._sql_processor = self._sql_processor_class(cache=self)

        return self._sql_processor

    @final
    def get_sql_engine(self) -> Engine:
        """Return a new SQL engine to use."""
        return self.processor.get_sql_engine()

    @abc.abstractmethod
    def get_sql_alchemy_url(self) -> str:
        """Returns a SQL Alchemy URL."""
        ...

    @abc.abstractmethod
    def get_database_name(self) -> str:
        """Return the name of the database."""
        ...

    @final
    @property
    def streams(self) -> dict[str, CachedDataset]:
        """Return a temporary table name."""
        result = {}
        stream_names = self.processor.expected_streams
        stream_names |= set(self.catalog_manager.stream_names)

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
        if self._state_backend is None:
            self._state_backend = SqlStateBackend(
                engine=self.get_sql_engine(),
                table_prefix=self.table_prefix or "",
            )

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
        if self._state_backend is None:
            self._state_backend = SqlStateBackend(
                engine=self.get_sql_engine(),
                table_prefix=self.table_prefix or "",
            )

        return self._state_backend.get_state_writer(
            source_name=source_name,
            table_prefix=self.table_prefix or "",
        )

    @property
    def catalog_manager(self) -> CatalogManagerBase:
        """Return the catalog manager from the record processor."""
        if self._catalog_manager is None:
            self._initialize_cache()

        return cast(CatalogManagerBase, self._catalog_manager)  # Not `None` at this point

    def register_source(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        stream_names: set[str],
    ) -> None:
        """Register the source name and catalog."""
        self.catalog_manager.register_source(
            source_name=source_name,
            incoming_source_catalog=incoming_source_catalog,
            incoming_stream_names=stream_names,
        )

    def __getitem__(self, stream: str) -> DatasetBase:
        """Return a dataset by stream name."""
        return self.streams[stream]

    def __contains__(self, stream: str) -> bool:
        """Return whether a stream is in the cache."""
        return stream in (self.processor.expected_streams)

    def __iter__(  # type: ignore [override]  # Overrides Pydantic BaseModel return type
        self,
    ) -> Iterator[tuple[str, Any]]:
        """Iterate over the streams in the cache."""
        return ((name, dataset) for name, dataset in self.streams.items())
