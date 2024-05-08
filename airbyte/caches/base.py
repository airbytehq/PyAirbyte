# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A SQL Cache implementation."""

from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, final

from pydantic import BaseModel, PrivateAttr

from airbyte.datasets._sql import CachedDataset


if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Engine

    from airbyte._future_cdk.catalog_manager import CatalogManagerBase
    from airbyte._future_cdk.sql_processor import SqlProcessorBase
    from airbyte._future_cdk.state_manager import StateManagerBase
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
    _state_manager: Optional[StateManagerBase] = PrivateAttr(default=None)

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
    def streams(
        self,
    ) -> dict[str, CachedDataset]:
        """Return a temporary table name."""
        result = {}
        stream_names = self.processor.expected_streams
        stream_names |= set(self.catalog_manager.stream_names)

        for stream_name in stream_names:
            result[stream_name] = CachedDataset(self, stream_name)

        return result

    def _get_state(
        self,
        source_name: str,
        streams: list[str] | None,
    ) -> list[dict[str, Any]] | None:
        """Return the state artifact for all requested streams."""
        return self.state_manager.get_state(
            source_name=source_name,
            streams=streams,
        )

    @property
    def catalog_manager(
        self,
    ) -> CatalogManagerBase:
        """Return the catalog manager from the record processor."""
        return self.processor.catalog_manager

    @property
    def state_manager(
        self,
    ) -> StateManagerBase:
        """Return the state manager from the record processor."""
        return self.processor.state_manager

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
