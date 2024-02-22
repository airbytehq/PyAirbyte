# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A SQL Cache implementation."""

from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING, Any, final

from pydantic import BaseModel, PrivateAttr

from airbyte.datasets._sql import CachedDataset


if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.engine import Engine

    from airbyte._processors.sql.base import SqlProcessorBase
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

    table_prefix: str | None = None
    """ A prefix to add to all table names.
    If 'None', a prefix will be created based on the source name.
    """

    table_suffix: str = ""
    """A suffix to add to all table names."""

    _sql_processor_class: type[SqlProcessorBase] = PrivateAttr()
    _sql_processor: SqlProcessorBase | None = PrivateAttr(default=None)

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
        if self.processor._catalog_manager is not None:  # noqa: SLF001
            stream_names |= set(self.processor._catalog_manager.stream_names)  # noqa: SLF001
        for stream_name in stream_names:
            result[stream_name] = CachedDataset(self, stream_name)

        return result

    def __getitem__(self, stream: str) -> DatasetBase:
        return self.streams[stream]

    def __contains__(self, stream: str) -> bool:
        return stream in (self.processor.expected_streams)

    def __iter__(self) -> Generator[tuple[str, Any], None, None]:
        return ((name, dataset) for name, dataset in self.streams.items())
