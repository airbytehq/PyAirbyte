# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A SQL Cache implementation."""

from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast, final

import ulid
from pydantic import BaseModel, PrivateAttr

from airbyte import exceptions as exc
from airbyte.caches._catalog_manager import CatalogManager
from airbyte.caches.util import get_default_cache
from airbyte.datasets._sql import CachedDataset
from airbyte.results import ReadResult
from airbyte.strategies import WriteStrategy


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

    table_prefix: Optional[str] = None
    """ A prefix to add to all table names.
    If 'None', a prefix will be created based on the source name.
    """

    table_suffix: str = ""
    """A suffix to add to all table names."""

    _sql_processor_class: type[SqlProcessorBase] = PrivateAttr()
    _sql_processor: Optional[SqlProcessorBase] = PrivateAttr(default=None)

    @property
    def name(self) -> str:
        """Return the name of the cache."""
        return self.__class__.__name__

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
        if self._has_catalog_manager:
            stream_names |= set(self._catalog_manager.stream_names)
        for stream_name in stream_names:
            result[stream_name] = CachedDataset(self, stream_name)

        return result

    def _get_state(
        self,
        source_name: str,
        streams: list[str] | None,
    ) -> list[dict[str, Any]] | None:
        return self._catalog_manager.get_state(
            source_name=source_name,
            streams=streams,
        )

    @property
    def _has_catalog_manager(
        self,
    ) -> bool:
        """Return whether the cache has a catalog manager."""
        # Member is private until we have a public API for it.
        return self.processor._catalog_manager is not None  # noqa: SLF001

    @property
    def _catalog_manager(
        self,
    ) -> CatalogManager:
        if not self._has_catalog_manager:
            raise exc.AirbyteLibInternalError(
                message="Catalog manager should exist but does not.",
            )

        # Member is private until we have a public API for it.
        return cast(CatalogManager, self.processor._catalog_manager)  # noqa: SLF001

    def __getitem__(self, stream: str) -> DatasetBase:
        return self.streams[stream]

    def __contains__(self, stream: str) -> bool:
        return stream in (self.processor.expected_streams)

    def __iter__(self) -> Generator[tuple[str, Any], None, None]:
        return ((name, dataset) for name, dataset in self.streams.items())

    # DB-As-Source

    def get_available_streams(self) -> list[str]:
        """Get the available streams from the spec."""
        return self.processor.get_tables_list()

    def _log_sync_start(self, cache: CacheBase) -> None:
        """TODO: Implement logging"""

    def _write_table_to_files(
        self,
        table: str,
        output_path: Path,
    ) -> None:
        """Write a table to files."""
        self._write_table_to_files(
            table=table,
            output_path=output_path,
        )

    def read(
        self,
        cache: CacheBase | None = None,
        *,
        streams: str | list[str] | None = None,
        write_strategy: str | WriteStrategy = WriteStrategy.AUTO,
        force_full_refresh: bool = False,
    ) -> ReadResult:
        """Read from the connector and write to the cache.

        Args:
            cache: The cache to write to. If None, a default cache will be used.
            write_strategy: The strategy to use when writing to the cache. If a string, it must be
                one of "append", "upsert", "replace", or "auto". If a WriteStrategy, it must be one
                of WriteStrategy.APPEND, WriteStrategy.UPSERT, WriteStrategy.REPLACE, or
                WriteStrategy.AUTO.
            streams: Optional if already set. A list of stream names to select for reading. If set
                to "*", all streams will be selected.
            force_full_refresh: If True, the source will operate in full refresh mode. Otherwise,
                streams will be read in incremental mode if supported by the connector. This option
                must be True when using the "replace" strategy.
        """
        if not streams:
            raise exc.AirbyteLibInputError(
                message="No streams selected.",
                context={
                    "available_streams": self.processor.get_tables_list(),
                },
            )

        if isinstance(streams, str):
            streams = [streams]

        if write_strategy == WriteStrategy.REPLACE and not force_full_refresh:
            raise exc.AirbyteLibInputError(
                message="The replace strategy requires full refresh mode.",
                context={
                    "write_strategy": write_strategy,
                    "force_full_refresh": force_full_refresh,
                },
            )
        if cache is None:
            cache = get_default_cache()

        if isinstance(write_strategy, str):
            try:
                write_strategy = WriteStrategy(write_strategy)
            except ValueError:
                raise exc.AirbyteLibInputError(
                    message="Invalid strategy",
                    context={
                        "write_strategy": write_strategy,
                        "available_strategies": [s.value for s in WriteStrategy],
                    },
                ) from None

        cache.processor.register_source(
            source_name=self.name,
            incoming_source_catalog=self.configured_catalog,
            stream_names=set(streams),
        )

        state = (
            self._get_state(
                source_name=self.name,
                streams=streams,
            )
            if not force_full_refresh
            else None
        )
        self._log_sync_start(cache=cache)
        job_files_root = Path(".") / "job_files" / ulid.ULID()
        for stream_name in streams:
            stream_files_path = job_files_root / stream_name
            self._write_table_to_files(
                table=self.processor.get_sql_table_name(stream_name),
                output_path=stream_files_path,
            )
            temp_table_name: str = cache.processor._write_files_to_new_table(
                files=(job_files_path / stream_name).files(),
                source_name=self.name,
                batch_id=ulid.ULID(),
            )
            final_table_name: str = cache.processor._get_final_table_name(
                stream_name=stream_name,
            )
            self._write_temp_table_to_final_table(
                stream_name=stream_name,
                temp_table_name=temp_table_name,
                final_table_name=final_table_name,
                write_strategy=write_strategy,
            )

        self._log_sync_success(cache=cache)
        return ReadResult(
            processed_records=self._processed_records,
            cache=cache,
            processed_streams=[stream.stream.name for stream in self.configured_catalog.streams],
        )
