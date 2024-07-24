# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from airbyte.datasets import CachedDataset


if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Engine

    from airbyte._future_cdk.catalog_providers import CatalogProvider
    from airbyte._future_cdk.state_writers import StateWriterBase
    from airbyte.caches import CacheBase
    from airbyte.destinations.base import Destination
    from airbyte.progress import ProgressTracker
    from airbyte.sources.base import Source


class ReadResult(Mapping[str, CachedDataset]):
    """The result of a read operation.

    This class is used to return information about the read operation, such as the number of
    records read. It should not be created directly, but instead returned by the write method
    of a destination.
    """

    def __init__(
        self,
        *,
        source_name: str,
        processed_streams: list[str],
        cache: CacheBase,
        progress_tracker: ProgressTracker,
    ) -> None:
        self.source_name = source_name
        self._progress_tracker = progress_tracker
        self._cache = cache
        self._processed_streams = processed_streams

    def __getitem__(self, stream: str) -> CachedDataset:
        if stream not in self._processed_streams:
            raise KeyError(stream)

        return CachedDataset(self._cache, stream)

    def __contains__(self, stream: object) -> bool:
        if not isinstance(stream, str):
            return False

        return stream in self._processed_streams

    def __iter__(self) -> Iterator[str]:
        return self._processed_streams.__iter__()

    def __len__(self) -> int:
        return len(self._processed_streams)

    def get_sql_engine(self) -> Engine:
        return self._cache.get_sql_engine()

    @property
    def processed_records(self) -> int:
        return self._progress_tracker.total_records_read

    @property
    def streams(self) -> Mapping[str, CachedDataset]:
        return {
            stream_name: CachedDataset(self._cache, stream_name)
            for stream_name in self._processed_streams
        }

    @property
    def cache(self) -> CacheBase:
        return self._cache


class WriteResult:
    """The result of a write operation.

    This class is used to return information about the write operation, such as the number of
    records written. It should not be created directly, but instead returned by the write method
    of a destination.
    """

    def __init__(
        self,
        *,
        destination: Destination,
        source_data: Source | ReadResult,
        catalog_provider: CatalogProvider,
        state_writer: StateWriterBase,
        progress_tracker: ProgressTracker,
    ) -> None:
        self._destination: Destination = destination
        self._source_data: Source | ReadResult = source_data
        self._catalog_provider: CatalogProvider = catalog_provider
        self._state_writer: StateWriterBase = state_writer
        self._progress_tracker: ProgressTracker = progress_tracker

    @property
    def processed_records(self) -> int:
        return self._progress_tracker.total_destination_records_delivered
