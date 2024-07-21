# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from airbyte.datasets import CachedDataset


if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Engine

    from airbyte.caches import CacheBase
    from airbyte.progress import ReadProgress, WriteProgress


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
        processed_records: int,
        processed_streams: list[str],
        cache: CacheBase,
        progress_tracker: ReadProgress,
    ) -> None:
        self.source_name = source_name
        self.processed_records = processed_records
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
        progress_tracker: WriteProgress,
    ) -> None:
        self._progress_tracker: WriteProgress = progress_tracker

    @classmethod
    def from_progress_tracker(cls, progress_tracker: WriteProgress) -> WriteResult:
        return cls(progress_tracker)
