# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from overrides import overrides

from airbyte.datasets import DatasetBase
from airbyte.datasets._inmemory import InMemoryDataset


if TYPE_CHECKING:
    import threading
    from collections.abc import Iterator, Mapping

    from airbyte_protocol.models import ConfiguredAirbyteStream

    from airbyte import progress


class LazyDataset(DatasetBase):
    """A dataset that is loaded incrementally from a source or a SQL query."""

    def __init__(
        self,
        iterator: Iterator[dict[str, Any]],
        *,
        stream_metadata: ConfiguredAirbyteStream,
        stop_event: threading.Event | None,
        progress_tracker: progress.ProgressTracker,
    ) -> None:
        self._stop_event: threading.Event | None = stop_event or None
        self._progress_tracker = progress_tracker
        self._iterator: Iterator[dict[str, Any]] = iterator
        super().__init__(
            stream_metadata=stream_metadata,
        )

    @overrides
    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self._iterator

    def __next__(self) -> Mapping[str, Any]:
        try:
            return next(self._iterator)
        except StopIteration:
            # The iterator is exhausted, tell the producer they can stop if they are still
            # producing records. (Esp. when an artificial limit is reached.)
            self._progress_tracker.log_success()
            if self._stop_event:
                self._stop_event.set()
            raise

    def fetch_all(self) -> InMemoryDataset:
        """Fetch all records to memory and return an InMemoryDataset."""
        return InMemoryDataset(
            records=list(self._iterator),
            stream_metadata=self._stream_metadata,
        )

    def close(self) -> None:
        """Stop the dataset iterator.

        This method is used to signal the dataset to stop fetching records, for example
        when the dataset is being fetched incrementally and the user wants to stop the
        fetching process.
        """
        if self._stop_event:
            self._stop_event.set()

    def __del__(self) -> None:
        """Close the dataset when the object is deleted."""
        self.close()
