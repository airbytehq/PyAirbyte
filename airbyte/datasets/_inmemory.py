# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""In-memory dataset class."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from overrides import overrides

from airbyte.datasets import DatasetBase


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte_protocol.models import ConfiguredAirbyteStream


class InMemoryDataset(DatasetBase):
    """A dataset that is held in memory.

    This dataset is useful for testing and debugging purposes, but should not be used with any
    large datasets.
    """

    def __init__(
        self,
        records: list[dict[str, Any]],
        stream_metadata: ConfiguredAirbyteStream,
    ) -> None:
        """Initialize the dataset with a list of records."""
        # Should already be a list, but we convert it to a list just in case an iterator is passed.
        self._records: list[dict[str, Any]] = list(records)
        super().__init__(
            stream_metadata=stream_metadata,
        )

    @overrides
    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Return the iterator of records."""
        return iter(self._records)

    def __len__(self) -> int:
        """Return the number of records in the dataset."""
        return len(self._records)
