# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""An emulated executor for running Airbyte sources and destinations in-process."""
# TODO: Delete this class if not needed.

from __future__ import annotations

from typing import IO, TYPE_CHECKING

from airbyte.executors.base import Executor


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte._message_iterators import AirbyteMessageIterator


class EmulatedSourceExecutor(Executor):
    """An emulated executor for running Airbyte sources and destinations in-process.

    This executor is intended for use in development and testing environments where the overhead of
    running a separate process is not desirable. It is not recommended for production use.
    """

    def execute(
        self, args: list[str], *, stdin: IO[str] | AirbyteMessageIterator | None = None
    ) -> Iterator[str]:
        raise NotImplementedError(
            "The emulated executor does not support calling execute() directly."
        )
