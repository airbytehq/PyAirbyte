# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Message generator for Airbyte messages."""

from __future__ import annotations

import datetime
import sys
from typing import IO, TYPE_CHECKING, Callable, cast

import pydantic
from typing_extensions import final

from airbyte_protocol.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    Type,
)

from airbyte.constants import AB_EXTRACTED_AT_COLUMN
from airbyte.progress import ProgressStyle, ReadProgress


if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator
    from pathlib import Path

    from airbyte._future_cdk.state_providers import StateProviderBase
    from airbyte.results import ReadResult
    from airbyte.sources.base import Source


class AirbyteMessageGenerator:
    """Abstract base class for Airbyte message generators.

    This class behaves like Iterator[AirbyteMessage] but it can also be used
    as IO[str]. In the latter case, it will return the JSON string representation of
    the all messages in the generator.
    """

    def __init__(
        self,
        generator: Iterator[AirbyteMessage],
    ) -> None:
        self._generator = generator

    @final
    def __iter__(self) -> Iterator[AirbyteMessage]:
        """The class itself is not a generator but this method makes it iterable."""
        return iter(self._generator)

    @final
    def __next__(self) -> AirbyteMessage:
        """Delegate to the internal generator."""
        return next(self._generator)

    @final
    def read(self) -> str:
        """Read the next message from the generator."""
        return next(self).model_dump_json()

    @classmethod
    def from_read_result(cls, read_result: ReadResult) -> AirbyteMessageGenerator:
        """Create a generator from a `ReadResult` object."""

        state_provider = read_result.cache.get_state_provider(
            source_name=read_result.source_name,
        )

        def generator() -> Generator[AirbyteMessage, None, None]:
            for stream_name, dataset in read_result.items():
                for record in dataset:
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(
                            stream=stream_name,
                            data=record,
                            emitted_at=int(
                                cast(
                                    datetime.datetime, record.get(AB_EXTRACTED_AT_COLUMN)
                                ).timestamp()
                            ),
                            # `meta` and `namespace` are not handled:
                            meta=None,
                            namespace=None,
                        ),
                    )

                # Send the latest state message from the source.
                yield AirbyteMessage(
                    type=Type.STATE,
                    state=state_provider.get_state_message_artifact(stream_name=stream_name),
                )

        return cls(generator())

    @classmethod
    def from_messages(cls, messages: Iterable[AirbyteMessage]) -> AirbyteMessageGenerator:
        """Create a generator from an iterable of messages."""
        return cls(iter(messages))

    @classmethod
    def from_str_buffer(cls, buffer: IO[str]) -> AirbyteMessageGenerator:
        """Create a generator that reads messages from a buffer."""

        def generator() -> Generator[AirbyteMessage, None, None]:
            """Yields AirbyteMessage objects read from STDIN."""
            while True:
                next_line: str | None = next(buffer, None)  # Read the next line from STDIN
                if next_line is None:
                    # End of file (EOF) indicates no more input from STDIN
                    break
                try:
                    # Let Pydantic handle the JSON decoding from the raw string
                    yield AirbyteMessage.model_validate_json(next_line)
                except pydantic.ValidationError:
                    # Handle JSON decoding errors (optional)
                    raise ValueError("Invalid JSON format")  # noqa: B904, TRY003

        return cls(generator())

    @classmethod
    def from_str_iterable(cls, buffer: Iterable[str]) -> AirbyteMessageGenerator:
        """Yields AirbyteMessage objects read from STDIN."""

        def generator() -> Generator[AirbyteMessage, None, None]:
            for line in buffer:
                try:
                    # Let Pydantic handle the JSON decoding from the raw string
                    yield AirbyteMessage.model_validate_json(line)
                except pydantic.ValidationError:
                    # Handle JSON decoding errors (optional)
                    raise ValueError(f"Invalid JSON format in input string: {line}")  # noqa: B904, TRY003

        return cls(generator())

    @classmethod
    def from_stdin(cls) -> AirbyteMessageGenerator:
        """Create a generator that reads messages from STDIN."""
        return cls.from_str_buffer(sys.stdin)

    @classmethod
    def from_files(
        cls, file_iterator: Iterator[Path], file_opener: Callable[[Path], IO[str]]
    ) -> AirbyteMessageGenerator:
        """Create a generator that reads messages from a file iterator."""

        def generator() -> Generator[AirbyteMessage, None, None]:
            current_file_buffer: IO[str] | None = None
            current_file: Path | None = None
            while True:
                if current_file_buffer is None:
                    try:
                        current_file = next(file_iterator)
                        current_file_buffer = file_opener(current_file)
                    except StopIteration:
                        # No more files to read; Exit the loop
                        break

                next_line: str = current_file_buffer.readline()
                if next_line == "":  # noqa: PLC1901  # EOF produces an empty string
                    # Close the current file and open the next one
                    current_file_buffer.close()
                    current_file_buffer = None  # Ensure the buffer is reset
                    continue  # Skip further processing and move to the next file

                try:
                    # Let Pydantic handle the JSON decoding from the raw string
                    yield (
                        AirbyteMessage.model_validate_json(next_line),
                        current_file,
                    )
                except pydantic.ValidationError:
                    # Handle JSON decoding errors
                    current_file_buffer.close()
                    current_file_buffer = None
                    raise ValueError("Invalid JSON format")  # noqa: B904, TRY003

        return cls(generator())

    @classmethod
    def from_source(
        cls,
        source: Source,
        *,
        streams: list[str] | None = None,
        progress_tracker: ReadProgress | None = None,
        force_full_refresh: bool = False,
        state_provider: StateProviderBase | None = None,
    ) -> AirbyteMessageGenerator:
        """Create a generator that reads messages from a source.

        Only the `source` parameter is required. The other parameters are optional.

        If a `progress_tracker` is not provided, a silent progress tracker will be used.
        """
        progress_tracker = progress_tracker or ReadProgress(ProgressStyle.NONE)
        return source._get_airbyte_message_generator(  # noqa: SLF001
            streams=streams,
            progress_tracker=progress_tracker,
            force_full_refresh=force_full_refresh,
            state_provider=state_provider,
        )
