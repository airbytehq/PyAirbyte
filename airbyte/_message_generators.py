# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Message generator for Airbyte messages."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Callable

import pydantic

from airbyte_cdk import AirbyteMessage


if TYPE_CHECKING:
    import io
    from collections.abc import Generator, Iterator
    from pathlib import Path


class AirbyteMessageGenerator(abc.ABC):
    """Abstract base class for Airbyte message generators."""

    def __iter__(self) -> Iterator[AirbyteMessage]:
        return self

    @abc.abstractmethod
    def __next__(self) -> AirbyteMessage:
        """Should be implemented by subclasses to return the next message."""
        ...


class StdinMessageGenerator(AirbyteMessageGenerator):
    """A message generator that reads messages from STDIN."""

    def __iter__(self) -> Iterator[AirbyteMessage]:
        return self

    def __next__(self) -> AirbyteMessage:
        """Reads the next message from STDIN and returns it as an AirbyteMessage."""
        try:
            # Let Pydantic handle the JSON decoding from the raw string
            return AirbyteMessage.parse_raw(input())
        except EOFError:
            # End of file (EOF) indicates no more input from STDIN
            raise StopIteration from None
        except pydantic.ValidationError:
            # Handle JSON decoding errors (optional)
            raise ValueError("Invalid JSON format")  # noqa: B904, TRY003


class FileBasedMessageGenerator(AirbyteMessageGenerator):
    """A message generator that reads messages from a file iterator.

    File iterator should be already sorted so that records are generated in the
    correct order.
    """

    def __init__(
        self,
        file_iterator: Iterator[Path],
        file_opener: Callable[[Path], io.StringIO],
    ) -> None:
        self._file_iterator: Iterator[Path] = file_iterator
        self._file_opener: Callable[[Path], io.StringIO] = file_opener
        self._current_file: Path | None = None
        self._current_file_buffer: io.StringIO | None = None
        self._generator_instance: Generator[AirbyteMessage, None, None] = self._generator()

    def _generator(self) -> Generator[AirbyteMessage]:
        """Read the next line from the current file and return it as a tuple with the file path."""
        while True:
            if self._current_file_buffer is None:
                try:
                    self._current_file = next(self._file_iterator)
                    self._current_file_buffer = self._file_opener(self._current_file)
                except StopIteration:
                    # No more files to read; Exit the loop
                    break

            next_line: str = self._current_file_buffer.readline()
            if next_line == "":  # noqa: PLC1901  # EOF produces an empty string
                # Close the current file and open the next one
                self._current_file_buffer.close()
                self._current_file_buffer = None  # Ensure the buffer is reset
                continue  # Skip further processing and move to the next file

            try:
                # Let Pydantic handle the JSON decoding from the raw string
                yield (
                    AirbyteMessage.parse_raw(next_line),
                    self._current_file,
                )
            except pydantic.ValidationError:
                # Handle JSON decoding errors
                self._current_file_buffer.close()
                self._current_file_buffer = None
                raise ValueError("Invalid JSON format")  # noqa: B904, TRY003

    def __iter__(self) -> Iterator[AirbyteMessage]:
        return self._generator_instance

    def __next__(self) -> AirbyteMessage:
        """Reads the next message from the file iterator and return it as an AirbyteMessage."""
        return next(self._generator_instance)
