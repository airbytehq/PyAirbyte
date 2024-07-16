# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Message generator for Airbyte messages."""

from __future__ import annotations

import abc
import sys
from typing import IO, TYPE_CHECKING, Any, Callable

import pydantic
from typing_extensions import final

from airbyte_cdk import AirbyteMessage


if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator
    from pathlib import Path


class AirbyteMessageGenerator(abc.ABC):
    """Abstract base class for Airbyte message generators.

    This class behaves like Iterator[AirbyteMessage] but it can also be used
    as IO[str]. In the latter case, it will return the JSON string representation of
    the all messages in the generator.
    """

    @final
    def __iter__(self) -> Iterator[AirbyteMessage]:
        """The class itself is not a generator but this method makes it iterable."""
        return self._generator()

    @abc.abstractmethod
    # def _generator(self) -> Generator[AirbyteMessage, None, None]:
    def _generator(self) -> Generator[Any, Any, Any]:
        """Should be implemented by subclasses to yield AirbyteMessage objects."""
        yield  # Placeholder for actual implementation. This should be replaced with actual logic.

    @final
    def __next__(self) -> AirbyteMessage:
        """Delegate to the internal generator."""
        if not hasattr(self, "_internal_generator"):
            self._internal_generator = self._generator()
        return next(self._internal_generator)

    @final
    def read(self) -> str:
        """Read the next message from the generator."""
        return next(self).json()


class MessageGeneratorFromMessages(AirbyteMessageGenerator):
    """A message generator that reads messages from STDIN."""

    def __init__(self, messages: Iterable[AirbyteMessage]) -> None:
        self.messages: Iterable[AirbyteMessage] = messages

    def _generator(self) -> Generator[AirbyteMessage, None, None]:
        """Yields AirbyteMessage objects read from STDIN."""
        yield from self.messages


class MessageGeneratorFromStrBuffer(AirbyteMessageGenerator):
    """A message generator that reads messages from STDIN."""

    def __init__(self, buffer: IO[str]) -> None:
        self.buffer: IO[str] = buffer

    def _generator(self) -> Generator[AirbyteMessage, None, None]:
        """Yields AirbyteMessage objects read from STDIN."""
        while True:
            next_line: str | None = next(self.buffer, None)  # Read the next line from STDIN
            if next_line is None:
                # End of file (EOF) indicates no more input from STDIN
                break
            try:
                # Let Pydantic handle the JSON decoding from the raw string
                yield AirbyteMessage.model_validate_json(next_line)
            except pydantic.ValidationError:
                # Handle JSON decoding errors (optional)
                raise ValueError("Invalid JSON format")  # noqa: B904, TRY003


class MessageGeneratorFromStrIterable(AirbyteMessageGenerator):
    """A message generator that reads messages from STDIN."""

    def __init__(self, buffer: Iterable[str]) -> None:
        self.buffer: Iterable[str] = buffer

    def _generator(self) -> Generator[AirbyteMessage, None, None]:
        """Yields AirbyteMessage objects read from STDIN."""
        for line in self.buffer:
            try:
                # Let Pydantic handle the JSON decoding from the raw string
                yield AirbyteMessage.model_validate_json(line)
            except pydantic.ValidationError:
                # Handle JSON decoding errors (optional)
                raise ValueError(f"Invalid JSON format in input string: {line}")  # noqa: B904, TRY003


class StdinMessageGenerator(MessageGeneratorFromStrBuffer):
    """A message generator that reads messages from STDIN."""

    def __init__(self) -> None:
        super().__init__(buffer=sys.stdin)


class FileBasedMessageGenerator(AirbyteMessageGenerator):
    """A message generator that reads messages from a file iterator.

    File iterator should be already sorted so that records are generated in the
    correct order.
    """

    def __init__(
        self,
        file_iterator: Iterator[Path],
        file_opener: Callable[[Path], IO[str]],
    ) -> None:
        self._file_iterator: Iterator[Path] = file_iterator
        self._file_opener: Callable[[Path], IO[str]] = file_opener
        self._current_file: Path | None = None
        self._current_file_buffer: IO[str] | None = None
        self._generator_instance: Generator[AirbyteMessage, None, None] = self._generator()

    def _generator(self) -> Generator[AirbyteMessage, None, None]:
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
                    AirbyteMessage.model_validate_json(next_line),
                    self._current_file,
                )
            except pydantic.ValidationError:
                # Handle JSON decoding errors
                self._current_file_buffer.close()
                self._current_file_buffer = None
                raise ValueError("Invalid JSON format")  # noqa: B904, TRY003
