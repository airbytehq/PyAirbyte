# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Abstract base class for Processors, including SQL processors.

Processors accept Airbyte messages as input from STDIN or from another input stream.
"""

from __future__ import annotations

import abc
import io
import sys
from collections import defaultdict
from typing import IO, TYPE_CHECKING, cast, final

from airbyte_cdk import AirbyteMessage
from airbyte_protocol.models import (
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    AirbyteTraceMessage,
    Type,
)

from airbyte import exceptions as exc
from airbyte._future_cdk.state_writers import StdOutStateWriter
from airbyte._message_iterators import AirbyteMessageIterator
from airbyte.records import StreamRecordHandler
from airbyte.strategies import WriteStrategy


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from airbyte._batch_handles import BatchHandle
    from airbyte._future_cdk.catalog_providers import CatalogProvider
    from airbyte._future_cdk.state_writers import StateWriterBase
    from airbyte.progress import ProgressTracker


class AirbyteMessageParsingError(Exception):
    """Raised when an Airbyte message is invalid or cannot be parsed."""


class RecordProcessorBase(abc.ABC):
    """Abstract base class for classes which can process Airbyte messages from a source.

    This class is responsible for all aspects of handling Airbyte protocol.

    The class should be passed a catalog manager and stream manager class to handle the
    catalog and state aspects of the protocol.
    """

    def __init__(
        self,
        *,
        catalog_provider: CatalogProvider,
        state_writer: StateWriterBase | None = None,
    ) -> None:
        """Initialize the processor.

        If a state writer is not provided, the processor will use the default (STDOUT) state writer.
        """
        self._catalog_provider: CatalogProvider | None = catalog_provider
        self._state_writer: StateWriterBase | None = state_writer or StdOutStateWriter()

        self._pending_state_messages: dict[str, list[AirbyteStateMessage]] = defaultdict(list, {})
        self._finalized_state_messages: dict[
            str,
            list[AirbyteStateMessage],
        ] = defaultdict(list, {})

        self._setup()

    @property
    def expected_streams(self) -> set[str]:
        """Return the expected stream names."""
        return set(self.catalog_provider.stream_names)

    @property
    def catalog_provider(
        self,
    ) -> CatalogProvider:
        """Return the catalog manager.

        Subclasses should set this property to a valid catalog manager instance if one
        is not explicitly passed to the constructor.

        Raises:
            PyAirbyteInternalError: If the catalog manager is not set.
        """
        if not self._catalog_provider:
            raise exc.PyAirbyteInternalError(
                message="Catalog manager should exist but does not.",
            )

        return self._catalog_provider

    @property
    def state_writer(
        self,
    ) -> StateWriterBase:
        """Return the state writer instance.

        Subclasses should set this property to a valid state manager instance if one
        is not explicitly passed to the constructor.

        Raises:
            PyAirbyteInternalError: If the state manager is not set.
        """
        if not self._state_writer:
            raise exc.PyAirbyteInternalError(
                message="State manager should exist but does not.",
            )

        return self._state_writer

    @final
    def process_stdin(
        self,
        *,
        write_strategy: WriteStrategy = WriteStrategy.AUTO,
        progress_tracker: ProgressTracker,
    ) -> None:
        """Process the input stream from stdin.

        Return a list of summaries for testing.
        """
        input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
        self.process_input_stream(
            input_stream,
            write_strategy=write_strategy,
            progress_tracker=progress_tracker,
        )

    @final
    def _airbyte_messages_from_buffer(
        self,
        buffer: io.TextIOBase,
    ) -> Iterator[AirbyteMessage]:
        """Yield messages from a buffer."""
        yield from (AirbyteMessage.model_validate_json(line) for line in buffer)

    @final
    def process_input_stream(
        self,
        input_stream: IO[str],
        *,
        write_strategy: WriteStrategy = WriteStrategy.AUTO,
        progress_tracker: ProgressTracker,
    ) -> None:
        """Parse the input stream and process data in batches.

        Return a list of summaries for testing.
        """
        messages = AirbyteMessageIterator.from_str_buffer(input_stream)
        self.process_airbyte_messages(
            messages,
            write_strategy=write_strategy,
            progress_tracker=progress_tracker,
        )

    @abc.abstractmethod
    def process_record_message(
        self,
        record_msg: AirbyteRecordMessage,
        stream_record_handler: StreamRecordHandler,
        progress_tracker: ProgressTracker,
    ) -> None:
        """Write a record.

        This method is called for each record message.

        In most cases, the SQL processor will not perform any action, but will pass this along to to
        the file processor.
        """

    @final
    def process_airbyte_messages(
        self,
        messages: Iterable[AirbyteMessage],
        *,
        write_strategy: WriteStrategy,
        progress_tracker: ProgressTracker,
    ) -> None:
        """Process a stream of Airbyte messages."""
        if not isinstance(write_strategy, WriteStrategy):
            raise exc.AirbyteInternalError(
                message="Invalid `write_strategy` argument. Expected instance of WriteStrategy.",
                context={"write_strategy": write_strategy},
            )

        stream_record_handlers: dict[str, StreamRecordHandler] = {}

        # Process messages, writing to batches as we go
        for message in messages:
            if message.type is Type.RECORD:
                record_msg = cast(AirbyteRecordMessage, message.record)
                stream_name = record_msg.stream

                if stream_name not in stream_record_handlers:
                    stream_record_handlers[stream_name] = StreamRecordHandler(
                        json_schema=self.catalog_provider.get_stream_json_schema(
                            stream_name=stream_name,
                        ),
                        normalize_keys=True,
                        prune_extra_fields=True,
                    )

                self.process_record_message(
                    record_msg,
                    stream_record_handler=stream_record_handlers[stream_name],
                    progress_tracker=progress_tracker,
                )

            elif message.type is Type.STATE:
                state_msg = cast(AirbyteStateMessage, message.state)
                if state_msg.type in {AirbyteStateType.GLOBAL, AirbyteStateType.LEGACY}:
                    self._pending_state_messages[f"_{state_msg.type}"].append(state_msg)
                else:
                    stream_state = cast(AirbyteStreamState, state_msg.stream)
                    stream_name = stream_state.stream_descriptor.name
                    self._pending_state_messages[stream_name].append(state_msg)

            elif message.type is Type.TRACE:
                trace_msg: AirbyteTraceMessage = cast(AirbyteTraceMessage, message.trace)
                if trace_msg.stream_status and trace_msg.stream_status.status == "SUCCEEDED":
                    # This stream has completed successfully, so go ahead and write the data.
                    # This will also finalize any pending state messages.
                    self.write_stream_data(
                        stream_name=trace_msg.stream_status.stream_descriptor.name,
                        write_strategy=write_strategy,
                        progress_tracker=progress_tracker,
                    )

            else:
                # Ignore unexpected or unhandled message types:
                # Type.LOG, Type.CONTROL, etc.
                pass

        # We've finished processing input data.
        # Finalize all received records and state messages:
        self.write_all_stream_data(
            write_strategy=write_strategy,
            progress_tracker=progress_tracker,
        )

        self.cleanup_all()

    def write_all_stream_data(
        self,
        write_strategy: WriteStrategy,
        progress_tracker: ProgressTracker,
    ) -> None:
        """Finalize any pending writes."""
        for stream_name in self.catalog_provider.stream_names:
            self.write_stream_data(
                stream_name,
                write_strategy=write_strategy,
                progress_tracker=progress_tracker,
            )

    @abc.abstractmethod
    def write_stream_data(
        self,
        stream_name: str,
        write_strategy: WriteStrategy,
        progress_tracker: ProgressTracker,
    ) -> list[BatchHandle]:
        """Write pending stream data to the cache."""
        ...

    def _finalize_state_messages(
        self,
        state_messages: list[AirbyteStateMessage],
    ) -> None:
        """Handle state messages by passing them to the catalog manager."""
        if state_messages:
            self.state_writer.write_state(
                state_message=state_messages[-1],
            )

    def _setup(self) -> None:  # noqa: B027  # Intentionally empty, not abstract
        """Create the database.

        By default this is a no-op but subclasses can override this method to prepare
        any necessary resources.
        """
        pass

    def cleanup_all(self) -> None:  # noqa: B027  # Intentionally empty, not abstract
        """Clean up all resources.

        The default implementation is a no-op.
        """
        pass
