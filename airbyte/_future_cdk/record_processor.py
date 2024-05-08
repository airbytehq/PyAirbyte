# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Abstract base class for Processors, including SQL and File writers.

Processors can take input from STDIN or a stream of Airbyte messages.

Caches will pass their input to the File Writer. They share a common base class so certain
abstractions like "write" and "finalize" can be handled in either layer, or both.
"""

from __future__ import annotations

import abc
import io
import sys
from collections import defaultdict
from typing import TYPE_CHECKING, Any, cast, final

from airbyte_protocol.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    Type,
)

from airbyte import exceptions as exc
from airbyte.caches.base import CacheBase
from airbyte.strategies import WriteStrategy


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from airbyte._batch_handles import BatchHandle
    from airbyte._catalog_manager import CatalogManagerBase
    from airbyte._state_manager import StateManagerBase


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
        cache: CacheBase,
        *,
        catalog_manager: CatalogManagerBase | None = None,
        state_manager: StateManagerBase | None = None,
    ) -> None:
        self._expected_streams: set[str] | None = None
        self.cache: CacheBase = cache
        if not isinstance(self.cache, CacheBase):
            raise exc.PyAirbyteInputError(
                message=(
                    f"Expected config class of type 'CacheBase'.  "
                    f"Instead received type '{type(self.cache).__name__}'."
                ),
            )

        self.configured_catalog: ConfiguredAirbyteCatalog | None = None
        self._source_name: str | None = None

        self._pending_state_messages: dict[str, list[AirbyteStateMessage]] = defaultdict(list, {})
        self._finalized_state_messages: dict[
            str,
            list[AirbyteStateMessage],
        ] = defaultdict(list, {})

        self._catalog_manager: CatalogManagerBase | None = catalog_manager
        self._state_manager: StateManagerBase | None = state_manager
        self._setup()

    @property
    def expected_streams(self) -> set[str]:
        """Return the expected stream names."""
        return self._expected_streams or set()

    @property
    def catalog_manager(
        self,
    ) -> CatalogManagerBase:
        """Return the catalog manager.

        Subclasses should set this property to a valid catalog manager instance if one
        is not explicitly passed to the constructor.

        Raises:
            PyAirbyteInternalError: If the catalog manager is not set.
        """
        if not self._catalog_manager:
            raise exc.PyAirbyteInternalError(
                message="Catalog manager should exist but does not.",
            )

        return self._catalog_manager

    @property
    def state_manager(
        self,
    ) -> StateManagerBase:
        """Return the state manager.

        Subclasses should set this property to a valid state manager instance if one
        is not explicitly passed to the constructor.

        Raises:
            PyAirbyteInternalError: If the state manager is not set.
        """
        if not self._state_manager:
            raise exc.PyAirbyteInternalError(
                message="State manager should exist but does not.",
            )

        return self._state_manager

    def register_source(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        stream_names: set[str],
    ) -> None:
        """Register the source name and catalog."""
        if not self._catalog_manager:
            raise exc.PyAirbyteInternalError(
                message="Catalog manager should exist but does not.",
            )
        self._catalog_manager.register_source(
            source_name,
            incoming_source_catalog=incoming_source_catalog,
            incoming_stream_names=stream_names,
        )
        self._expected_streams = stream_names

    @final
    def process_stdin(
        self,
        write_strategy: WriteStrategy = WriteStrategy.AUTO,
    ) -> None:
        """Process the input stream from stdin.

        Return a list of summaries for testing.
        """
        input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
        self.process_input_stream(input_stream, write_strategy=write_strategy)

    @final
    def _airbyte_messages_from_buffer(
        self,
        buffer: io.TextIOBase,
    ) -> Iterator[AirbyteMessage]:
        """Yield messages from a buffer."""
        yield from (AirbyteMessage.parse_raw(line) for line in buffer)

    @final
    def process_input_stream(
        self,
        input_stream: io.TextIOBase,
        write_strategy: WriteStrategy = WriteStrategy.AUTO,
    ) -> None:
        """Parse the input stream and process data in batches.

        Return a list of summaries for testing.
        """
        messages = self._airbyte_messages_from_buffer(input_stream)
        self.process_airbyte_messages(
            messages,
            write_strategy=write_strategy,
        )

    @abc.abstractmethod
    def process_record_message(
        self,
        record_msg: AirbyteRecordMessage,
        stream_schema: dict,
    ) -> None:
        """Write a record to the cache.

        This method is called for each record message.

        In most cases, the SQL processor will not perform any action, but will pass this along to to
        the file processor.
        """

    @final
    def process_airbyte_messages(
        self,
        messages: Iterable[AirbyteMessage],
        write_strategy: WriteStrategy,
    ) -> None:
        """Process a stream of Airbyte messages."""
        if not isinstance(write_strategy, WriteStrategy):
            raise exc.AirbyteInternalError(
                message="Invalid `write_strategy` argument. Expected instance of WriteStrategy.",
                context={"write_strategy": write_strategy},
            )

        stream_schemas: dict[str, dict] = {}

        # Process messages, writing to batches as we go
        for message in messages:
            if message.type is Type.RECORD:
                record_msg = cast(AirbyteRecordMessage, message.record)
                stream_name = record_msg.stream

                if stream_name not in stream_schemas:
                    stream_schemas[stream_name] = self.catalog_manager.get_stream_json_schema(
                        stream_name=stream_name
                    )

                self.process_record_message(
                    record_msg,
                    stream_schema=stream_schemas[stream_name],
                )

            elif message.type is Type.STATE:
                state_msg = cast(AirbyteStateMessage, message.state)
                if state_msg.type in {AirbyteStateType.GLOBAL, AirbyteStateType.LEGACY}:
                    self._pending_state_messages[f"_{state_msg.type}"].append(state_msg)
                else:
                    stream_state = cast(AirbyteStreamState, state_msg.stream)
                    stream_name = stream_state.stream_descriptor.name
                    self._pending_state_messages[stream_name].append(state_msg)

            else:
                # Ignore unexpected or unhandled message types:
                # Type.LOG, Type.TRACE, Type.CONTROL, etc.
                pass

        self.write_all_stream_data(
            write_strategy=write_strategy,
        )

        # Clean up files, if requested.
        if self.cache.cleanup:
            self.cleanup_all()

    def write_all_stream_data(self, write_strategy: WriteStrategy) -> None:
        """Finalize any pending writes."""
        for stream_name in self.expected_streams:
            self.write_stream_data(stream_name, write_strategy=write_strategy)

    @abc.abstractmethod
    def write_stream_data(
        self,
        stream_name: str,
        write_strategy: WriteStrategy,
    ) -> list[BatchHandle]:
        """Write pending stream data to the cache."""
        ...

    def _finalize_state_messages(
        self,
        stream_name: str,
        state_messages: list[AirbyteStateMessage],
    ) -> None:
        """Handle state messages by passing them to the catalog manager."""
        if not self._state_manager:
            raise exc.PyAirbyteInternalError(
                message="Catalog manager should exist but does not.",
            )
        if state_messages and self._source_name:
            self._state_manager.save_state(
                source_name=self._source_name,
                stream_name=stream_name,
                state=state_messages[-1],
            )

    def _setup(self) -> None:  # noqa: B027  # Intentionally empty, not abstract
        """Create the database.

        By default this is a no-op but subclasses can override this method to prepare
        any necessary resources.
        """
        pass

    @final
    def _get_configured_stream_info(
        self,
        stream_name: str,
    ) -> ConfiguredAirbyteStream:
        """Return the definition of the given stream.

        This method is a convenience wrapper around the catalog manager implementation.
        """
        if not self._catalog_manager:
            raise exc.PyAirbyteInternalError(
                message="Catalog manager should exist but does not.",
            )

        return self._catalog_manager.get_configured_stream_info(stream_name)

    @final
    def get_stream_json_schema(
        self,
        stream_name: str,
    ) -> dict[str, Any]:
        """Return the column definitions for the given stream.

        This method is a convenience wrapper around the catalog manager implementation.
        """
        if not self._catalog_manager:
            raise exc.PyAirbyteInternalError(
                message="Catalog manager should exist but does not.",
            )

        return self._catalog_manager.get_stream_json_schema(stream_name)

    def cleanup_all(self) -> None:  # noqa: B027  # Intentionally empty, not abstract
        """Clean up all resources.

        The default implementation is a no-op.
        """
        pass
