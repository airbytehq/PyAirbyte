# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Abstract base class for Processors, including SQL and File writers.

Processors can take input from STDIN or a stream of Airbyte messages.

Caches will pass their input to the File Writer. They share a common base class so certain
abstractions like "write" and "finalize" can be handled in either layer, or both.
"""

from __future__ import annotations

import abc
import contextlib
import io
import sys
from collections import defaultdict
from typing import TYPE_CHECKING, Any, cast, final

import pyarrow as pa
import ulid

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
from airbyte._batch_handles import BatchHandle
from airbyte.caches.base import CacheBase
from airbyte.progress import progress
from airbyte.strategies import WriteStrategy
from airbyte.types import _get_pyarrow_type


if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator

    from airbyte.caches._catalog_manager import CatalogManager


DEFAULT_BATCH_SIZE = 10_000
DEBUG_MODE = False  # Set to True to enable additional debug logging.


class AirbyteMessageParsingError(Exception):
    """Raised when an Airbyte message is invalid or cannot be parsed."""


class RecordProcessor(abc.ABC):
    """Abstract base class for classes which can process input records."""

    MAX_BATCH_SIZE: int = DEFAULT_BATCH_SIZE

    skip_finalize_step: bool = False

    def __init__(
        self,
        cache: CacheBase,
        *,
        catalog_manager: CatalogManager | None = None,
    ) -> None:
        self._expected_streams: set[str] | None = None
        self.cache: CacheBase = cache
        if not isinstance(self.cache, CacheBase):
            raise exc.AirbyteLibInputError(
                message=(
                    f"Expected config class of type 'CacheBase'.  "
                    f"Instead received type '{type(self.cache).__name__}'."
                ),
            )

        self.source_catalog: ConfiguredAirbyteCatalog | None = None
        self._source_name: str | None = None

        self._active_batches: dict[str, BatchHandle] = {}
        self._pending_batches: dict[str, list[BatchHandle]] = defaultdict(list, {})
        self._finalized_batches: dict[str, list[BatchHandle]] = defaultdict(list, {})

        self._pending_state_messages: dict[str, list[AirbyteStateMessage]] = defaultdict(list, {})
        self._finalized_state_messages: dict[
            str,
            list[AirbyteStateMessage],
        ] = defaultdict(list, {})

        self._catalog_manager: CatalogManager | None = catalog_manager
        self._setup()

    @property
    def expected_streams(self) -> set[str]:
        """Return the expected stream names."""
        return self._expected_streams or set()

    def register_source(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        stream_names: set[str],
    ) -> None:
        """Register the source name and catalog."""
        if not self._catalog_manager:
            raise exc.AirbyteLibInternalError(
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

    def _process_record_message(
        self,
        record_msg: AirbyteRecordMessage,
    ) -> None:
        """Write a record to the cache.

        This method is called for each record message, before the batch is written.

        By default this is a no-op but file writers can override this method to write the record to
        files.
        """
        _ = record_msg  # Unused
        pass

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

        # Process messages, writing to batches as we go
        for message in messages:
            if message.type is Type.RECORD:
                record_msg = cast(AirbyteRecordMessage, message.record)
                self._process_record_message(record_msg)

            elif message.type is Type.STATE:
                state_msg = cast(AirbyteStateMessage, message.state)
                if state_msg.type in [AirbyteStateType.GLOBAL, AirbyteStateType.LEGACY]:
                    self._pending_state_messages[f"_{state_msg.type}"].append(state_msg)
                else:
                    stream_state = cast(AirbyteStreamState, state_msg.stream)
                    stream_name = stream_state.stream_descriptor.name
                    self._pending_state_messages[stream_name].append(state_msg)

            else:
                # Ignore unexpected or unhandled message types:
                # Type.LOG, Type.TRACE, Type.CONTROL, etc.
                pass

        # We are at the end of the stream. Process whatever else is queued.
        self._flush_active_batches()

        all_streams = list(set(self._pending_batches.keys()) | set(self._finalized_batches.keys()))
        # Add empty streams to the streams list, so we create a destination table for it
        for stream_name in self.expected_streams:
            if stream_name not in all_streams:
                if DEBUG_MODE:
                    print(f"Stream {stream_name} has no data")
                all_streams.append(stream_name)

        # Finalize any pending batches
        for stream_name in all_streams:
            self._finalize_batches(stream_name, write_strategy=write_strategy)
            progress.log_stream_finalized(stream_name)

    def _flush_active_batches(
        self,
    ) -> None:
        """Flush active batches for all streams."""
        for stream_name in self._active_batches:
            self._flush_active_batch(stream_name)

    def _flush_active_batch(
        self,
        stream_name: str,
    ) -> None:
        """Flush the active batch for the given stream.

        This entails moving the active batch to the pending batches, closing any open files, and
        logging the batch as written.
        """
        raise NotImplementedError(
            "Subclasses must implement the _flush_active_batch() method.",
        )

    def _cleanup_batch(  # noqa: B027  # Intentionally empty, not abstract
        self,
        batch_handle: BatchHandle,
    ) -> None:
        """Clean up the cache.

        This method is called after the given batch has been finalized.

        For instance, file writers can override this method to delete the files created. Caches,
        similarly, can override this method to delete any other temporary artifacts.
        """
        pass

    def _new_batch_id(self) -> str:
        """Return a new batch handle."""
        return str(ulid.ULID())

    def _new_batch(
        self,
        stream_name: str,
    ) -> BatchHandle:
        """Create and return a new batch handle.

        By default this is a concatenation of the stream name and batch ID.
        However, any Python object can be returned, such as a Path object.
        """
        batch_id = self._new_batch_id()
        return BatchHandle(stream_name=stream_name, batch_id=batch_id)

    def _finalize_batches(
        self,
        stream_name: str,
        write_strategy: WriteStrategy,
    ) -> list[BatchHandle]:
        """Finalize all uncommitted batches.

        Returns a mapping of batch IDs to batch handles, for processed batches.

        This is a generic implementation, which can be overridden.
        """
        _ = write_strategy  # Unused
        with self._finalizing_batches(stream_name) as batches_to_finalize:
            if batches_to_finalize and not self.skip_finalize_step:
                raise NotImplementedError(
                    "Caches need to be finalized but no _finalize_batch() method "
                    f"exists for class {self.__class__.__name__}",
                )

            return batches_to_finalize

    @abc.abstractmethod
    def _finalize_state_messages(
        self,
        stream_name: str,
        state_messages: list[AirbyteStateMessage],
    ) -> None:
        """Handle state messages.
        Might be a no-op if the processor doesn't handle incremental state."""
        pass

    @final
    @contextlib.contextmanager
    def _finalizing_batches(
        self,
        stream_name: str,
    ) -> Generator[list[BatchHandle], str, None]:
        """Context manager to use for finalizing batches, if applicable.

        Returns a mapping of batch IDs to batch handles, for those processed batches.
        """
        batches_to_finalize: list[BatchHandle] = self._pending_batches[stream_name].copy()
        state_messages_to_finalize: list[AirbyteStateMessage] = self._pending_state_messages[
            stream_name
        ].copy()
        self._pending_batches[stream_name].clear()
        self._pending_state_messages[stream_name].clear()

        progress.log_batches_finalizing(stream_name, len(batches_to_finalize))
        yield batches_to_finalize
        self._finalize_state_messages(stream_name, state_messages_to_finalize)
        progress.log_batches_finalized(stream_name, len(batches_to_finalize))

        self._finalized_batches[stream_name] += batches_to_finalize
        self._finalized_state_messages[stream_name] += state_messages_to_finalize

        for batch_handle in batches_to_finalize:
            self._cleanup_batch(batch_handle)

    def _setup(self) -> None:  # noqa: B027  # Intentionally empty, not abstract
        """Create the database.

        By default this is a no-op but subclasses can override this method to prepare
        any necessary resources.
        """
        pass

    def _teardown(self) -> None:
        """Teardown the processor resources.

        By default, the base implementation simply calls _cleanup_batch() for all pending batches.
        """
        batch_lists: list[list[BatchHandle]] = list(self._pending_batches.values()) + list(
            self._finalized_batches.values()
        )

        # TODO: flatten lists and remove nested 'for'
        for batch_list in batch_lists:
            for batch_handle in batch_list:
                self._cleanup_batch(
                    batch_handle=batch_handle,
                )

    @final
    def __del__(self) -> None:
        """Teardown temporary resources when instance is unloaded from memory."""
        self._teardown()

    @final
    def _get_stream_config(
        self,
        stream_name: str,
    ) -> ConfiguredAirbyteStream:
        """Return the column definitions for the given stream."""
        if not self._catalog_manager:
            raise exc.AirbyteLibInternalError(
                message="Catalog manager should exist but does not.",
            )

        return self._catalog_manager.get_stream_config(stream_name)

    @final
    def _get_stream_json_schema(
        self,
        stream_name: str,
    ) -> dict[str, Any]:
        """Return the column definitions for the given stream."""
        return self._get_stream_config(stream_name).stream.json_schema

    def _get_stream_pyarrow_schema(
        self,
        stream_name: str,
    ) -> pa.Schema:
        """Return the column definitions for the given stream."""
        return pa.schema(
            fields=[
                pa.field(prop_name, _get_pyarrow_type(prop_def))
                for prop_name, prop_def in self._get_stream_json_schema(stream_name)[
                    "properties"
                ].items()
            ]
        )
