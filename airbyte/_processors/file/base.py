# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Abstract base class for File Writers, which write and read from file storage."""

from __future__ import annotations

import abc
from collections import defaultdict
from pathlib import Path
from typing import IO, TYPE_CHECKING, final

import ulid

from airbyte import exceptions as exc
from airbyte._batch_handles import BatchHandle
from airbyte._util.name_normalizers import LowerCaseNormalizer
from airbyte.progress import progress
from airbyte.records import StreamRecord


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        AirbyteRecordMessage,
    )


DEFAULT_BATCH_SIZE = 10000


class FileWriterBase(abc.ABC):
    """A generic base implementation for a file-based cache."""

    default_cache_file_suffix: str = ".batch"
    prune_extra_fields: bool = False

    MAX_BATCH_SIZE: int = DEFAULT_BATCH_SIZE

    def __init__(
        self,
        cache_dir: Path,
        *,
        cleanup: bool = True,
    ) -> None:
        """Initialize the file writer."""
        self._cache_dir = cache_dir
        self._do_cleanup = cleanup
        self._active_batches: dict[str, BatchHandle] = {}
        self._completed_batches: dict[str, list[BatchHandle]] = defaultdict(list, {})

    def _get_new_cache_file_path(
        self,
        stream_name: str,
        batch_id: str | None = None,  # ULID of the batch
    ) -> Path:
        """Return a new cache file path for the given stream."""
        batch_id = batch_id or str(ulid.ULID())
        target_dir = Path(self._cache_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / f"{stream_name}_{batch_id}{self.default_cache_file_suffix}"

    def _open_new_file(
        self,
        file_path: Path,
    ) -> IO[bytes]:
        """Open a new file for writing."""
        return file_path.open("wb")

    def _flush_active_batch(
        self,
        stream_name: str,
    ) -> None:
        """Flush the active batch for the given stream.

        This entails moving the active batch to the pending batches, closing any open files, and
        logging the batch as written.
        """
        if stream_name not in self._active_batches:
            return

        batch_handle: BatchHandle = self._active_batches[stream_name]
        batch_handle.close_files()
        del self._active_batches[stream_name]

        self._completed_batches[stream_name].append(batch_handle)
        progress.log_batch_written(
            stream_name=stream_name,
            batch_size=batch_handle.record_count,
        )

    def _new_batch(
        self,
        stream_name: str,
    ) -> BatchHandle:
        """Create and return a new batch handle.

        The base implementation creates and opens a new file for writing so it is ready to receive
        records.

        This also flushes the active batch if one already exists for the given stream.
        """
        if stream_name in self._active_batches:
            self._flush_active_batch(stream_name)

        batch_id = self._new_batch_id()
        new_file_path = self._get_new_cache_file_path(stream_name)

        batch_handle = BatchHandle(
            stream_name=stream_name,
            batch_id=batch_id,
            files=[new_file_path],
            file_opener=self._open_new_file,
        )
        self._active_batches[stream_name] = batch_handle
        return batch_handle

    def _close_batch(
        self,
        batch_handle: BatchHandle,
    ) -> None:
        """Close the current batch."""
        if not batch_handle.open_file_writer:
            return

        batch_handle.close_files()

    @final
    def cleanup_all(self) -> None:
        """Clean up the cache.

        For file writers, this means deleting the files created and declared in the batch.

        This method is final because it should not be overridden.

        Subclasses should override `_cleanup_batch` instead.
        """
        for batch_handle in self._active_batches.values():
            self._cleanup_batch(batch_handle)

        for batch_list in self._completed_batches.values():
            for batch_handle in batch_list:
                self._cleanup_batch(batch_handle)

    def process_record_message(
        self,
        record_msg: AirbyteRecordMessage,
        stream_schema: dict,
    ) -> None:
        """Write a record to the cache.

        This method is called for each record message, before the batch is written.
        """
        stream_name = record_msg.stream

        batch_handle: BatchHandle
        if stream_name not in self._active_batches:
            batch_handle = self._new_batch(stream_name=stream_name)

        else:
            batch_handle = self._active_batches[stream_name]

        if batch_handle.record_count + 1 > self.MAX_BATCH_SIZE:
            # Already at max batch size, so start a new batch.
            batch_handle = self._new_batch(stream_name=stream_name)

        if batch_handle.open_file_writer is None:
            raise exc.PyAirbyteInternalError(message="Expected open file writer.")

        self._write_record_dict(
            record_dict=StreamRecord.from_record_message(
                record_message=record_msg,
                expected_keys=stream_schema["properties"].keys(),
                normalizer=LowerCaseNormalizer,
                prune_extra_fields=self.prune_extra_fields,
            ),
            open_file_writer=batch_handle.open_file_writer,
        )
        batch_handle.increment_record_count()

    def flush_active_batches(
        self,
    ) -> None:
        """Flush active batches for all streams."""
        streams = list(self._active_batches.keys())
        for stream_name in streams:
            self._flush_active_batch(stream_name)

    def _cleanup_batch(
        self,
        batch_handle: BatchHandle,
    ) -> None:
        """Clean up the cache.

        For file writers, this means deleting the files created and declared in the batch.

        This method is a no-op if the `cleanup` config option is set to False.
        """
        self._close_batch(batch_handle)

        if self._do_cleanup:
            batch_handle.delete_files()

    def _new_batch_id(self) -> str:
        """Return a new batch handle."""
        return str(ulid.ULID())

    # Destructor

    @final
    def __del__(self) -> None:
        """Teardown temporary resources when instance is unloaded from memory."""
        if self._do_cleanup:
            self.cleanup_all()

    # Abstract methods

    @abc.abstractmethod
    def _write_record_dict(
        self,
        record_dict: StreamRecord,
        open_file_writer: IO[bytes],
    ) -> None:
        """Write one record to a file."""
        raise NotImplementedError("No default implementation.")

    # Public methods (for use by Cache and SQL Processor classes)

    def get_active_batch(self, stream_name: str) -> BatchHandle | None:
        """Return the active batch for a specific stream name."""
        return self._active_batches.get(stream_name, None)

    def get_pending_batches(self, stream_name: str) -> list[BatchHandle]:
        """Return the pending batches for a specific stream name."""
        return [
            batch for batch in self._completed_batches.get(stream_name, []) if not batch.finalized
        ]

    def get_finalized_batches(self, stream_name: str) -> list[BatchHandle]:
        """Return the finalized batches for a specific stream name."""
        return [batch for batch in self._completed_batches.get(stream_name, []) if batch.finalized]
