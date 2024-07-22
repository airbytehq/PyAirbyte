# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A simple progress bar for the command line and IPython notebooks.

Note: Some runtimes (e.g. Dagger) may not support Rich Live views, and sometimes because they _also_
use Rich, and Rich only supports one live view at a time. PyAirbyte will try to use smart defaults
based on your execution environment.

If you experience issues, you can force plain text status reporting by setting the environment
variable `NO_LIVE_PROGRESS=1`.
"""

from __future__ import annotations

import datetime
import importlib
import math
import os
import sys
import time
import warnings
from collections import defaultdict
from contextlib import suppress
from enum import Enum, auto
from typing import IO, TYPE_CHECKING, Any, cast

from rich.errors import LiveError
from rich.live import Live as RichLive
from rich.markdown import Markdown as RichMarkdown

from airbyte_protocol.models import Type

from airbyte._util import meta


if TYPE_CHECKING:
    from collections.abc import Generator, Iterable
    from types import ModuleType

    from airbyte_protocol.models import AirbyteMessage


DEFAULT_REFRESHES_PER_SECOND = 2
IS_REPL = hasattr(sys, "ps1")  # True if we're in a Python REPL, in which case we can use Rich.

ipy_display: ModuleType | None
try:
    # Default to IS_NOTEBOOK=False if a TTY is detected.
    IS_NOTEBOOK = not sys.stdout.isatty()
    ipy_display = importlib.import_module("IPython.display")

except ImportError:
    # If IPython is not installed, then we're definitely not in a notebook.
    ipy_display = None
    IS_NOTEBOOK = False


class ProgressStyle(Enum):
    """An enum of progress bar styles."""

    AUTO = auto()
    """Automatically select the best style for the environment."""

    RICH = auto()
    """A Rich progress bar."""

    IPYTHON = auto()
    """Use IPython display methods."""

    PLAIN = auto()
    """A plain text progress print."""

    NONE = auto()
    """Skip progress prints."""


MAX_UPDATE_FREQUENCY = 5_000
"""The max number of records to read before updating the progress bar."""


def _to_time_str(timestamp: float) -> str:
    """Convert a timestamp float to a local time string.

    For now, we'll just use UTC to avoid breaking tests. In the future, we should
    return a local time string.
    """
    datetime_obj = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    # TODO: Uncomment this line when we can get tests to properly account for local timezones.
    #       For now, we'll just use UTC to avoid breaking tests.
    # datetime_obj = datetime_obj.astimezone()
    return datetime_obj.strftime("%H:%M:%S")


def _get_elapsed_time_str(seconds: float) -> str:
    """Return duration as a string.

    Seconds are included until 10 minutes is exceeded.
    Minutes are always included after 1 minute elapsed.
    Hours are always included after 1 hour elapsed.
    """
    if seconds <= 2:  # noqa: PLR2004  # Magic numbers OK here.
        # Less than 1 minute elapsed
        return f"{seconds:.2f} seconds"

    if seconds <= 60:  # noqa: PLR2004  # Magic numbers OK here.
        # Less than 1 minute elapsed
        return f"{seconds:.0f} seconds"

    if seconds < 60 * 10:
        # Less than 10 minutes elapsed
        minutes = seconds // 60
        seconds %= 60
        return f"{minutes:.0f}min {seconds:.0f}s"

    if seconds < 60 * 60:
        # Less than 1 hour elapsed
        minutes = seconds // 60
        seconds %= 60
        return f"{minutes}min"

    # Greater than 1 hour elapsed
    hours = seconds // (60 * 60)
    minutes = (seconds % (60 * 60)) // 60
    return f"{hours}hr {minutes}min"


class ProgressTracker:  # noqa: PLR0904  # Too many public methods
    """A simple progress bar for the command line and IPython notebooks."""

    def __init__(
        self,
        style: ProgressStyle = ProgressStyle.AUTO,
    ) -> None:
        """Initialize the progress tracker."""
        # Streams expected (for progress bar)
        self.num_streams_expected = 0

        # Reads
        self.read_start_time = time.time()
        self.read_end_time: float | None = None
        self.first_record_received_time: float | None = None
        self.total_records_read = 0

        # Stream reads
        self.stream_read_counts: dict[str, int] = defaultdict(int)
        self.stream_read_start_times: dict[str, float] = {}
        self.stream_read_end_times: dict[str, float] = {}

        # Cache Writes
        self.total_records_written = 0
        self.total_batches_written = 0
        self.written_stream_names: set[str] = set()

        # Cache Finalization
        self.finalize_start_time: float | None = None
        self.finalize_end_time: float | None = None
        self.total_records_finalized = 0
        self.total_batches_finalized = 0
        self.finalized_stream_names: set[str] = set()

        # Destination stream writes
        self.destination_stream_records_delivered: dict[str, int] = defaultdict(int)
        self.destination_stream_records_confirmed: dict[str, int] = defaultdict(int)

        # Progress bar properties
        self.last_update_time: float | None = None
        self._rich_view: RichLive | None = None

        self.reset_progress_style(style)

    def tally_records_read(
        self,
        messages: Iterable[AirbyteMessage],
    ) -> Generator[AirbyteMessage, Any, None]:
        """This method simply tallies the number of records processed and yields the messages."""
        # Update the display before we start.
        self.total_records_read = 0
        self._update_display()
        self._start_rich_view()

        update_period = 1  # Reset the update period to 1 before start.

        for count, message in enumerate(messages, start=1):
            # Yield the message immediately.
            yield message

            # Tally the record.
            self.total_records_read = count

            # Bail if we're not due for a progress update.
            if count % update_period != 0:
                continue

            # If this is the first record, set the start time.
            if self.first_record_received_time is None:
                self.first_record_received_time = time.time()

            # Update the update period to the latest scale of data.
            update_period = self._get_update_period(count)

            # Update the display.
            self._update_display()

    def log_read_complete(self) -> None:
        """Log that reading is complete."""
        self.read_end_time = time.time()
        self._update_display(force_refresh=True)

    def reset_progress_style(
        self,
        style: ProgressStyle = ProgressStyle.AUTO,
    ) -> None:
        """Set the progress bar style.

        You can call this method at any time to change the progress bar style as needed.

        Usage:

        ```python
        from airbyte.progress import progress, ProgressStyle

        progress.reset_progress_style(ProgressStyle.PLAIN)
        ```
        """
        self._stop_rich_view()  # Stop Rich's live view if running.
        self.style: ProgressStyle = style
        if self.style == ProgressStyle.AUTO:
            self.style = ProgressStyle.PLAIN
            if IS_NOTEBOOK:
                self.style = ProgressStyle.IPYTHON

            elif IS_REPL or "NO_LIVE_PROGRESS" in os.environ:
                self.style = ProgressStyle.PLAIN

            elif meta.is_ci():
                # Some CI environments support Rich, but Dagger does not.
                # TODO: Consider updating this to check for Dagger specifically.
                self.style = ProgressStyle.PLAIN

            else:
                # Test for Rich availability:
                self._rich_view = RichLive()
                try:
                    self._rich_view.start()
                    self._rich_view.stop()
                    self._rich_view = None
                except LiveError:
                    # Rich live view not available. Using plain text progress.
                    self._rich_view = None
                    self.style = ProgressStyle.PLAIN
                else:
                    # No exceptions raised, so we can use Rich.
                    self.style = ProgressStyle.RICH

    def _start_rich_view(self) -> None:
        """Start the rich view display, if applicable per `self.style`.

        Otherwise, this is a no-op.
        """
        if self.style == ProgressStyle.RICH and not self._rich_view:
            try:
                self._rich_view = RichLive(
                    auto_refresh=True,
                    refresh_per_second=DEFAULT_REFRESHES_PER_SECOND,
                )
                self._rich_view.start()
            except Exception:
                warnings.warn(
                    "Failed to start Rich live view. Falling back to plain text progress.",
                    stacklevel=2,
                )
                self.style = ProgressStyle.PLAIN
                self._stop_rich_view()

    def _stop_rich_view(self) -> None:
        """Stop the rich view display, if applicable.

        Otherwise, this is a no-op.
        """
        if self._rich_view:
            with suppress(Exception):
                self._rich_view.stop()
                self._rich_view = None

    def __del__(self) -> None:
        """Close the Rich view."""
        self._stop_rich_view()

    def log_success(self) -> None:
        """Log success and stop tracking progress."""
        if self.finalize_end_time is None:
            # If we haven't already finalized, do so now.

            self.finalize_end_time = time.time()

            self._update_display(force_refresh=True)
            self._stop_rich_view()

    @property
    def elapsed_seconds(self) -> float:
        """Return the number of seconds elapsed since the operation started."""
        if self.finalize_end_time:
            return self.finalize_end_time - self.read_start_time

        return time.time() - self.read_start_time

    @property
    def elapsed_read_seconds(self) -> float:
        """Return the number of seconds elapsed since the read operation started."""
        if self.finalize_start_time:
            return self.finalize_start_time - (
                self.first_record_received_time or self.read_start_time
            )

        return time.time() - (self.first_record_received_time or self.read_start_time)

    @property
    def elapsed_time_string(self) -> str:
        """Return duration as a string."""
        return _get_elapsed_time_str(self.elapsed_seconds)

    @property
    def elapsed_seconds_since_last_update(self) -> float | None:
        """Return the number of seconds elapsed since the last update."""
        if self.last_update_time is None:
            return None

        return time.time() - self.last_update_time

    @property
    def elapsed_read_time_string(self) -> str:
        """Return duration as a string."""
        return _get_elapsed_time_str(self.elapsed_read_seconds)

    @property
    def elapsed_finalization_seconds(self) -> float:
        """Return the number of seconds elapsed since the read operation started."""
        if self.finalize_start_time is None:
            return 0
        if self.finalize_end_time is None:
            return time.time() - self.finalize_start_time
        return self.finalize_end_time - self.finalize_start_time

    @property
    def elapsed_finalization_time_str(self) -> str:
        """Return duration as a string."""
        return _get_elapsed_time_str(self.elapsed_finalization_seconds)

    @staticmethod
    def _get_update_period(
        current_count: int,
    ) -> int:
        """Return the number of records to read before updating the progress bar.

        This is some math to make updates adaptive to the scale of records read.
        We want to update the display more often when the count is low, and less
        often when the count is high.
        """
        return min(MAX_UPDATE_FREQUENCY, 10 ** math.floor(math.log10(max(current_count, 1)) / 4))

    def log_batch_written(self, stream_name: str, batch_size: int) -> None:
        """Log that a batch has been written.

        Args:
            stream_name: The name of the stream.
            batch_size: The number of records in the batch.
        """
        self.total_records_written += batch_size
        self.total_batches_written += 1
        self.written_stream_names.add(stream_name)
        self._update_display()

    def log_batches_finalizing(self, stream_name: str, num_batches: int) -> None:
        """Log that batch are ready to be finalized.

        In our current implementation, we ignore the stream name and number of batches.
        We just use this as a signal that we're finished reading and have begun to
        finalize any accumulated batches.
        """
        _ = stream_name, num_batches  # unused for now
        if self.finalize_start_time is None:
            self.read_end_time = time.time()
            self.finalize_start_time = self.read_end_time

        self._update_display(force_refresh=True)

    def log_batches_finalized(self, stream_name: str, num_batches: int) -> None:
        """Log that a batch has been finalized."""
        _ = stream_name  # unused for now
        self.total_batches_finalized += num_batches
        self._update_display(force_refresh=True)

    def log_stream_finalized(self, stream_name: str) -> None:
        """Log that a stream has been finalized."""
        self.finalized_stream_names.add(stream_name)
        self._update_display(force_refresh=True)
        if len(self.finalized_stream_names) == self.num_streams_expected:
            self.log_success()

    def _update_display(self, *, force_refresh: bool = False) -> None:
        """Update the display."""
        # Don't update more than twice per second unless force_refresh is True.
        if (
            not force_refresh
            and self.last_update_time  # if not set, then we definitely need to update
            and cast(float, self.elapsed_seconds_since_last_update) < 0.8  # noqa: PLR2004
        ):
            return

        status_message = self._get_status_message()

        if self.style == ProgressStyle.IPYTHON and ipy_display is not None:
            # We're in a notebook so use the IPython display.
            assert ipy_display is not None
            ipy_display.clear_output(wait=True)
            ipy_display.display(ipy_display.Markdown(status_message))

        elif self.style == ProgressStyle.RICH and self._rich_view is not None:
            self._rich_view.update(RichMarkdown(status_message))

        elif self.style == ProgressStyle.PLAIN:
            # TODO: Add a plain text progress print option that isn't too noisy.
            pass

        elif self.style == ProgressStyle.NONE:
            pass

        self.last_update_time = time.time()

    def _get_status_message(self) -> str:
        """Compile and return a status message."""
        # Format start time as a friendly string in local timezone:
        start_time_str = _to_time_str(self.read_start_time)
        records_per_second: float = 0.0
        if self.elapsed_read_seconds > 0:
            records_per_second = self.total_records_read / self.elapsed_read_seconds

        status_message = (
            f"### Read Progress\n\n"
            f"**Started reading from source at `{start_time_str}`:**\n\n"
            f"- Read **{self.total_records_read:,}** records "
            f"over **{self.elapsed_read_time_string}** "
            f"({records_per_second:,.1f} records / second).\n\n"
        )
        if self.total_records_written > 0:
            status_message += (
                f"- Cached **{self.total_records_written:,}** records "
                f"into {self.total_batches_written:,} local cache file(s).\n\n"
            )
        if self.read_end_time is not None:
            read_end_time_str = _to_time_str(self.read_end_time)
            status_message += f"- Finished reading from source at `{read_end_time_str}`.\n\n"
        if self.finalize_start_time is not None:
            finalize_start_time_str = _to_time_str(self.finalize_start_time)
            status_message += f"**Started cache processing at `{finalize_start_time_str}`:**\n\n"
            status_message += (
                f"- Processed **{self.total_batches_finalized}** cache "
                f"file(s) over **{self.elapsed_finalization_time_str}**.\n\n"
            )
            if self.finalize_end_time is not None:
                completion_time_str = _to_time_str(self.finalize_end_time)
                status_message += f"- Finished cache processing at `{completion_time_str}`.\n\n"

        if self.finalized_stream_names:
            status_message += (
                f"**Completed processing {len(self.finalized_stream_names)} "
                + (f"out of {self.num_streams_expected} " if self.num_streams_expected else "")
                + "streams:**\n\n"
            )
            for stream_name in self.finalized_stream_names:
                status_message += f"  - {stream_name}\n"

        status_message += "\n\n"

        if self.finalize_end_time is not None:
            status_message += f"**Total time elapsed: {self.elapsed_time_string}**\n\n"
        status_message += "\n------------------------------------------------\n"

        return status_message

    def tally_pending_writes(
        self,
        messages: Iterable[AirbyteMessage] | IO[str],
    ) -> Generator[AirbyteMessage, None, None]:
        """This method simply tallies the number of records processed and yields the messages."""
        # Update the display before we start.
        self._update_display()
        self._start_rich_view()

        update_period = 1  # Reset the update period to 1 before start.

        for count, message in enumerate(messages, start=1):
            yield message  # Yield the message immediately.
            if isinstance(message, str):
                # This is a string message, not an AirbyteMessage.
                # For now at least, we don't need to pay the cost of parsing it.
                continue

            if message.record and message.record.stream:
                self.destination_stream_records_delivered[
                    message.state.stream.stream_descriptor.name
                ] += 1

            if count % update_period != 0:
                continue

            # If this is the first record, set the start time.
            if self.first_record_received_time is None:
                self.first_record_received_time = time.time()

            # Update the update period to the latest scale of data.
            update_period = self._get_update_period(count)

            # Update the display.
            self._update_display()

    def tally_confirmed_writes(
        self,
        messages: Iterable[AirbyteMessage],
    ) -> Generator[AirbyteMessage, Any, None]:
        """This method watches for state messages and tally records that are confirmed written.

        The original messages are passed through unchanged.
        """
        self._start_rich_view()  # Start Rich's live view if not already running.
        for message in messages:
            if message.type is Type.STATE:
                # This is a state message from the destination. Tally the records written.
                if message.state.stream:
                    stream_name = message.state.stream.stream_descriptor.name
                    if message.state.destinationStats:
                        self.destination_stream_records_confirmed[stream_name] += (
                            message.state.destinationStats.recordCount
                        )
                    self.destination_stream_records_confirmed[stream_name] += (
                        message.state.data.get("records_written", 0)
                    )
                self._update_display()

            yield message

        self._update_display()

    @property
    def total_destination_records_delivered(self) -> int:
        """Return the total number of records delivered to the destination."""
        if not self.destination_stream_records_delivered:
            return 0

        return sum(self.destination_stream_records_delivered.values())

    @property
    def total_destination_records_confirmed(self) -> int:
        """Return the total number of records confirmed by the destination."""
        if not self.destination_stream_records_confirmed:
            return 0

        return sum(self.destination_stream_records_confirmed.values())
