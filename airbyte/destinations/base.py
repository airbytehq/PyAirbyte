# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Destination base classes."""

from __future__ import annotations

from typing import IO, TYPE_CHECKING, Any

from typing_extensions import Literal

from airbyte_protocol.models import (
    Type,
)

from airbyte import exceptions as exc
from airbyte._connector_base import ConnectorBase
from airbyte._future_cdk.catalog_providers import CatalogProvider
from airbyte._future_cdk.state_writers import StdOutStateWriter
from airbyte._message_generators import AirbyteMessageGenerator
from airbyte._util.temp_files import as_temp_files
from airbyte.progress import WriteProgress
from airbyte.results import ReadResult, WriteResult
from airbyte.sources.base import Source
from airbyte.strategies import WriteStrategy


if TYPE_CHECKING:
    from airbyte._future_cdk.state_writers import StateWriterBase
    from airbyte.caches.base import CacheBase
    from airbyte.executors.base import Executor


class Destination(ConnectorBase):
    """A class representing a destination that can be called."""

    connector_type: Literal["destination"] = "destination"

    def __init__(
        self,
        executor: Executor,
        name: str,
        config: dict[str, Any] | None = None,
        *,
        validate: bool = False,
    ) -> None:
        """Initialize the source.

        If config is provided, it will be validated against the spec if validate is True.
        """
        super().__init__(
            executor=executor,
            name=name,
            config=config,
            validate=validate,
        )

    def write(
        self,
        data: Source | ReadResult,
        *,
        streams: list[str] | str | None = None,
        cache: CacheBase | None | Literal[False] = None,
        write_strategy: WriteStrategy = WriteStrategy.AUTO,
        state_backend: CacheBase | None | Literal[False] = None,
        force_full_refresh: bool = False,
    ) -> WriteResult:
        """Write data to the destination.

        Args:
            data: The data to write to the destination. Can be a `Source`, a `ReadResult`, or a
                `Dataset` object.
            streams: The streams to write to the destination. If omitted or if "*" is provided,
                all streams will be written. If `data` is a source, then streams must be selected
                here or on the source. If both are specified, this setting will override the stream
                selection on the source.
            cache: The cache to use for reading data. If `None`, no cache will be used. If False,
                the cache will be disabled. This must be `None` if `data` is already a `Cache`
                object.
            write_strategy: The strategy to use for writing data. If `AUTO`, the connector will
                decide the best strategy to use.
            state_backend: A cache to use for storing incremental state. You do not need to set this
                if `cache` is specified or if `data` is a `Cache` object. Set to `False` to disable
                state management.
            force_full_refresh: Whether to force a full refresh of the data. If `True`, any existing
                state will be ignored and all data will be re-read.
        """
        source: Source | None = None
        read_result: ReadResult | None = None
        if isinstance(data, Source) and cache is not False:
            source = data
            read_result = source.read(
                cache=cache,
                streams=streams,
                write_strategy=write_strategy,
                force_full_refresh=force_full_refresh,
                skip_validation=False,
            )
        elif isinstance(data, ReadResult):
            read_result = data
            cache = cache or read_result.cache

        else:
            raise exc.PyAirbyteInputError(
                message="Invalid input type for `data`.",
                context={"data_type_provided": type(data).__name__},
            )

        if state_backend:
            state_cache = state_backend

        progress_tracker = WriteProgress()

        # Write the data
        self._write_airbyte_message_stream(
            stdin=AirbyteMessageGenerator.from_read_result(read_result),
            catalog_provider=CatalogProvider.from_read_result(read_result),
            state_writer=state_cache.get_state_writer(read_result.source_name),
            skip_validation=False,
            progress_tracker=progress_tracker,
        )
        return WriteResult.from_progress_tracker(progress_tracker)

    def _write_airbyte_message_stream(
        self,
        stdin: IO[str] | AirbyteMessageGenerator,
        *,
        catalog_provider: CatalogProvider,
        state_writer: StateWriterBase | None = None,
        skip_validation: bool = False,
        progress_tracker: WriteProgress,
    ) -> None:
        """Read from the connector and write to the cache."""
        _ = progress_tracker  # TODO: Implement progress tracking

        # Run optional validation step
        if not skip_validation:
            self.validate_config()

        if state_writer is None:
            state_writer = StdOutStateWriter()

        with as_temp_files(
            files_contents=[
                self._config,
                catalog_provider.configured_catalog.model_dump_json(),
            ]
        ) as [
            config_file,
            catalog_file,
        ]:
            try:
                for destination_message in self._execute(
                    [
                        "write",
                        "--config",
                        config_file,
                        "--catalog",
                        catalog_file,
                    ],
                    stdin=stdin,
                ):
                    if destination_message.type is Type.STATE:
                        state_writer.write_state(state_message=destination_message.state)

            # TODO: We should catch more specific exceptions here
            except Exception as ex:
                raise exc.AirbyteConnectorFailedError(
                    connector_name=self.name,
                    log_text=self._last_log_messages,
                ) from ex


__all__ = [
    "Destination",
]
