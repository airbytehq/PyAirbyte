# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Destination base classes."""

from __future__ import annotations

import warnings
from typing import IO, TYPE_CHECKING, Any, cast

from typing_extensions import Literal

from airbyte_protocol.models import (
    Type,
)

from airbyte import exceptions as exc
from airbyte._connector_base import ConnectorBase
from airbyte._future_cdk.catalog_providers import CatalogProvider
from airbyte._future_cdk.state_writers import NoOpStateWriter, StateWriterBase, StdOutStateWriter
from airbyte._message_generators import AirbyteMessageGenerator
from airbyte._util.temp_files import as_temp_files
from airbyte.caches.util import get_default_cache
from airbyte.progress import ProgressTracker
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
        source_data: Source | ReadResult,
        *,
        streams: list[str] | Literal["*"] | None = None,
        cache: CacheBase | None | Literal[False] = None,
        state_cache: CacheBase | None | Literal[False] = None,
        write_strategy: WriteStrategy = WriteStrategy.AUTO,
        force_full_refresh: bool = False,
    ) -> WriteResult:
        """Write data to the destination.

        Args:
            source_data: The source data to write to the destination. Can be a `Source`, a `Cache`,
                or a `ReadResult` object.
            streams: The streams to write to the destination. If omitted or if "*" is provided,
                all streams will be written. If `source_data` is a source, then streams must be
                selected here or on the source. If both are specified, this setting will override
                the stream selection on the source.
            cache: The cache to use for reading source_data. If `None`, no cache will be used. If
                False, the cache will be disabled. This must be `None` if `source_data` is already
                a `Cache` object.
            state_cache: A cache to use for storing incremental state. You do not need to set this
                if `cache` is specified or if `source_data` is a `Cache` object. Set to `False` to
                disable state management.
            write_strategy: The strategy to use for writing source_data. If `AUTO`, the connector
                will decide the best strategy to use.
            force_full_refresh: Whether to force a full refresh of the source_data. If `True`, any
                existing state will be ignored and all source data will be reloaded.
        """
        if not isinstance(source_data, (ReadResult, Source)):
            raise exc.PyAirbyteInputError(
                message="Invalid source_data type for `source_data` arg.",
                context={
                    "source_data_type_provided": type(source_data).__name__,
                },
            )

        # Resolve `source`, `read_result`, and `source_name`
        source: Source | None = source_data if isinstance(source_data, Source) else None
        read_result: ReadResult | None = (
            source_data if isinstance(source_data, ReadResult) else None
        )
        source_name: str = source.name if source else cast(ReadResult, read_result).source_name

        # Resolve `cache`
        if cache is not False:
            if isinstance(source_data, ReadResult):
                cache = source_data.cache

            cache = cache or get_default_cache()

        progress_tracker = ProgressTracker(
            source=source if isinstance(source_data, Source) else None,
            cache=cache or None,
            destination=self,
        )

        # Resolve `state_cache`
        if state_cache is not False:
            state_cache = state_cache or get_default_cache()

        # Resolve `state_provider` and `state_writer`
        if state_cache:
            state_provider = (
                None
                if force_full_refresh
                else state_cache.get_state_provider(
                    source_name=source_name,
                    destination_name=self.name,
                )
            )
            state_writer: StateWriterBase = state_cache.get_state_writer(
                source_name=source_name,
                destination_name=self.name,
            )
        elif state_cache is False:
            state_writer = NoOpStateWriter()
            state_provider = None
        else:
            warnings.warn(
                "No state backend or cache provided. State will not be tracked."
                "To track state, provide a cache or state backend."
                "To silence this warning, set `state_cache=False` explicitly.",
                category=exc.PyAirbyteWarning,
                stacklevel=2,
            )
            state_writer = NoOpStateWriter()
            state_provider = None

        # Resolve `catalog_provider`
        if source:
            catalog_provider = CatalogProvider(
                configured_catalog=source.get_configured_catalog(
                    streams=streams,
                )
            )
        elif read_result:
            catalog_provider = CatalogProvider.from_read_result(read_result)
        else:
            raise exc.PyAirbyteInternalError(
                message="`source_data` must be a `Source` or `ReadResult` object.",
            )

        # Get message generator for source (caching disabled)
        if source:
            if cache is False:
                message_generator = source._get_airbyte_message_generator(  # noqa: SLF001 # Non-public API
                    streams=streams,
                    state_provider=state_provider,
                    progress_tracker=progress_tracker,
                    force_full_refresh=force_full_refresh,
                )
            else:
                # Caching enabled and we are reading from a source.
                # Read the data to cache if caching is enabled.
                read_result = source.read(
                    cache=cache,
                    streams=streams,
                    write_strategy=write_strategy,
                    force_full_refresh=force_full_refresh,
                    skip_validation=False,
                )
                message_generator = AirbyteMessageGenerator.from_read_result(
                    read_result=read_result,
                )
        else:  # Else we are reading from a read result
            assert read_result is not None
            message_generator = AirbyteMessageGenerator.from_read_result(
                read_result=read_result,
            )

        # Write the data to the destination
        self._write_airbyte_message_stream(
            stdin=message_generator,
            catalog_provider=catalog_provider,
            state_writer=state_writer,
            skip_validation=False,
            progress_tracker=progress_tracker,
        )
        return WriteResult(
            destination=self,
            source_data=source_data,
            catalog_provider=catalog_provider,
            state_writer=state_writer,
            progress_tracker=progress_tracker,
        )

    def _write_airbyte_message_stream(
        self,
        stdin: IO[str] | AirbyteMessageGenerator,
        *,
        catalog_provider: CatalogProvider,
        state_writer: StateWriterBase | None = None,
        skip_validation: bool = False,
        progress_tracker: ProgressTracker,
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
                # We call the connector to write the data, tallying the inputs and outputs
                for destination_message in progress_tracker.tally_confirmed_writes(
                    messages=self._execute(
                        args=[
                            "write",
                            "--config",
                            config_file,
                            "--catalog",
                            catalog_file,
                        ],
                        stdin=progress_tracker.tally_pending_writes(
                            stdin,  # type: ignore [arg-type]  # ignore mypy false-positive
                        ),
                    )
                ):
                    if destination_message.type is Type.STATE:
                        state_writer.write_state(state_message=destination_message.state)

            except exc.AirbyteConnectorFailedError as ex:
                raise exc.AirbyteConnectorWriteError(
                    connector_name=self.name,
                    log_text=self._last_log_messages,
                ) from ex


__all__ = [
    "Destination",
]
