# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Destination base classes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Literal

from airbyte_protocol.models.airbyte_protocol import AirbyteMessage, ConfiguredAirbyteCatalog

from airbyte._connector_base import ConnectorBase
from airbyte._future_cdk.record_processor import RecordProcessorBase
from airbyte._message_generators import (
    AirbyteMessageGenerator,
    FileBasedMessageGenerator,
    StdinMessageGenerator,
)
from airbyte._util.temp_files import as_temp_files


if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from airbyte._executors.base import Executor
    from airbyte._future_cdk.state_writers import StateWriterBase


class DestinationProcessor(RecordProcessorBase):
    """Processor for Destinations."""


class Destination(ConnectorBase):
    """A class representing a destination that can be called."""

    connector_type: Literal["destination", "source"]

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

    # TODO: Delete if not needed:
    # def _write(
    #     self,
    #     source: Source,
    #     *,
    #     streams: Literal["*"] | list[str] | None = None,
    #     write_strategy: str | WriteStrategy = WriteStrategy.AUTO,
    #     skip_validation: bool = False,
    #     state_writer: StateWriterBase,
    #     state_provider: state_providers.StateProviderBase,
    # ) -> None:
    #     """Write records to the destination."""
    #     source.read()
    #     configured_catalog: ConfiguredAirbyteCatalog = source.get_configured_catalog(
    #         streams=streams
    #     )

    #     if not skip_validation:
    #         source.validate_config()
    #         self.validate_config()

    #     source_msg_iterator: Iterator[AirbyteMessage] = source._read_with_catalog(
    #         catalog=configured_catalog,
    #         state=state_provider,
    #     )
    #     str_iterator: Generator[str, None, None] = (str(msg) for msg in source_msg_iterator)
    #     # Create an IO buffer to store a stringified version of the iterator output

    def _write_stream(
        self,
        stream_name: str,
        data_files: list[Path] | None = None,
        *,
        configured_catalog: ConfiguredAirbyteCatalog,
        state_writer: StateWriterBase,
    ) -> None:
        """Write records to the destination."""
        print(f"Sending `{stream_name}` stream data to destination `{self.name}`...")
        message_iterator: AirbyteMessageGenerator
        if data_files:
            file_iterator: Iterator[Path] = iter(data_files)
            message_iterator = FileBasedMessageGenerator(
                file_iterator=file_iterator,
                file_opener=open,  # TODO: Use explicit utf-8 encoding
            )
        else:
            message_iterator = StdinMessageGenerator()
        with as_temp_files(
            [
                self._config,
                configured_catalog.dict(),
            ]
        ) as [
            config_file,
            catalog_file,
        ]:
            destination_message: AirbyteMessage
            for destination_message in self._execute(
                [
                    "write",
                    "--config",
                    config_file,
                    "--catalog",
                    catalog_file,
                ],
                stdin=message_iterator,
            ):
                if destination_message.type is AirbyteMessage.Type.STATE:
                    state_writer.write_state(destination_message.state)
        print(f"Finished processing `{stream_name}` stream data in `{self.name}` destination.")


__all__ = [
    "Destination",
]
