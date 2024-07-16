# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Destination base classes."""

from __future__ import annotations

from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, cast

from typing_extensions import Literal

from airbyte_protocol.models import (
    Type,
)

from airbyte import exceptions as exc
from airbyte._connector_base import ConnectorBase
from airbyte._future_cdk.record_processor import RecordProcessorBase
from airbyte._future_cdk.state_writers import StdOutStateWriter
from airbyte._message_generators import (
    AirbyteMessageGenerator,
    FileBasedMessageGenerator,
)
from airbyte._util.temp_files import as_temp_files


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte._future_cdk.catalog_providers import CatalogProvider
    from airbyte._future_cdk.state_writers import StateWriterBase
    from airbyte.executors.base import Executor


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

    def write(
        self,
        stdin: IO[str] | AirbyteMessageGenerator,
        *,
        catalog_provider: CatalogProvider,
        state_writer: StateWriterBase | None = None,
        skip_validation: bool = False,
    ) -> None:
        """Read from the connector and write to the cache."""
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
                    log_text=self._last_log_messages,
                ) from ex

    def _write_stream_from_files(
        self,
        stream_name: str,
        data_files: list[Path],
        *,
        catalog_provider: CatalogProvider,
        state_writer: StateWriterBase,
    ) -> None:
        """Write records to the destination."""
        print(f"Sending `{stream_name}` stream data to destination `{self.name}`...")
        message_iterator: AirbyteMessageGenerator
        file_iterator: Iterator[Path] = iter(data_files)

        def fn_file_opener(file_path: Path) -> IO[str]:
            return cast(IO[str], Path.open(file_path, encoding="utf-8"))

        message_iterator = FileBasedMessageGenerator(
            file_iterator=file_iterator,
            file_opener=fn_file_opener,
        )
        self.write(
            stdin=message_iterator,
            catalog_provider=catalog_provider,
            state_writer=state_writer,
        )
        print(f"Finished processing `{stream_name}` stream data in `{self.name}` destination.")


__all__ = [
    "Destination",
]
