# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Universal source implementation using PyAirbyte to wrap any source connector."""

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING, Any

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    Type,
)
from airbyte_cdk.sources.source import Source

import airbyte as ab


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


logger = logging.getLogger("airbyte")


class SourcePyAirbyteUniversal(Source):
    """Universal source that wraps any PyAirbyte-supported source connector.

    This source acts as a proxy, using PyAirbyte to instantiate and run
    any registered source connector based on the configuration provided.
    """

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:  # noqa: ARG002
        """Return the connector specification."""
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/sources/pyairbyte-universal",
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "PyAirbyte Universal Source Spec",
                "type": "object",
                "required": ["source_name", "source_config"],
                "properties": {
                    "source_name": {
                        "type": "string",
                        "title": "Source Connector Name",
                        "description": (
                            "The name of the source connector to use "
                            "(e.g., 'source-github', 'source-postgres')."
                        ),
                        "examples": ["source-github", "source-postgres", "source-stripe"],
                    },
                    "source_config": {
                        "type": "object",
                        "title": "Source Configuration",
                        "description": "The configuration for the underlying source connector.",
                        "additionalProperties": True,
                    },
                    "source_version": {
                        "type": "string",
                        "title": "Source Version",
                        "description": "Optional: specific version of the source connector to use.",
                        "default": "latest",
                    },
                },
            },
        )

    def _get_pyairbyte_source(
        self,
        config: Mapping[str, Any],
    ) -> ab.Source:
        """Get a PyAirbyte source instance from the configuration."""
        source_name = config.get("source_name")
        source_config = config.get("source_config", {})
        source_version = config.get("source_version", "latest")

        if not source_name:
            raise ValueError("source_name is required in configuration")

        return ab.get_source(
            source_name,
            config=source_config,
            version=source_version if source_version != "latest" else None,
            install_if_missing=True,
        )

    def check(
        self,
        logger: logging.Logger,  # noqa: ARG002
        config: Mapping[str, Any],
    ) -> AirbyteConnectionStatus:
        """Test the connection to the underlying source connector."""
        try:
            source = self._get_pyairbyte_source(config)
            source.check()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except ValueError as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=str(e),
            )
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Connection check failed: {e!r}",
            )

    def discover(
        self,
        logger: logging.Logger,  # noqa: ARG002
        config: Mapping[str, Any],
    ) -> AirbyteCatalog:
        """Discover the catalog from the underlying source connector."""
        source = self._get_pyairbyte_source(config)
        # The catalog types are compatible at runtime but differ in type annotations
        return source.discovered_catalog  # pyrefly: ignore[bad-return]

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[Any] | None = None,  # noqa: ARG002
    ) -> Iterable[AirbyteMessage]:
        """Read data from the underlying source connector.

        This method uses PyAirbyte's get_records functionality to stream
        records from the configured source connector.
        """
        source = self._get_pyairbyte_source(config)

        # Select the streams from the catalog
        stream_names = [stream.stream.name for stream in catalog.streams]
        source.select_streams(stream_names)

        # Use get_records to iterate through each stream and yield AirbyteMessages
        for stream_name in stream_names:
            logger.info(f"Reading stream: {stream_name}")
            for record in source.get_records(stream_name):
                # Convert the record to an AirbyteMessage
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(
                        stream=stream_name,
                        data=dict(record),
                        emitted_at=int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                        ),
                    ),
                )
