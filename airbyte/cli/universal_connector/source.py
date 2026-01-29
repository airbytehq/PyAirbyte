# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Universal source implementation using PyAirbyte to wrap any source connector."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.source import Source

import airbyte as ab
from airbyte.progress import ProgressStyle, ProgressTracker


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
        # Convert PyAirbyte catalog to CDK catalog format
        # Serialize to dict with enum values as strings, then construct CDK objects
        catalog_json = source.discovered_catalog.model_dump_json()
        catalog_dict = json.loads(catalog_json)

        streams = []
        for stream_dict in catalog_dict.get("streams", []):
            # Convert sync mode strings to SyncMode enums
            sync_modes = [SyncMode(mode) for mode in stream_dict.get("supported_sync_modes", [])]
            streams.append(
                AirbyteStream(
                    name=stream_dict["name"],
                    json_schema=stream_dict.get("json_schema", {}),
                    supported_sync_modes=sync_modes,
                    source_defined_cursor=stream_dict.get("source_defined_cursor"),
                    default_cursor_field=stream_dict.get("default_cursor_field"),
                    source_defined_primary_key=stream_dict.get("source_defined_primary_key"),
                    namespace=stream_dict.get("namespace"),
                )
            )
        return AirbyteCatalog(streams=streams)

    def read(
        self,
        logger: logging.Logger,  # noqa: ARG002
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[Any] | None = None,  # noqa: ARG002
    ) -> Iterable[AirbyteMessage]:
        """Read data from the underlying source connector.

        This method uses PyAirbyte's _read_with_catalog to stream
        AirbyteMessages from the configured source connector.
        """
        source = self._get_pyairbyte_source(config)

        # Select the streams from the catalog
        stream_names = [stream.stream.name for stream in catalog.streams]
        source.select_streams(stream_names)

        # Get the configured catalog from PyAirbyte
        configured_catalog = source.get_configured_catalog(streams=stream_names)

        # Use _read_with_catalog to get raw AirbyteMessages
        progress_tracker = ProgressTracker(
            ProgressStyle.PLAIN,
            source=source,
            cache=None,
            destination=None,
            expected_streams=stream_names,
        )

        # Convert PyAirbyte messages to CDK messages
        # The types differ (airbyte_protocol.models vs airbyte_cdk.models) and enum values
        # need to be serialized to strings for the CDK serializer
        for message in source._read_with_catalog(  # noqa: SLF001
            catalog=configured_catalog,
            progress_tracker=progress_tracker,
        ):
            # Only yield RECORD messages - filter out LOG, TRACE, etc.
            # The CDK will handle its own logging
            if message.type.name == "RECORD" and message.record:
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(
                        stream=message.record.stream,
                        data=message.record.data,
                        emitted_at=message.record.emitted_at,
                        namespace=message.record.namespace,
                    ),
                )
            # TK-TODO: Add STATE message handling for incremental sync support.
            # STATE messages require per-stream format conversion which is complex.
            # For now, only full-refresh syncs are supported.
