# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test destination capabilities using the JSONL destination (docker-based)."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from airbyte import Destination, Source, get_destination, get_source

from airbyte_protocol.models import (
    AirbyteControlConnectorConfigMessage,
    AirbyteControlMessage,
    AirbyteMessage,
    OrchestratorType,
    Type,
)


def config_change_callback(config: dict[str, Any]) -> None:
    print(f"Updated config: {config}")


@pytest.fixture
def new_duckdb_destination() -> Destination:
    """Return a new JSONL destination."""
    return get_destination(
        name="destination-duckdb",
        config={
            # This path is relative to the container:
            "destination_path": "/local/temp/db.duckdb",
        },
        config_change_callback=config_change_callback,
    )


@pytest.fixture
def new_source_faker(*, use_docker: bool) -> Source:
    return get_source(
        "source-faker",
        config={
            "count": 100,
            "seed": 1234,
            "parallelism": 16,
        },
        install_if_missing=True,
        streams=["products"],
        config_change_callback=config_change_callback,
        docker_image=use_docker,
    )


def test_source_config_callback(
    new_duckdb_destination: Destination,
    new_source_faker: Source,
) -> None:
    with patch.object(
        new_source_faker, "config_change_callback"
    ) as mock_config_change_callback:
        updated_config = {
            "count": 1000,
            "seed": 1234,
            "parallelism": 16,
        }
        airbyte_source_control_message = AirbyteMessage(
            type=Type.CONTROL,
            control=AirbyteControlMessage(
                type=OrchestratorType.CONNECTOR_CONFIG,
                emitted_at=0,
                connectorConfig=AirbyteControlConnectorConfigMessage(
                    config=updated_config
                ),
            ),
        )

        new_source_faker._peek_airbyte_message(airbyte_source_control_message)
        mock_config_change_callback.assert_called_once_with(updated_config)


def test_destination_config_callback(
    new_duckdb_destination: Destination,
    new_source_faker: Source,
) -> None:
    with patch.object(
        new_duckdb_destination, "config_change_callback"
    ) as mock_config_change_callback:
        updated_config = {
            "destination_path": "/local/temp/db.duckdb",
        }
        airbyte_destination_control_message = AirbyteMessage(
            type=Type.CONTROL,
            control=AirbyteControlMessage(
                type=OrchestratorType.CONNECTOR_CONFIG,
                emitted_at=0,
                connectorConfig=AirbyteControlConnectorConfigMessage(
                    config=updated_config
                ),
            ),
        )

        new_duckdb_destination._peek_airbyte_message(
            airbyte_destination_control_message
        )
        mock_config_change_callback.assert_called_once_with(updated_config)
