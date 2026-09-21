"""Unit tests for Airbyte Cloud response models and connectors."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.models import CloudDestinationInfo, CloudSourceInfo
from airbyte_api.models import (
    DestinationDuckdb,
    DestinationResponse,
    SourceFaker,
    SourceResponse,
)


@pytest.mark.parametrize(
    "response,from_api_response,expected_definition_id",
    [
        pytest.param(
            SourceResponse(
                configuration=SourceFaker(),
                created_at=1,
                definition_id="source-faker-definition",
                name="Test source",
                source_id="source-id",
                source_type="faker",
                workspace_id="workspace-id",
            ),
            CloudSourceInfo.from_api_response,
            "source-faker-definition",
            id="source",
        ),
        pytest.param(
            DestinationResponse(
                configuration=DestinationDuckdb(destination_path="/tmp/test.duckdb"),
                created_at=1,
                definition_id="destination-duckdb-definition",
                destination_id="destination-id",
                destination_type="duckdb",
                name="Test destination",
                workspace_id="workspace-id",
            ),
            CloudDestinationInfo.from_api_response,
            "destination-duckdb-definition",
            id="destination",
        ),
    ],
)
def test_cloud_connector_info_from_api_response_populates_definition_id(
    response: SourceResponse | DestinationResponse,
    from_api_response: Callable[..., CloudSourceInfo | CloudDestinationInfo],
    expected_definition_id: str,
) -> None:
    """Verify Cloud connector info models retain the API definition ID."""
    info = from_api_response(response)

    assert info.definition_id == expected_definition_id


@pytest.mark.parametrize(
    "connector_factory,response,expected_definition_id",
    [
        pytest.param(
            CloudSource._from_source_response,
            SourceResponse(
                configuration=SourceFaker(),
                created_at=1,
                definition_id="source-faker-definition",
                name="Test source",
                source_id="source-id",
                source_type="faker",
                workspace_id="workspace-id",
            ),
            "source-faker-definition",
            id="source",
        ),
        pytest.param(
            CloudDestination._from_destination_response,
            DestinationResponse(
                configuration=DestinationDuckdb(destination_path="/tmp/test.duckdb"),
                created_at=1,
                definition_id="destination-duckdb-definition",
                destination_id="destination-id",
                destination_type="duckdb",
                name="Test destination",
                workspace_id="workspace-id",
            ),
            "destination-duckdb-definition",
            id="destination",
        ),
    ],
)
def test_cloud_connector_definition_id_uses_cached_info(
    connector_factory: Callable[..., CloudSource | CloudDestination],
    response: SourceResponse | DestinationResponse,
    expected_definition_id: str,
) -> None:
    """Verify Cloud connector definition IDs are available from cached API info."""
    connector = connector_factory(
        CloudWorkspace(workspace_id="workspace-id", bearer_token="token"),
        response,
    )

    assert connector.definition_id == expected_definition_id


def test_destination_configuration_fetches_via_get_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """List-seeded info does not supply configuration; it is fetched on first access."""
    destination = CloudDestination(
        workspace=CloudWorkspace(workspace_id="workspace-id", bearer_token="token"),
        connector_id="destination-id",
    )
    destination._connector_info = CloudDestinationInfo(  # noqa: SLF001
        destination_id="destination-id",
        name="Test destination",
        definition_id="destination-snowflake-definition",
        configuration={"database": "x"},
    )
    fetched_info = CloudDestinationInfo(
        destination_id="destination-id",
        name="Test destination",
        definition_id="destination-snowflake-definition",
        configuration={"database": "x", "schema": "y"},
    )
    fetch_calls: list[None] = []

    def _fetch() -> CloudDestinationInfo:
        fetch_calls.append(None)
        return fetched_info

    monkeypatch.setattr(destination, "_fetch_connector_info", _fetch)

    assert destination.configuration is not None
    assert destination.configuration is not None
    assert destination.configuration["schema"] == "y"
    assert len(fetch_calls) == 1
    assert destination.name == "Test destination"
    assert len(fetch_calls) == 1
