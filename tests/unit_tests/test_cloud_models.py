"""Unit tests for Airbyte Cloud response models and connectors."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace

import pytest
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.models import (
    CloudConnectionInfo,
    CloudDestinationInfo,
    CloudSourceInfo,
    ConnectionSchedule,
    ConnectionStatus,
)
from airbyte_server_models.public_api.models import (
    DestinationResponse,
    SourceResponse,
)


@pytest.mark.parametrize(
    "response,from_api_response,expected_definition_id,has_configuration",
    [
        pytest.param(
            SourceResponse(
                configuration={},
                created_at=1,
                definition_id="source-faker-definition",
                name="Test source",
                source_id="source-id",
                source_type="faker",
                workspace_id="workspace-id",
            ),
            CloudSourceInfo.from_api_response,
            "source-faker-definition",
            True,
            id="source",
        ),
        pytest.param(
            SourceResponse.model_construct(
                configuration=None,
                created_at=1,
                definition_id="source-empty-definition",
                name="Source without config",
                source_id="source-empty-id",
                source_type="faker",
                workspace_id="workspace-id",
            ),
            CloudSourceInfo.from_api_response,
            "source-empty-definition",
            False,
            id="source-without-configuration",
        ),
        pytest.param(
            DestinationResponse(
                configuration={"destination_path": "/tmp/test.duckdb"},
                created_at=1,
                definition_id="destination-duckdb-definition",
                destination_id="destination-id",
                destination_type="duckdb",
                name="Test destination",
                workspace_id="workspace-id",
            ),
            CloudDestinationInfo.from_api_response,
            "destination-duckdb-definition",
            True,
            id="destination",
        ),
    ],
)
def test_cloud_connector_info_from_api_response_populates_definition_id(
    response: SourceResponse | DestinationResponse,
    from_api_response: Callable[..., CloudSourceInfo | CloudDestinationInfo],
    expected_definition_id: str,
    has_configuration: bool,
) -> None:
    """Cloud connector info retains its definition ID and typed configuration."""
    info = from_api_response(response)

    assert info.definition_id == expected_definition_id
    assert (info.configuration is not None) is has_configuration


@pytest.mark.parametrize(
    ("api_status", "expected_status"),
    [
        pytest.param("active", ConnectionStatus.ACTIVE, id="active"),
        pytest.param("inactive", ConnectionStatus.INACTIVE, id="inactive"),
        pytest.param("deprecated", ConnectionStatus.DEPRECATED, id="deprecated"),
    ],
)
def test_cloud_connection_info_from_api_response_populates_schedule(
    api_status: str,
    expected_status: ConnectionStatus,
) -> None:
    """`CloudConnectionInfo` carries the schedule returned by the API."""
    schedule = SimpleNamespace(schedule_type="manual")
    info = CloudConnectionInfo.from_api_response(
        SimpleNamespace(
            connection_id="conn-1",
            workspace_id="workspace-id",
            source_id="source-1",
            destination_id="dest-1",
            name="sync",
            configurations=None,
            prefix=None,
            namespace_definition=None,
            namespace_format=None,
            schedule=schedule,
            status=api_status,
        )
    )

    assert info.schedule.schedule_type == "manual"
    assert info.status is expected_status


@pytest.mark.parametrize(
    ("schedule_type", "cron_expression", "basic_timing", "expected_expression"),
    [
        pytest.param("cron", "0 8 * * *", None, "0 8 * * *", id="cron"),
        pytest.param("basic", None, "Every 24 HOURS", "Every 24 HOURS", id="basic"),
        pytest.param("manual", None, None, None, id="manual"),
    ],
)
def test_connection_schedule_from_api_response(
    schedule_type: str,
    cron_expression: str | None,
    basic_timing: str | None,
    expected_expression: str | None,
) -> None:
    """`ConnectionSchedule.from_api_response` maps schedule fields by type."""
    schedule = ConnectionSchedule.from_api_response(
        SimpleNamespace(
            schedule_type=schedule_type,
            cron_expression=cron_expression,
            basic_timing=basic_timing,
        )
    )

    assert schedule.schedule_type == schedule_type
    assert schedule.schedule_expression == expected_expression


@pytest.mark.parametrize(
    ("schedule", "expected"),
    [
        pytest.param(
            ConnectionSchedule(schedule_type="manual"),
            "manual",
            id="manual",
        ),
        pytest.param(
            ConnectionSchedule(schedule_type="cron", schedule_expression="0 8 * * *"),
            "0 8 * * *",
            id="cron_expression",
        ),
        pytest.param(
            ConnectionSchedule(schedule_type="cron"),
            "cron",
            id="cron_no_expression",
        ),
        pytest.param(
            ConnectionSchedule(
                schedule_type="basic", schedule_expression="every_24_hours"
            ),
            "every 24 hours",
            id="basic_every_24_hours",
        ),
        pytest.param(
            ConnectionSchedule(schedule_type="basic"),
            "basic",
            id="basic_no_expression",
        ),
    ],
)
def test_connection_schedule_friendly_description(
    schedule: ConnectionSchedule, expected: str
) -> None:
    """`friendly_description` renders each schedule type as a display string."""
    assert schedule.friendly_description == expected
    assert str(schedule) == expected


@pytest.mark.parametrize(
    "connector_factory,response,expected_definition_id",
    [
        pytest.param(
            CloudSource._from_source_response,
            SourceResponse(
                configuration={},
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
                configuration={"destination_path": "/tmp/test.duckdb"},
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
