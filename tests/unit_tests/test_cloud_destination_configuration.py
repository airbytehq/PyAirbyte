# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for preserving raw Airbyte Cloud connector configurations."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest
from airbyte._util import api_util
from airbyte.cloud import CloudWorkspace, sync_results
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.models import CloudDestinationInfo, CloudSourceInfo
from airbyte.cloud.sync_results import SyncResult
from airbyte.secrets.base import SecretString


API_ROOT = "https://api.airbyte.test/api/public/v1"
DESTINATION_ID = "44444444-4444-4444-8444-444444444444"
DEFINITION_ID = "33333333-3333-4333-8333-333333333333"
WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
TOKEN = SecretString("test-bearer-token")
SOURCE_ID = "55555555-5555-4555-8555-555555555555"

PARTIAL_CONFIGURATION = {
    "project_id": "my-project",
    "credentials_json": "**********",
}
FULL_CONFIGURATION = {
    "project_id": "p",
    "dataset_id": "d",
    "dataset_location": "US",
    "extra_unknown": 1,
}


def _destination_response(configuration: dict[str, Any]) -> dict[str, Any]:
    return {
        "destinationId": DESTINATION_ID,
        "name": "BigQuery destination",
        "destinationType": "bigquery",
        "definitionId": DEFINITION_ID,
        "workspaceId": WORKSPACE_ID,
        "createdAt": 1700000000,
        "configuration": configuration,
    }


def _get_destination() -> Any:
    return api_util.get_destination(
        destination_id=DESTINATION_ID,
        api_root=API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
    )


def _source_response(configuration: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceId": SOURCE_ID,
        "name": "Faker source",
        "sourceType": "faker",
        "definitionId": DEFINITION_ID,
        "workspaceId": WORKSPACE_ID,
        "createdAt": 1700000000,
        "configuration": configuration,
    }


def _mock_httpx_get(
    monkeypatch: pytest.MonkeyPatch,
    *,
    url: str,
    body: dict[str, Any],
) -> None:
    httpx_module = api_util.httpx

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert str(request.url) == url
        return httpx_module.Response(200, json=body, request=request)

    transport = httpx_module.MockTransport(handle)
    monkeypatch.setattr(
        api_util,
        "httpx",
        SimpleNamespace(Client=lambda: httpx_module.Client(transport=transport)),
    )


def _get_source() -> Any:
    return api_util.get_source(
        source_id=SOURCE_ID,
        api_root=API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
    )


@pytest.mark.parametrize(
    "connector_type",
    [
        pytest.param("source", id="source"),
        pytest.param("destination", id="destination"),
    ],
)
@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param(PARTIAL_CONFIGURATION, id="partial"),
        pytest.param(FULL_CONFIGURATION, id="full-with-unknown-key"),
    ],
)
def test_get_connector_returns_raw_configuration(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    configuration: dict[str, Any],
) -> None:
    """Source and destination responses preserve the raw API configuration."""
    if connector_type == "source":
        _mock_httpx_get(
            monkeypatch,
            url=f"{API_ROOT}/sources/{SOURCE_ID}",
            body=_source_response(configuration),
        )
        result = _get_source()
    else:
        _mock_httpx_get(
            monkeypatch,
            url=f"{API_ROOT}/destinations/{DESTINATION_ID}",
            body=_destination_response(configuration),
        )
        result = _get_destination()

    assert result.configuration == configuration


@pytest.mark.parametrize(
    "connector_type",
    [
        pytest.param("source", id="source"),
        pytest.param("destination", id="destination"),
    ],
)
def test_cloud_connector_info_preserves_raw_configuration(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
) -> None:
    """Cloud source and destination info expose raw API configuration."""
    if connector_type == "source":
        _mock_httpx_get(
            monkeypatch,
            url=f"{API_ROOT}/sources/{SOURCE_ID}",
            body=_source_response(PARTIAL_CONFIGURATION),
        )
        info = CloudSourceInfo.from_api_response(_get_source())
    else:
        _mock_httpx_get(
            monkeypatch,
            url=f"{API_ROOT}/destinations/{DESTINATION_ID}",
            body=_destination_response(PARTIAL_CONFIGURATION),
        )
        info = CloudDestinationInfo.from_api_response(_get_destination())

    assert info.configuration == PARTIAL_CONFIGURATION


@pytest.mark.parametrize(
    ("connector_type", "connector_id"),
    [
        pytest.param("source", SOURCE_ID, id="source"),
        pytest.param("destination", DESTINATION_ID, id="destination"),
    ],
)
def test_cloud_connector_configuration_is_cached(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    connector_id: str,
) -> None:
    """Source and destination configuration are fetched once and cached."""
    workspace = CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        bearer_token=TOKEN,
        api_root=API_ROOT,
    )
    connector_cls = CloudSource if connector_type == "source" else CloudDestination
    connector = connector_cls(workspace=workspace, connector_id=connector_id)
    if connector_type == "source":
        connector._connector_info = (  # noqa: SLF001  # Seed cache.
            CloudSourceInfo(
                source_id=connector_id,
                name="Faker source",
                definition_id=DEFINITION_ID,
                configuration=PARTIAL_CONFIGURATION,
            )
        )
        fetched_info = CloudSourceInfo(
            source_id=connector_id,
            name="Faker source",
            definition_id=DEFINITION_ID,
            configuration=FULL_CONFIGURATION,
        )
        expected_name = "Faker source"
    else:
        connector._connector_info = (  # noqa: SLF001  # Seed cache.
            CloudDestinationInfo(
                destination_id=connector_id,
                name="BigQuery destination",
                definition_id=DEFINITION_ID,
                configuration=PARTIAL_CONFIGURATION,
            )
        )
        fetched_info = CloudDestinationInfo(
            destination_id=connector_id,
            name="BigQuery destination",
            definition_id=DEFINITION_ID,
            configuration=FULL_CONFIGURATION,
        )
        expected_name = "BigQuery destination"
    fetch_info = Mock(return_value=fetched_info)
    monkeypatch.setattr(connector, "_fetch_connector_info", fetch_info)

    assert connector.configuration == FULL_CONFIGURATION
    assert connector.configuration == FULL_CONFIGURATION
    fetch_info.assert_called_once_with()
    assert connector.name == expected_name


def test_sync_result_destination_configuration_includes_destination_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync destination configuration includes its type for cache conversion."""
    raw_configuration = {"project_id": "my-project", "credentials_json": "**********"}
    sync_result = SyncResult(
        workspace=cast(Any, Mock()),
        connection=cast(Any, Mock()),
        job_id=1,
    )
    monkeypatch.setattr(
        sync_result,
        "_get_connection_info",
        Mock(return_value=SimpleNamespace(destination_id=DESTINATION_ID)),
    )
    monkeypatch.setattr(
        sync_results.api_util,
        "get_destination",
        Mock(
            return_value=SimpleNamespace(
                configuration=raw_configuration,
                destination_type="bigquery",
            )
        ),
    )

    assert sync_result._get_destination_configuration() == {
        **raw_configuration,
        "destinationType": "bigquery",
    }
