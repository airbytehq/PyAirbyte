# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for preserving raw Airbyte Cloud destination configuration."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest
import responses
from airbyte._util import api_util
from airbyte.cloud import sync_results
from airbyte.cloud.models import CloudDestinationInfo
from airbyte.cloud.sync_results import SyncResult
from airbyte.secrets.base import SecretString


API_ROOT = "https://api.airbyte.test/api/public/v1"
DESTINATION_ID = "44444444-4444-4444-8444-444444444444"
DEFINITION_ID = "33333333-3333-4333-8333-333333333333"
WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
TOKEN = SecretString("test-bearer-token")

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


def _register_destination_response(configuration: dict[str, Any]) -> None:
    responses.add(
        responses.GET,
        f"{API_ROOT}/destinations/{DESTINATION_ID}",
        json={
            "destinationId": DESTINATION_ID,
            "name": "BigQuery destination",
            "destinationType": "bigquery",
            "definitionId": DEFINITION_ID,
            "workspaceId": WORKSPACE_ID,
            "createdAt": 1700000000,
            "configuration": configuration,
        },
    )


def _get_destination() -> Any:
    return api_util.get_destination(
        destination_id=DESTINATION_ID,
        api_root=API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
    )


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param(PARTIAL_CONFIGURATION, id="partial"),
        pytest.param(FULL_CONFIGURATION, id="full-with-unknown-key"),
    ],
)
@responses.activate
def test_get_destination_returns_raw_bigquery_configuration(
    configuration: dict[str, Any],
) -> None:
    """The destination response keeps the API configuration unchanged."""
    _register_destination_response(configuration)

    result = _get_destination()

    assert result.configuration == configuration


@responses.activate
def test_cloud_destination_info_keeps_raw_partial_configuration() -> None:
    """Cloud destination info exposes the raw API configuration."""
    _register_destination_response(PARTIAL_CONFIGURATION)

    result = _get_destination()
    destination_info = CloudDestinationInfo.from_api_response(result)

    assert destination_info.configuration == PARTIAL_CONFIGURATION


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
