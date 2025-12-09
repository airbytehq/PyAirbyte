# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for CloudWorkspace error handling in list operations."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airbyte_api.errors import SDKError

from airbyte.cloud.workspaces import _is_corrupted_resource_error


@pytest.mark.parametrize(
    "error_body,expected",
    [
        pytest.param(
            '{"message":"Secret reference 0c646a6d-8cfa-4c97-9615-86d8ad3bbaf8 '
            'does not exist but is referenced in the config"}',
            True,
            id="secret_reference_error",
        ),
        pytest.param(
            '{"message":"Internal server error"}',
            False,
            id="unrelated_500_error",
        ),
        pytest.param(
            '{"message":"Secret reference abc123"}',
            False,
            id="partial_match_secret_only",
        ),
        pytest.param(
            '{"message":"does not exist but is referenced in the config"}',
            False,
            id="partial_match_config_only",
        ),
        pytest.param(
            "",
            False,
            id="empty_body",
        ),
        pytest.param(
            None,
            False,
            id="none_body",
        ),
    ],
)
def test_is_corrupted_resource_error(error_body: str | None, expected: bool) -> None:
    """Test that _is_corrupted_resource_error correctly identifies corrupted resource errors."""
    error = MagicMock(spec=SDKError)
    error.body = error_body
    assert _is_corrupted_resource_error(error) is expected


@pytest.mark.parametrize(
    "list_method,api_mock_path",
    [
        pytest.param(
            "list_destinations",
            "airbyte.cloud.workspaces.api_util.list_destinations",
            id="list_destinations",
        ),
        pytest.param(
            "list_sources",
            "airbyte.cloud.workspaces.api_util.list_sources",
            id="list_sources",
        ),
        pytest.param(
            "list_connections",
            "airbyte.cloud.workspaces.api_util.list_connections",
            id="list_connections",
        ),
    ],
)
def test_list_operations_return_empty_on_corrupted_resource(
    list_method: str, api_mock_path: str
) -> None:
    """List operations should return empty list when corrupted resource error occurs."""
    corrupted_error = SDKError(
        message="API error occurred",
        status_code=500,
        body=(
            '{"message":"Secret reference 0c646a6d-8cfa-4c97-9615-86d8ad3bbaf8 '
            'does not exist but is referenced in the config"}'
        ),
        raw_response=MagicMock(),
    )

    with patch(api_mock_path) as mock_list:
        mock_list.side_effect = corrupted_error

        from airbyte.cloud.workspaces import CloudWorkspace

        with patch.object(CloudWorkspace, "__post_init__"):
            workspace = CloudWorkspace(
                workspace_id="test-workspace-id",
                client_id="test-client-id",
                client_secret="test-client-secret",
            )

        method = getattr(workspace, list_method)
        result = method()
        assert result == []


@pytest.mark.parametrize(
    "list_method,api_mock_path",
    [
        pytest.param(
            "list_destinations",
            "airbyte.cloud.workspaces.api_util.list_destinations",
            id="list_destinations",
        ),
        pytest.param(
            "list_sources",
            "airbyte.cloud.workspaces.api_util.list_sources",
            id="list_sources",
        ),
        pytest.param(
            "list_connections",
            "airbyte.cloud.workspaces.api_util.list_connections",
            id="list_connections",
        ),
    ],
)
def test_list_operations_raise_on_other_errors(
    list_method: str, api_mock_path: str
) -> None:
    """List operations should raise for non-corrupted-resource errors."""
    other_error = SDKError(
        message="API error occurred",
        status_code=500,
        body='{"message":"Internal server error"}',
        raw_response=MagicMock(),
    )

    with patch(api_mock_path) as mock_list:
        mock_list.side_effect = other_error

        from airbyte.cloud.workspaces import CloudWorkspace

        with patch.object(CloudWorkspace, "__post_init__"):
            workspace = CloudWorkspace(
                workspace_id="test-workspace-id",
                client_id="test-client-id",
                client_secret="test-client-secret",
            )

        method = getattr(workspace, list_method)
        with pytest.raises(SDKError):
            method()
