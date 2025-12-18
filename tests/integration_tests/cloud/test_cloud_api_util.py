# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test CRUD operations on the Airbyte API.

These tests are designed to be run against a running instance of the Airbyte API.
"""

from __future__ import annotations

from typing import Literal

import pytest
from airbyte._util import api_util, text_util
from airbyte._util.api_util import (
    CLOUD_API_ROOT,
    AirbyteError,
    check_connector,
    get_bearer_token,
)
from airbyte.secrets.base import SecretString
from airbyte_api.models import (
    DestinationDuckdb,
    DestinationResponse,
    SourceFaker,
    SourceResponse,
    WorkspaceResponse,
)


def test_get_workspace(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    workspace = api_util.get_workspace(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert workspace.workspace_id == workspace_id


def test_list_workspaces(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    result: list[WorkspaceResponse] = api_util.list_workspaces(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert result
    assert len(result) > 0
    assert all(isinstance(workspace, WorkspaceResponse) for workspace in result)


def test_list_sources(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    result: list[SourceResponse] = api_util.list_sources(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert (
        result
        and len(result) > 0
        and all(isinstance(source, SourceResponse) for source in result)
    )


def test_list_destinations(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    result: list[DestinationResponse] = api_util.list_destinations(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert (
        result
        and len(result) > 0
        and all(isinstance(destination, DestinationResponse) for destination in result)
    )


def test_create_and_delete_source(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    new_resource_name = "deleteme-source-faker" + text_util.generate_random_suffix()
    source_config = SourceFaker()
    source = api_util.create_source(
        name=new_resource_name,
        workspace_id=workspace_id,
        config=source_config,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert source.name == new_resource_name
    assert source.source_type == "faker"
    assert source.source_id

    api_util.delete_source(
        source_id=source.source_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )


def test_create_and_delete_destination(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    motherduck_api_key: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    new_resource_name = (
        "deleteme-destination-faker" + text_util.generate_random_suffix()
    )
    destination_config = DestinationDuckdb(
        destination_path="temp_db",
        motherduck_api_key=motherduck_api_key,
    )

    destination = api_util.create_destination(
        name=new_resource_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        config=destination_config,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert destination.name == new_resource_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )


def test_create_and_delete_connection(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
    motherduck_api_key: str,
) -> None:
    new_source_name = "deleteme-source-faker" + text_util.generate_random_suffix()
    new_destination_name = (
        "deleteme-destination-dummy" + text_util.generate_random_suffix()
    )
    new_connection_name = (
        "deleteme-connection-dummy" + text_util.generate_random_suffix()
    )
    source = api_util.create_source(
        name=new_source_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        config=SourceFaker(),
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert source.name == new_source_name
    assert source.source_type == "faker"
    assert source.source_id

    destination = api_util.create_destination(
        name=new_destination_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        config=DestinationDuckdb(
            destination_path="temp_db",
            motherduck_api_key=motherduck_api_key,
        ),
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert destination.name == new_destination_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    connection = api_util.create_connection(
        name=new_connection_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        source_id=source.source_id,
        destination_id=destination.destination_id,
        prefix="",
        selected_stream_names=["users", "purchases", "products"],
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    assert connection.source_id == source.source_id
    assert connection.destination_id == destination.destination_id
    assert connection.connection_id

    api_util.delete_connection(
        connection_id=connection.connection_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    api_util.delete_source(
        source_id=source.source_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )
    api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
        bearer_token=None,
    )


@pytest.mark.parametrize(
    "api_root",
    [
        CLOUD_API_ROOT,
    ],
)
def test_get_bearer_token(
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
    api_root: str,
) -> None:
    try:
        token: SecretString = get_bearer_token(
            client_id=airbyte_cloud_client_id,
            client_secret=airbyte_cloud_client_secret,
            api_root=api_root,
        )
        assert token is not None
    except AirbyteError as e:
        pytest.fail(f"API call failed: {e}")


@pytest.mark.parametrize(
    "connector_id, connector_type, expect_success",
    [
        ("f45dd701-d1f0-4e8e-97c4-2b89c40ac928", "source", True),
        # ("......-....-....-............", "destination", True),
    ],
)
def test_check_connector(
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
    connector_id: str,
    connector_type: Literal["source", "destination"],
    expect_success: bool,
) -> None:
    try:
        result, error_message = check_connector(
            actor_id=connector_id,
            connector_type=connector_type,
            client_id=airbyte_cloud_client_id,
            client_secret=airbyte_cloud_client_secret,
            bearer_token=None,
        )
        assert result == expect_success
    except AirbyteError as e:
        pytest.fail(f"API call failed: {e}")


def test_404_error_includes_request_url_context() -> None:
    """Test that 404 API errors include the full request URL in context.

    This test validates that when an API call fails with a 404, the error
    includes the full request URL that was attempted. This helps debug URL
    construction issues like those seen with custom API roots.

    Uses httpbin.org which returns 404 for /sources endpoint, allowing us
    to verify the error context contains the expected URL information.
    """
    from urllib.parse import parse_qs, urlparse

    api_root = "https://httpbin.org"
    workspace_id = "00000000-0000-0000-0000-000000000000"
    fake_bearer_token = SecretString("fake-token-for-testing")

    with pytest.raises(AirbyteError) as exc_info:
        api_util.list_sources(
            workspace_id=workspace_id,
            api_root=api_root,
            client_id=None,
            client_secret=None,
            bearer_token=fake_bearer_token,
        )

    error = exc_info.value

    # Assert error has context
    assert error.context is not None, "Error should have context dict"

    # Assert required context keys exist
    assert "api_root" in error.context, "Error context should contain 'api_root'"
    assert "status_code" in error.context, "Error context should contain 'status_code'"
    assert "request_url" in error.context, "Error context should contain 'request_url'"

    # Assert context values
    assert error.context["api_root"] == api_root
    assert error.context["status_code"] == 404

    # Parse and validate the request URL
    request_url = str(error.context["request_url"])
    parsed = urlparse(request_url)

    assert parsed.netloc == "httpbin.org", (
        f"Expected host 'httpbin.org', got '{parsed.netloc}'"
    )
    assert parsed.path.endswith("/sources"), (
        f"Expected path ending with '/sources', got '{parsed.path}'"
    )

    # Validate query params include workspace ID
    query_params = parse_qs(parsed.query)
    assert "workspaceIds" in query_params, "Expected 'workspaceIds' in query params"
    assert query_params["workspaceIds"] == [workspace_id], (
        f"Expected workspaceIds=['{workspace_id}'], got {query_params['workspaceIds']}"
    )


def test_url_construction_with_path_prefix() -> None:
    """Test that the SDK correctly preserves path prefixes in the base URL.

    This test uses httpbin.org/anything which echoes back the request details,
    allowing us to verify the actual URL that was constructed and sent.

    This test validates that when using a custom API root with a path prefix
    (like https://host/api/public/v1), the SDK correctly appends endpoints
    to that path rather than replacing it.
    """
    base_url_with_path = "https://httpbin.org/anything/api/public/v1"
    fake_bearer_token = SecretString("fake-token-for-testing")
    fake_workspace_id = "00000000-0000-0000-0000-000000000000"

    result = api_util.list_sources(
        workspace_id=fake_workspace_id,
        api_root=base_url_with_path,
        client_id=None,
        client_secret=None,
        bearer_token=fake_bearer_token,
    )

    print(f"\nBase URL with path prefix: {base_url_with_path}")
    print(f"Result type: {type(result)}")
    print(f"Result: {result}")

    assert result == [], (
        f"Expected empty list from httpbin (no real sources), but got: {result}"
    )
