# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from unittest.mock import patch

import pytest

from airbyte import exceptions as exc
from airbyte.cloud.client import CloudClient


def _api_patches(
    *,
    user: dict[str, object],
    parent_organization_id: str = "organization-id",
    permissions: list[dict[str, object]] | None = None,
):
    return (
        patch(
            "airbyte._util.api_util.get_user_by_auth_id",
            return_value=user,
        ),
        patch(
            "airbyte._util.api_util.get_user_id_from_bearer_token",
            return_value="auth-user-id",
        ),
        patch(
            "airbyte._util.api_util.get_bearer_token",
            return_value="bearer-token",
        ),
        patch(
            "airbyte._util.api_util.get_workspace_organization_info",
            return_value={"organizationId": parent_organization_id},
        ),
        patch(
            "airbyte._util.api_util.list_permissions_for_user",
            return_value=permissions or [],
        ),
    )


def test_authenticated_user_is_not_in_client_repr() -> None:
    client = CloudClient(bearer_token="token")

    assert "_authenticated_user_info" not in repr(client)


def test_get_workspace_uses_authenticated_user_default_workspace() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "defaultWorkspaceId": "user-workspace"}
    )
    with patches[0] as get_user, patches[1], patches[2], patches[3], patches[4]:
        workspace = CloudClient(bearer_token="token").get_workspace()

    assert workspace.workspace_id == "user-workspace"
    get_user.assert_called_once()


def test_get_workspace_explicit_workspace_ignores_authenticated_user_default() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "defaultWorkspaceId": "user-workspace"}
    )
    with patches[0] as get_user, patches[1], patches[2], patches[3], patches[4]:
        workspace = CloudClient(bearer_token="token").get_workspace(
            workspace_id="explicit-workspace"
        )

    assert workspace.workspace_id == "explicit-workspace"
    get_user.assert_not_called()


def test_configured_workspace_beats_authenticated_user_default() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "defaultWorkspaceId": "user-workspace"}
    )
    with patches[0] as get_user, patches[1], patches[2], patches[3], patches[4]:
        workspace = CloudClient(
            bearer_token="token",
            workspace_id="configured-workspace",
        ).get_workspace()

    assert workspace.workspace_id == "configured-workspace"
    get_user.assert_not_called()


def test_get_workspace_raises_when_authenticated_user_has_no_default_workspace() -> (
    None
):
    patches = _api_patches(user={"userId": "user-id"})
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        with pytest.raises(exc.PyAirbyteInputError, match="Workspace ID is required"):
            CloudClient(bearer_token="token").get_workspace()


def test_ambient_organization_uses_authenticated_user_default_workspace_parent() -> (
    None
):
    patches = _api_patches(
        user={"userId": "user-id", "defaultWorkspaceId": "user-workspace"},
        parent_organization_id="user-organization",
        permissions=[{"organizationId": "membership-organization"}],
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as get_workspace_organization_info,
        patches[4] as list_permissions,
    ):
        organization_id = CloudClient(
            bearer_token="token"
        )._resolve_ambient_organization_id()

    assert organization_id == "user-organization"
    get_workspace_organization_info.assert_called_once()
    list_permissions.assert_not_called()


def test_ambient_organization_falls_back_to_memberships_when_user_lookup_fails() -> (
    None
):
    patches = _api_patches(
        user={"userId": "user-id", "defaultWorkspaceId": "user-workspace"},
        permissions=[{"organizationId": "membership-organization"}],
    )
    with (
        patches[0] as get_user,
        patches[1],
        patches[2],
        patches[3],
        patches[4] as list_permissions,
    ):
        get_user.side_effect = exc.AirbyteError(message="User lookup failed.")
        client = CloudClient(bearer_token="token")
        client._authenticated_user_id = "user-id"  # noqa: SLF001
        organization_id = client._resolve_ambient_organization_id()  # noqa: SLF001

    assert organization_id == "membership-organization"
    get_user.assert_called_once()
    list_permissions.assert_called_once_with(
        "user-id",
        api_root=client.public_api_root,
        config_api_root=client.config_api_root,
        client_id=client.client_id,
        client_secret=client.client_secret,
        bearer_token=client.bearer_token,
    )


def test_authenticated_user_lookup_is_cached_across_workspace_and_organization_resolution() -> (
    None
):
    patches = _api_patches(
        user={"userId": "user-id", "defaultWorkspaceId": "user-workspace"},
        parent_organization_id="user-organization",
    )
    with patches[0] as get_user, patches[1], patches[2], patches[3], patches[4]:
        client = CloudClient(bearer_token="token")
        assert client.get_workspace().workspace_id == "user-workspace"
        assert client._resolve_ambient_organization_id() == "user-organization"

    assert get_user.call_count == 1
