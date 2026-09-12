# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from unittest.mock import patch

import pytest

from airbyte import exceptions as exc
from airbyte.cloud.client import CloudClient
from airbyte_api import models


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


@pytest.mark.parametrize(
    ("permissions", "expected_workspace_id"),
    [
        ([{"workspaceId": "direct-workspace"}], "direct-workspace"),
        (
            [{"workspaceId": "workspace-1"}, {"workspaceId": "workspace-2"}],
            None,
        ),
    ],
)
def test_resolve_default_workspace_id_uses_exactly_one_direct_grant(
    permissions: list[dict[str, object]],
    expected_workspace_id: str | None,
) -> None:
    patches = _api_patches(user={"userId": "user-id"}, permissions=permissions)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        assert CloudClient(bearer_token="token").resolve_default_workspace_id() == (
            expected_workspace_id
        )


def test_resolve_default_workspace_id_ignores_permission_lookup_failure() -> None:
    patches = _api_patches(user={"userId": "user-id"})
    with patches[0], patches[1], patches[2], patches[3], patches[4] as permissions:
        permissions.side_effect = exc.AirbyteError(message="Permission lookup failed.")
        assert CloudClient(bearer_token="token").resolve_default_workspace_id() is None


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


def test_list_workspaces_uses_direct_grants_when_memberships_are_ambiguous() -> None:
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {
                "permissionType": "organization_member",
                "organizationId": "organization-1",
            },
            {
                "permissionType": "organization_member",
                "organizationId": "organization-2",
            },
            {"permissionType": "workspace_admin", "workspaceId": "workspace-1"},
            {"permissionType": "workspace_admin", "workspaceId": "workspace-2"},
        ],
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "airbyte._util.api_util.get_workspace",
            side_effect=[
                models.WorkspaceResponse(
                    data_residency="auto",
                    name="Workspace 1",
                    notifications=models.NotificationsConfig(),
                    workspace_id="workspace-1",
                ),
                models.WorkspaceResponse(
                    data_residency="auto",
                    name="Workspace 2",
                    notifications=models.NotificationsConfig(),
                    workspace_id="workspace-2",
                ),
            ],
        ) as get_workspace,
        patch(
            "airbyte._util.api_util.list_workspaces_in_organization",
        ) as list_by_organization,
        patch(
            "airbyte._util.api_util.get_organization_info",
            return_value={"organizationName": "Organization"},
        ),
    ):
        workspaces = CloudClient(bearer_token="token").list_workspaces()

    assert [workspace.workspace_id for workspace in workspaces] == [
        "workspace-1",
        "workspace-2",
    ]
    get_workspace.assert_called()
    list_by_organization.assert_not_called()


def test_list_workspaces_rejects_unscoped_instance_admin_discovery() -> None:
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[{"permissionType": "instance_admin"}],
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        pytest.raises(exc.PyAirbyteInputError, match="instance administrator"),
    ):
        CloudClient(bearer_token="token").list_workspaces()


def test_get_default_context_is_bounded_to_permission_derived_scope() -> None:
    permissions = [
        {"permissionType": "instance_admin"},
        {"permissionType": "organization_admin", "organizationId": "organization-1"},
        {"permissionType": "organization_admin", "organizationId": "organization-2"},
        *[
            {"permissionType": "workspace_admin", "workspaceId": f"workspace-{index}"}
            for index in range(1, 7)
        ],
    ]
    patches = _api_patches(user={"userId": "user-id"}, permissions=permissions)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "airbyte._util.api_util.get_organization_info",
            side_effect=[
                {"organizationName": "Organization 1"},
                {"organizationName": "Organization 2"},
            ],
        ),
        patch(
            "airbyte._util.api_util.get_workspace",
            side_effect=[
                models.WorkspaceResponse(
                    data_residency="auto",
                    name=f"Workspace {index}",
                    notifications=models.NotificationsConfig(),
                    workspace_id=f"workspace-{index}",
                )
                for index in range(1, 7)
            ],
        ),
    ):
        context = CloudClient(bearer_token="token").get_default_context()

    assert context.user_id == "user-id"
    assert context.is_instance_admin is True
    assert context.default_workspace_id is None
    assert [item.organization_id for item in context.membership_organizations] == [
        "organization-1",
        "organization-2",
    ]
    assert [item.workspace_id for item in context.direct_workspaces] == [
        f"workspace-{index}" for index in range(1, 7)
    ]


def test_get_default_context_degrades_without_token_identity() -> None:
    patches = _api_patches(user={"userId": "user-id"})
    with (
        patches[0],
        patches[1] as get_user_id,
        patches[2],
        patches[3],
        patches[4],
    ):
        get_user_id.side_effect = exc.PyAirbyteInputError(
            message="The bearer token does not contain a user_id or sub claim."
        )
        context = CloudClient(bearer_token="token").get_default_context()

    assert context.user_id is None
    assert any("no user identity claim" in note for note in context.resolution_notes)
