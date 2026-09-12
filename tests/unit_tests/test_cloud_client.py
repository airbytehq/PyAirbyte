# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from unittest.mock import patch

import pytest

from airbyte import exceptions as exc
from airbyte.cloud.client import CloudClient
from airbyte.cloud.models import WorkspacePrivilegeScope
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
            return_value={
                "organizationId": parent_organization_id,
                "organizationName": "Organization",
            },
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
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "airbyte._util.api_util.get_workspace",
            return_value=models.WorkspaceResponse(
                data_residency="auto",
                name="Workspace",
                notifications=models.NotificationsConfig(),
                workspace_id="direct-workspace",
            ),
        ),
    ):
        assert CloudClient(bearer_token="token").resolve_default_workspace_id() == (
            expected_workspace_id
        )


def test_resolve_default_workspace_id_skips_stale_grants() -> None:
    permissions = [
        {"permissionType": "workspace_admin", "workspaceId": "stale-1"},
        {"permissionType": "workspace_admin", "workspaceId": "live-workspace"},
        {"permissionType": "workspace_admin", "workspaceId": "stale-2"},
    ]
    patches = _api_patches(user={"userId": "user-id"}, permissions=permissions)
    stale_error = exc.AirbyteMissingResourceError(
        resource_type="workspace",
        resource_name_or_id="stale-workspace",
    )

    def get_workspace(
        workspace_id: str,
        **kwargs: object,
    ) -> models.WorkspaceResponse:
        if workspace_id == "live-workspace":
            return models.WorkspaceResponse(
                data_residency="auto",
                name="Live Workspace",
                notifications=models.NotificationsConfig(),
                workspace_id="live-workspace",
            )
        raise stale_error

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as get_workspace_organization_info,
        patches[4],
        patch("airbyte._util.api_util.get_workspace", side_effect=get_workspace),
    ):
        get_workspace_organization_info.return_value = {
            "organizationId": "org-1",
            "organizationName": "Org One",
        }
        client = CloudClient(bearer_token="token")

        assert client.resolve_default_workspace_id() == "live-workspace"
        context = client.get_default_context_for_user()

    assert context.default_workspace_id == "live-workspace"
    assert context.default_workspace_name == "Live Workspace"
    assert context.default_workspace_verified is True
    assert context.default_organization_id == "org-1"
    assert context.default_organization_name == "Org One"
    assert [item.organization_id for item in context.member_organizations] == []
    assert context.member_organizations_truncated is False
    assert [workspace.workspace_id for workspace in context.member_workspaces] == [
        "live-workspace"
    ]
    assert context.member_workspaces[0].organization_id == "org-1"
    assert context.member_workspaces[0].organization_name == "Org One"
    get_workspace_organization_info.assert_called_once_with(
        workspace_id="live-workspace",
        api_root=client.public_api_root,
        config_api_root=client.config_api_root,
        client_id=client.client_id,
        client_secret=client.client_secret,
        bearer_token=client.bearer_token,
    )


def test_direct_workspace_validation_is_capped() -> None:
    permissions = [
        {"permissionType": "workspace_admin", "workspaceId": f"workspace-{index}"}
        for index in range(26)
    ]
    patches = _api_patches(user={"userId": "user-id"}, permissions=permissions)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as get_workspace_organization_info,
        patches[4],
        patch(
            "airbyte._util.api_util.get_workspace",
            side_effect=[
                models.WorkspaceResponse(
                    data_residency="auto",
                    name=f"Workspace {index}",
                    notifications=models.NotificationsConfig(),
                    workspace_id=f"workspace-{index}",
                )
                for index in range(26)
            ],
        ) as get_workspace,
    ):
        client = CloudClient(bearer_token="token")
        assert client.resolve_default_workspace_id() is None

        workspaces = client.list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.MEMBER_OF
        )
        assert [workspace.workspace_id for workspace in workspaces] == [
            f"workspace-{index}" for index in range(26)
        ]
        assert get_workspace.call_count == 26
        assert get_workspace_organization_info.call_count == 25

        limited_workspaces = client.list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.MEMBER_OF,
            limit=3,
        )
        assert len(limited_workspaces) == 3

        context = client.get_default_context_for_user()

    assert len(context.member_workspaces) == 25
    assert context.member_workspaces_truncated is True
    assert context.unvalidated_workspace_count == 1
    assert get_workspace.call_count == 26
    assert get_workspace_organization_info.call_count == 25


def test_workspace_organization_failure_is_cached() -> None:
    patches = _api_patches(user={"userId": "user-id"})
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as get_workspace_organization_info,
        patches[4],
    ):
        get_workspace_organization_info.side_effect = exc.AirbyteError(
            message="Organization lookup failed."
        )
        client = CloudClient(bearer_token="token")

        assert client._get_workspace_organization("ws-1") is None
        assert client._get_workspace_organization("ws-1") is None

    get_workspace_organization_info.assert_called_once()


def test_default_context_resolves_workspace_when_organization_lookup_fails() -> None:
    permissions = [{"permissionType": "workspace_admin", "workspaceId": "workspace-1"}]
    patches = _api_patches(user={"userId": "user-id"}, permissions=permissions)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as get_workspace_organization_info,
        patches[4],
        patch(
            "airbyte._util.api_util.get_workspace",
            return_value=models.WorkspaceResponse(
                data_residency="auto",
                name="Workspace 1",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-1",
            ),
        ),
    ):
        get_workspace_organization_info.side_effect = exc.AirbyteError(
            message="Organization lookup failed."
        )
        client = CloudClient(bearer_token="token")

        assert client.resolve_default_workspace_id() == "workspace-1"
        context = client.get_default_context_for_user()

    assert context.default_workspace_id == "workspace-1"
    assert context.default_workspace_name == "Workspace 1"
    assert context.default_workspace_verified is True
    assert context.default_organization_id is None
    assert context.default_organization_name is None
    assert context.member_organizations == []
    assert [workspace.workspace_id for workspace in context.member_workspaces] == [
        "workspace-1"
    ]
    assert context.member_workspaces[0].organization_id is None
    assert context.member_workspaces[0].organization_name is None


def test_default_context_enriches_configured_workspace() -> None:
    patches = _api_patches(user={"userId": "user-id"})
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as get_workspace_organization_info,
        patches[4],
        patch(
            "airbyte._util.api_util.get_workspace",
            return_value=models.WorkspaceResponse(
                data_residency="auto",
                name="Configured",
                notifications=models.NotificationsConfig(),
                workspace_id="configured-workspace",
            ),
        ),
    ):
        get_workspace_organization_info.return_value = {
            "organizationId": "org-1",
            "organizationName": "Org One",
        }
        client = CloudClient(
            bearer_token="token",
            workspace_id="configured-workspace",
        )
        context = client.get_default_context_for_user()

    assert context.default_workspace_id == "configured-workspace"
    assert context.default_workspace_name == "Configured"
    assert context.default_workspace_verified is True
    assert context.default_organization_id == "org-1"
    assert context.default_organization_name == "Org One"
    assert [item.organization_id for item in context.member_organizations] == []
    get_workspace_organization_info.assert_called_once_with(
        workspace_id="configured-workspace",
        api_root=client.public_api_root,
        config_api_root=client.config_api_root,
        client_id=client.client_id,
        client_secret=client.client_secret,
        bearer_token=client.bearer_token,
    )


def test_resolve_default_workspace_id_ignores_permission_lookup_failure() -> None:
    patches = _api_patches(user={"userId": "user-id"})
    with patches[0], patches[1], patches[2], patches[3], patches[4] as permissions:
        permissions.side_effect = exc.AirbyteError(message="Permission lookup failed.")
        assert CloudClient(bearer_token="token").resolve_default_workspace_id() is None


def test_stale_direct_workspace_grant_is_ignored_consistently() -> None:
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "stale-workspace"}
        ],
    )
    stale_error = exc.AirbyteMissingResourceError(
        resource_type="workspace",
        resource_name_or_id="stale-workspace",
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch("airbyte._util.api_util.get_workspace", side_effect=stale_error),
    ):
        client = CloudClient(bearer_token="token")

        assert client.resolve_default_workspace_id() is None
        assert (
            client.list_workspaces(privilege_scope=WorkspacePrivilegeScope.MEMBER_OF)
            == []
        )
        context = client.get_default_context_for_user()

    assert context.default_workspace_id is None
    assert context.member_workspaces == []
    assert context.member_workspaces_truncated is False


def test_list_workspaces_skips_stale_grant_before_valid_grant_with_limit() -> None:
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "stale-workspace"},
            {"permissionType": "workspace_admin", "workspaceId": "valid-workspace"},
        ],
    )
    stale_error = exc.AirbyteMissingResourceError(
        resource_type="workspace",
        resource_name_or_id="stale-workspace",
    )
    valid_workspace = models.WorkspaceResponse(
        data_residency="auto",
        name="Valid workspace",
        notifications=models.NotificationsConfig(),
        workspace_id="valid-workspace",
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "airbyte._util.api_util.get_workspace",
            side_effect=[stale_error, valid_workspace],
        ) as get_workspace,
    ):
        workspaces = CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.MEMBER_OF,
            limit=1,
        )

    assert [workspace.workspace_id for workspace in workspaces] == ["valid-workspace"]
    assert get_workspace.call_count == 2


def test_list_workspaces_propagates_non_not_found_workspace_error() -> None:
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-id"}
        ],
    )
    api_error = exc.AirbyteError(message="Workspace lookup failed.")
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch("airbyte._util.api_util.get_workspace", side_effect=api_error),
        pytest.raises(exc.AirbyteError, match="Workspace lookup failed"),
    ):
        CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.MEMBER_OF
        )


def test_list_workspaces_defaults_to_direct_memberships() -> None:
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": f"workspace-{index}"}
            for index in range(1, 4)
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
            return_value=models.WorkspaceResponse(
                data_residency="auto",
                name="Workspace 1",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-1",
            ),
        ) as get_workspace,
    ):
        workspaces = CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.MEMBER_OF,
            limit=1,
        )

    assert [workspace.workspace_id for workspace in workspaces] == ["workspace-1"]
    assert get_workspace.call_count == 3


def test_list_workspaces_organization_admin_lists_all_member_organizations() -> None:
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {
                "permissionType": "organization_member",
                "organizationId": "organization-1",
            },
            {
                "permissionType": "organization_admin",
                "organizationId": "organization-2",
            },
        ],
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "airbyte._util.api_util.list_workspaces_in_organization",
            side_effect=[
                [{"workspaceId": "workspace-1", "name": "Workspace 1"}],
                [{"workspaceId": "workspace-2", "name": "Workspace 2"}],
            ],
        ) as list_workspaces_in_organization,
    ):
        workspaces = CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.ORGANIZATION_ADMIN
        )

    assert [workspace.workspace_id for workspace in workspaces] == [
        "workspace-1",
        "workspace-2",
    ]
    assert [
        call.kwargs["organization_id"]
        for call in list_workspaces_in_organization.call_args_list
    ] == ["organization-1", "organization-2"]


def test_list_workspaces_instance_admin_scope_requires_instance_admin() -> None:
    patches = _api_patches(user={"userId": "user-id"}, permissions=[])
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        pytest.raises(
            exc.PyAirbyteInputError,
            match="privilege_scope=instance_admin requires the instance_admin permission",
        ),
    ):
        CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.INSTANCE_ADMIN
        )


def test_list_workspaces_any_scope_uses_unscoped_listing_for_instance_admin() -> None:
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
        patch(
            "airbyte._util.api_util.list_workspaces",
            return_value=[],
        ) as list_workspaces,
    ):
        CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.ANY
        )

    list_workspaces.assert_called_once()


def test_list_workspaces_any_scope_fails_closed_when_permissions_cannot_be_loaded() -> (
    None
):
    patches = _api_patches(user={"userId": "user-id"})
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4] as list_permissions,
        patch("airbyte._util.api_util.list_workspaces") as list_workspaces,
        pytest.raises(exc.AirbyteError, match="Permission lookup failed"),
    ):
        list_permissions.side_effect = exc.AirbyteError(
            message="Permission lookup failed"
        )
        CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.ANY
        )

    list_workspaces.assert_not_called()


def test_list_workspaces_any_scope_uses_member_organizations_without_instance_admin() -> (
    None
):
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {
                "permissionType": "organization_member",
                "organizationId": "organization-1",
            }
        ],
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "airbyte._util.api_util.list_workspaces_in_organization",
            return_value=[{"workspaceId": "workspace-1", "name": "Workspace 1"}],
        ) as list_workspaces_in_organization,
    ):
        workspaces = CloudClient(bearer_token="token").list_workspaces(
            privilege_scope=WorkspacePrivilegeScope.ANY
        )

    assert [workspace.workspace_id for workspace in workspaces] == ["workspace-1"]
    list_workspaces_in_organization.assert_called_once()


def test_list_workspaces_explicit_organization_ignores_privilege_scope() -> None:
    with patch(
        "airbyte._util.api_util.list_workspaces_in_organization",
        return_value=[{"workspaceId": "workspace-1", "name": "Workspace 1"}],
    ) as list_workspaces_in_organization:
        workspaces = CloudClient(bearer_token="token").list_workspaces(
            organization_id="organization-id",
            privilege_scope=WorkspacePrivilegeScope.MEMBER_OF,
        )

    assert [workspace.workspace_id for workspace in workspaces] == ["workspace-1"]
    list_workspaces_in_organization.assert_called_once()


def test_list_workspaces_all_organizations_alias_warns_and_maps_to_any() -> None:
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
        patch("airbyte._util.api_util.list_workspaces", return_value=[]),
        pytest.warns(DeprecationWarning, match="all_organizations"),
    ):
        CloudClient(bearer_token="token").list_workspaces(all_organizations=True)


def test_list_workspaces_all_organizations_alias_conflicts_with_scope() -> None:
    with pytest.raises(exc.PyAirbyteInputError, match="privilege_scope"):
        CloudClient(bearer_token="token").list_workspaces(
            all_organizations=True,
            privilege_scope=WorkspacePrivilegeScope.INSTANCE_ADMIN,
        )


def test_list_workspaces_explicit_workspace_resolution_does_not_use_member_fallback() -> (
    None
):
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-1"}
        ],
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as get_workspace_organization_info,
        patches[4],
        patch("airbyte._util.api_util.get_workspace") as get_workspace,
        pytest.raises(exc.PyAirbyteInputError),
    ):
        get_workspace_organization_info.side_effect = exc.PyAirbyteInputError(
            message="Workspace organization is ambiguous."
        )
        CloudClient(bearer_token="token").list_workspaces(
            workspace_id="unknown-workspace"
        )

    get_workspace.assert_not_called()


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


def test_get_default_context_for_user_is_bounded_to_permission_derived_scope() -> None:
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
        context = CloudClient(bearer_token="token").get_default_context_for_user()

    assert context.user_id == "user-id"
    assert context.default_workspace_id is None
    assert [item.organization_id for item in context.member_organizations] == [
        "organization-1",
        "organization-2",
    ]
    assert [item.workspace_id for item in context.member_workspaces] == [
        f"workspace-{index}" for index in range(1, 7)
    ]
    assert context.member_organizations_truncated is False
    assert context.member_workspaces_truncated is False
    assert len(context.discovery_hints) == 3
    assert any(
        "set_default_cloud_workspace" in hint for hint in context.discovery_hints
    )


def test_get_default_context_for_user_truncates_organization_memberships() -> None:
    permissions = [
        {
            "permissionType": "organization_member",
            "organizationId": f"organization-{index}",
        }
        for index in range(1, 12)
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
                {"organizationName": f"Organization {index}"} for index in range(1, 11)
            ],
        ),
    ):
        context = CloudClient(bearer_token="token").get_default_context_for_user()

    assert len(context.member_organizations) == 10
    assert context.member_organizations_truncated is True
    assert context.member_workspaces_truncated is False


def test_get_default_context_for_user_truncates_workspace_memberships() -> None:
    permissions = [
        {"permissionType": "workspace_admin", "workspaceId": f"workspace-{index}"}
        for index in range(1, 27)
    ]
    patches = _api_patches(user={"userId": "user-id"}, permissions=permissions)
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
                    name=f"Workspace {index}",
                    notifications=models.NotificationsConfig(),
                    workspace_id=f"workspace-{index}",
                )
                for index in range(1, 26)
            ],
        ) as get_workspace,
    ):
        context = CloudClient(bearer_token="token").get_default_context_for_user()

    assert len(context.member_workspaces) == 25
    assert context.member_organizations_truncated is False
    assert context.member_workspaces_truncated is True
    assert get_workspace.call_count == 25


def test_get_default_context_for_user_degrades_without_token_identity() -> None:
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
        context = CloudClient(bearer_token="token").get_default_context_for_user()

    assert context.user_id is None
    assert context.discovery_hints == []


def test_get_default_context_for_user_hints_setter_when_stored_default_missing() -> (
    None
):
    """An inferred single-grant workspace is not a stored default; still hint."""
    patches = _api_patches(
        user={"userId": "user-id"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-1"}
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
            return_value=models.WorkspaceResponse(
                data_residency="auto",
                name="Workspace 1",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-1",
            ),
        ),
    ):
        context = CloudClient(bearer_token="token").get_default_context_for_user()

    assert context.default_workspace_id == "workspace-1"
    assert context.discovery_hints == [
        "No default workspace is set. Use "
        "set_default_cloud_workspace(user_email=<your email>, "
        "workspace_id=<id>) to durably set one; it applies to both MCP "
        "sessions and the Airbyte Cloud web app."
    ]


def test_get_default_context_for_user_omits_setter_hint_when_default_stored() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "defaultWorkspaceId": "workspace-1"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-1"}
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
            return_value=models.WorkspaceResponse(
                data_residency="auto",
                name="Workspace 1",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-1",
            ),
        ),
    ):
        context = CloudClient(bearer_token="token").get_default_context_for_user()

    assert context.default_workspace_id == "workspace-1"
    assert context.discovery_hints == []


def _set_default_workspace_patches(
    *,
    workspace: dict[str, object] | BaseException = ...,
    organizations: list[dict[str, object]] | None = None,
    updated_user: dict[str, object] | None = None,
):
    """Patches for the API calls made by `set_default_workspace_for_user`."""
    workspace_patch = (
        patch(
            "airbyte._util.api_util.get_workspace_config_api",
            side_effect=workspace,
        )
        if isinstance(workspace, BaseException)
        else patch(
            "airbyte._util.api_util.get_workspace_config_api",
            return_value=(
                {
                    "workspaceId": "workspace-id",
                    "name": "Workspace",
                    "organizationId": "organization-id",
                    "tombstone": False,
                }
                if workspace is ...
                else workspace
            ),
        )
    )
    return (
        workspace_patch,
        patch(
            "airbyte._util.api_util.list_organizations_for_user_id",
            return_value=(
                [{"organizationId": "organization-id"}]
                if organizations is None
                else organizations
            ),
        ),
        patch(
            "airbyte._util.api_util.update_user_default_workspace",
            return_value=(
                {"userId": "user-id", "defaultWorkspaceId": "workspace-id"}
                if updated_user is None
                else updated_user
            ),
        ),
    )


def test_set_default_workspace_for_user_with_direct_grant() -> None:
    patches = _api_patches(
        user={
            "userId": "user-id",
            "email": "user@example.com",
            "defaultWorkspaceId": "old-workspace",
        },
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-id"}
        ],
    )
    extra = _set_default_workspace_patches()
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0] as get_workspace,
        extra[1],
        extra[2] as update_user,
    ):
        result = CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="user@example.com",
            workspace_id="workspace-id",
        )

    assert result.user_id == "user-id"
    assert result.user_email == "user@example.com"
    assert result.previous_default_workspace_id == "old-workspace"
    assert result.default_workspace_id == "workspace-id"
    assert result.default_workspace_name == "Workspace"
    assert result.organization_id == "organization-id"
    assert result.organization_name == "Organization"
    assert result.membership_basis == "workspace"
    get_workspace.assert_called_once()
    update_user.assert_called_once()
    assert update_user.call_args.args == ("user-id", "workspace-id")


def test_set_default_workspace_for_user_with_organization_grant() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "email": "user@example.com"},
        permissions=[
            {
                "permissionType": "organization_member",
                "organizationId": "organization-id",
            }
        ],
    )
    extra = _set_default_workspace_patches()
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0],
        extra[1],
        extra[2],
    ):
        result = CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email=" User@Example.com ",
            workspace_id="workspace-id",
        )

    assert result.default_workspace_id == "workspace-id"
    assert result.membership_basis == "organization"


def test_set_default_workspace_for_user_rejects_email_mismatch() -> None:
    patches = _api_patches(
        user={
            "userId": "user-id",
            "email": "user@example.com",
            "name": "User",
        }
    )
    extra = _set_default_workspace_patches()
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0] as get_workspace,
        extra[1],
        extra[2] as update_user,
        pytest.raises(exc.PyAirbyteInputError) as exc_info,
    ):
        CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="other@example.com",
            workspace_id="workspace-id",
        )

    assert "does not match the authenticated user" in str(exc_info.value)
    assert "user@example.com" in str(exc_info.value.context)
    assert "get_default_cloud_context" in str(exc_info.value.guidance)
    get_workspace.assert_not_called()
    update_user.assert_not_called()


def test_set_default_workspace_for_user_rejects_disabled_user() -> None:
    patches = _api_patches(
        user={
            "userId": "user-id",
            "email": "user@example.com",
            "status": "disabled",
        }
    )
    extra = _set_default_workspace_patches()
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0] as get_workspace,
        extra[1],
        extra[2] as update_user,
        pytest.raises(exc.PyAirbyteInputError, match="disabled"),
    ):
        CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="user@example.com",
            workspace_id="workspace-id",
        )

    get_workspace.assert_not_called()
    update_user.assert_not_called()


def test_set_default_workspace_for_user_rejects_missing_workspace() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "email": "user@example.com"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-id"}
        ],
    )
    extra = _set_default_workspace_patches(
        workspace=exc.AirbyteMissingResourceError(
            resource_type="workspace",
            resource_name_or_id="workspace-id",
        )
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0],
        extra[1],
        extra[2] as update_user,
        pytest.raises(exc.PyAirbyteInputError, match="not found"),
    ):
        CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="user@example.com",
            workspace_id="workspace-id",
        )

    update_user.assert_not_called()


def test_set_default_workspace_for_user_rejects_tombstoned_workspace() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "email": "user@example.com"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-id"}
        ],
    )
    extra = _set_default_workspace_patches(
        workspace={
            "workspaceId": "workspace-id",
            "name": "Deleted workspace",
            "organizationId": "organization-id",
            "tombstone": True,
        }
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0],
        extra[1],
        extra[2] as update_user,
        pytest.raises(exc.PyAirbyteInputError, match="tombstoned"),
    ):
        CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="user@example.com",
            workspace_id="workspace-id",
        )

    update_user.assert_not_called()


def test_set_default_workspace_for_user_rejects_instance_admin_only() -> None:
    """Instance-admin access alone must not count as membership."""
    patches = _api_patches(
        user={"userId": "user-id", "email": "user@example.com"},
        permissions=[{"permissionType": "instance_admin"}],
    )
    extra = _set_default_workspace_patches()
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0],
        extra[1],
        extra[2] as update_user,
        pytest.raises(exc.PyAirbyteInputError, match="not an explicit member"),
    ):
        CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="user@example.com",
            workspace_id="workspace-id",
        )

    update_user.assert_not_called()


def test_set_default_workspace_for_user_rejects_tombstoned_organization() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "email": "user@example.com"},
        permissions=[
            {
                "permissionType": "organization_member",
                "organizationId": "organization-id",
            }
        ],
    )
    extra = _set_default_workspace_patches(
        organizations=[{"organizationId": "other-organization"}]
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0],
        extra[1],
        extra[2] as update_user,
        pytest.raises(exc.PyAirbyteInputError, match="tombstoned or no longer"),
    ):
        CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="user@example.com",
            workspace_id="workspace-id",
        )

    update_user.assert_not_called()


def test_set_default_workspace_for_user_fails_when_update_does_not_persist() -> None:
    patches = _api_patches(
        user={"userId": "user-id", "email": "user@example.com"},
        permissions=[
            {"permissionType": "workspace_admin", "workspaceId": "workspace-id"}
        ],
    )
    extra = _set_default_workspace_patches(
        updated_user={"userId": "user-id", "defaultWorkspaceId": "other-workspace"}
    )
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        extra[0],
        extra[1],
        extra[2],
        pytest.raises(exc.AirbyteError, match="did not persist"),
    ):
        CloudClient(bearer_token="token").set_default_workspace_for_user(
            user_email="user@example.com",
            workspace_id="workspace-id",
        )
