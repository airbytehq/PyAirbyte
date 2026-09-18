# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import os
from collections.abc import Callable
from typing import NoReturn

import pytest
import requests
from pydantic import ValidationError
from airbyte_api import models

from airbyte import constants
from airbyte._util import api_util
from airbyte.cloud import _credentials as cloud_credentials
from airbyte.cloud.client import CloudClient
from airbyte.cloud.connectors import (
    CloudDestination,
    CloudSource,
    ConnectorFeature,
    ConnectorType,
)
from airbyte.cloud.models import (
    SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS,
    CloudDestinationInfo,
    CloudSourceInfo,
    CloudWorkspaceInfo,
    WorkspacePrivilegeScope,
)
from airbyte.cloud import organizations as cloud_organizations
from airbyte.cloud.organizations import CloudOrganization
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteError,
    AirbyteMissingResourceError,
    PyAirbyteInputError,
)
from airbyte.mcp import cloud as mcp_cloud
from airbyte.secrets.base import SecretString


def _raise(error: Exception) -> Callable[..., NoReturn]:
    def _raiser(*_args: object, **_kwargs: object) -> NoReturn:
        raise error

    return _raiser


def _stub_organization_features(
    monkeypatch: pytest.MonkeyPatch, *, enabled: bool = False
) -> None:
    """Answer organization feature flags without touching the Context layer API."""
    monkeypatch.setattr(
        cloud_organizations.deployment, "is_agents_api_available", lambda **_: enabled
    )
    monkeypatch.setattr(
        cloud_organizations.agents_api_util,
        "list_agent_workspaces",
        lambda **_: [],
    )


def _patch_workspace_discovery(
    monkeypatch: pytest.MonkeyPatch,
    *,
    permissions: list[dict[str, object]] | None = None,
    parent_organization_id: str | None = None,
    org_scoped_result: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        api_util,
        "get_user_id_from_bearer_token",
        lambda _: "auth-user-id",
    )
    monkeypatch.setattr(
        api_util,
        "get_user_by_auth_id",
        lambda *_, **__: {"userId": "user-id"},
    )
    monkeypatch.setattr(
        api_util,
        "list_permissions_for_user",
        lambda *_, **__: permissions or [],
    )

    def fake_get_workspace_organization_info(**_: object) -> dict[str, object]:
        if parent_organization_id is None:
            pytest.fail("workspace parent lookup should not be called")
        return {"organizationId": parent_organization_id}

    monkeypatch.setattr(
        api_util,
        "get_workspace_organization_info",
        fake_get_workspace_organization_info,
    )
    monkeypatch.setattr(
        api_util,
        "list_workspaces",
        lambda **_: pytest.fail("cross-organization lookup should not be called"),
    )

    def fake_list_workspaces_in_organization(
        **kwargs: object,
    ) -> list[dict[str, object]]:
        captured.update(kwargs)
        return org_scoped_result if org_scoped_result is not None else []

    monkeypatch.setattr(
        api_util,
        "list_workspaces_in_organization",
        fake_list_workspaces_in_organization,
    )
    return captured


def test_airbyte_credentials_from_auth_uses_pyairbyte_secret_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = {
        constants.CLOUD_BEARER_TOKEN_ENV_VAR: SecretString("test-bearer-token"),
        constants.CLOUD_WORKSPACE_ID_ENV_VAR: SecretString("test-workspace-id"),
    }

    def fake_try_get_secret(
        secret_name: str,
        /,
        *,
        default: str | SecretString | None = None,
        **_: object,
    ) -> SecretString | str | None:
        return secrets.get(secret_name, default)

    monkeypatch.setattr(cloud_credentials, "try_get_secret", fake_try_get_secret)

    credentials = cloud_credentials._AirbyteCredentials.from_auth(env_vars=True)

    assert credentials.bearer_token == "test-bearer-token"
    assert credentials.workspace_id == "test-workspace-id"


def test_airbyte_credentials_from_auth_defaults_to_env_var_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = {
        constants.CLOUD_BEARER_TOKEN_ENV_VAR: SecretString("test-bearer-token"),
    }

    def fake_try_get_secret(
        secret_name: str,
        /,
        *,
        default: str | SecretString | None = None,
        **_: object,
    ) -> SecretString | str | None:
        return secrets.get(secret_name, default)

    monkeypatch.setattr(cloud_credentials, "try_get_secret", fake_try_get_secret)

    credentials = cloud_credentials._AirbyteCredentials.from_auth()

    assert credentials.bearer_token == "test-bearer-token"


def test_airbyte_credentials_from_auth_ignores_legacy_api_root_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy_public_api_root_env_var = "AIRBYTE_API_ROOT"
    legacy_config_api_root_env_var = "AIRBYTE_CONFIG_API_ROOT"
    for env_var in (
        constants.CLOUD_API_ROOT_ENV_VAR,
        constants.CLOUD_CONFIG_API_ROOT_ENV_VAR,
        constants.CLOUD_BEARER_TOKEN_ENV_VAR,
        constants.CLOUD_CLIENT_ID_ENV_VAR,
        constants.CLOUD_CLIENT_SECRET_ENV_VAR,
        cloud_credentials.BEARER_TOKEN_ENV_VAR,
        cloud_credentials.CLIENT_ID_ENV_VAR,
        cloud_credentials.CLIENT_SECRET_ENV_VAR,
    ):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setenv(constants.CLOUD_BEARER_TOKEN_ENV_VAR, "test-bearer-token")
    monkeypatch.setenv(
        legacy_public_api_root_env_var, "http://legacy.example.com/api/public/v1"
    )
    monkeypatch.setenv(
        legacy_config_api_root_env_var, "http://legacy.example.com/api/v1"
    )

    def fake_try_get_secret(
        secret_name: str,
        /,
        *,
        default: str | SecretString | None = None,
        **_: object,
    ) -> SecretString | str | None:
        return os.environ.get(secret_name, default)

    monkeypatch.setattr(cloud_credentials, "try_get_secret", fake_try_get_secret)

    credentials = cloud_credentials._AirbyteCredentials.from_auth(env_vars=True)

    assert credentials.public_api_root == constants.CLOUD_API_ROOT
    assert credentials.config_api_root is None

    monkeypatch.setenv(
        constants.CLOUD_API_ROOT_ENV_VAR,
        "https://example.airbyte.com/api/public/v1",
    )
    monkeypatch.setenv(
        constants.CLOUD_CONFIG_API_ROOT_ENV_VAR,
        "https://example.airbyte.com/api/v1",
    )
    credentials = cloud_credentials._AirbyteCredentials.from_auth(env_vars=True)

    assert credentials.public_api_root == "https://example.airbyte.com/api/public/v1"
    assert credentials.config_api_root == "https://example.airbyte.com/api/v1"


@pytest.mark.parametrize(
    "env_vars, expected_guidance",
    [
        pytest.param(
            False,
            "Provide `bearer_token`, or both `client_id` and `client_secret`.",
            id="explicit_inputs",
        ),
        pytest.param(
            True,
            (
                "Provide `bearer_token`, or both `client_id` and `client_secret`, as "
                "arguments or via the `AIRBYTE_CLOUD_BEARER_TOKEN`, "
                "`AIRBYTE_CLOUD_CLIENT_ID`, and `AIRBYTE_CLOUD_CLIENT_SECRET` "
                "environment variables."
            ),
            id="env_vars",
        ),
    ],
)
def test_airbyte_credentials_missing_credentials_guidance_matches_resolution_mode(
    monkeypatch: pytest.MonkeyPatch,
    env_vars: bool,
    expected_guidance: str,
) -> None:
    monkeypatch.setattr(cloud_credentials, "try_get_secret", lambda *_, **__: None)

    with pytest.raises(PyAirbyteInputError) as exc_info:
        cloud_credentials._AirbyteCredentials.from_auth(env_vars=env_vars)

    assert exc_info.value.guidance == expected_guidance


def test_airbyte_credentials_rejects_mixed_auth_methods() -> None:
    with pytest.raises(PyAirbyteInputError, match="Cannot use both"):
        cloud_credentials._AirbyteCredentials.from_auth(
            bearer_token="token",
            client_id="client-id",
            client_secret="client-secret",
            env_vars=False,
        )


@pytest.mark.parametrize(
    "client_id, client_secret, bearer_token, expected_message",
    [
        pytest.param(
            "client-id",
            None,
            None,
            "Client ID and client secret are both required.",
            id="missing_client_secret",
        ),
        pytest.param(
            "client-id",
            "client-secret",
            "token",
            "Cannot use both client credentials and bearer token authentication.",
            id="mixed_auth_methods",
        ),
    ],
)
def test_cloud_client_init_validates_auth_inputs(
    client_id: str | None,
    client_secret: str | None,
    bearer_token: str | None,
    expected_message: str,
) -> None:
    with pytest.raises(PyAirbyteInputError, match=expected_message):
        CloudClient(
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )


def test_cloud_client_list_workspaces_forwards_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_limit = None

    def fake_list_workspaces(
        *,
        limit: int | None = None,
        **_: object,
    ) -> list[object]:
        nonlocal captured_limit
        captured_limit = limit
        return []

    monkeypatch.setattr(api_util, "list_workspaces", fake_list_workspaces)

    client = CloudClient(bearer_token="token")
    monkeypatch.setattr(client, "_is_instance_admin", lambda: True)
    client.list_workspaces(
        limit=3,
        privilege_scope=WorkspacePrivilegeScope.INSTANCE_ADMIN,
    )

    assert captured_limit == 3


@pytest.mark.parametrize(
    ("request_kwargs", "expected_message"),
    [
        pytest.param(
            {"name_contains": "target", "name_filter": lambda _: True},
            "provide name_contains or name_filter, but not both",
            id="name-contains-with-name-filter",
        ),
        pytest.param(
            {"name": "target", "name_contains": "target"},
            "provide name or name_contains, but not both",
            id="name-with-name-contains",
        ),
    ],
)
def test_cloud_client_list_workspaces_rejects_invalid_argument_combinations(
    request_kwargs: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(PyAirbyteInputError, match=expected_message):
        CloudClient(bearer_token="token").list_workspaces(**request_kwargs)


def test_cloud_client_list_workspaces_applies_name_contains_to_all_org_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_list_workspaces(
        **kwargs: object,
    ) -> list[models.WorkspaceResponse]:
        captured.update(kwargs)
        workspaces = [
            models.WorkspaceResponse(
                data_residency="auto",
                name="target-one",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-target-one",
            ),
            models.WorkspaceResponse(
                data_residency="auto",
                name="other",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-other",
            ),
            models.WorkspaceResponse(
                data_residency="auto",
                name="target-two",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-target-two",
            ),
        ]
        workspace_filter = kwargs["name_filter"]
        assert callable(workspace_filter)
        matching_workspaces = [
            workspace for workspace in workspaces if workspace_filter(workspace.name)
        ]
        return matching_workspaces[
            : kwargs["limit"] if isinstance(kwargs["limit"], int) else None
        ]

    monkeypatch.setattr(api_util, "list_workspaces", fake_list_workspaces)

    client = CloudClient(bearer_token="token")
    monkeypatch.setattr(client, "_is_instance_admin", lambda: True)
    result = client.list_workspaces(
        name_contains="TARGET",
        limit=1,
        privilege_scope=WorkspacePrivilegeScope.INSTANCE_ADMIN,
    )

    assert captured.get("name") is None
    assert callable(captured["name_filter"])
    assert captured["limit"] == 1
    assert [workspace.name for workspace in result] == ["target-one"]


def test_cloud_client_list_workspaces_in_organization_applies_name_filter_before_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_limit = None

    def fake_list_workspaces_in_organization(
        *,
        limit: int | None = None,
        **_: object,
    ) -> list[dict[str, object]]:
        nonlocal captured_limit
        captured_limit = limit
        return [
            {"name": "miss", "workspaceId": "workspace-miss"},
            {"name": "target-one", "workspaceId": "workspace-target-one"},
            {"name": "target-two", "workspaceId": "workspace-target-two"},
        ]

    monkeypatch.setattr(
        api_util,
        "list_workspaces_in_organization",
        fake_list_workspaces_in_organization,
    )

    result = CloudClient(
        bearer_token="token",
        organization_id="organization-id",
    ).list_workspaces(
        organization_id="organization-id",
        name_filter=lambda name: name.startswith("target"),
        limit=1,
    )

    assert captured_limit is None
    assert all(isinstance(workspace, CloudWorkspaceInfo) for workspace in result)
    assert [workspace.name for workspace in result] == ["target-one"]
    assert [workspace.workspace_id for workspace in result] == ["workspace-target-one"]


def test_cloud_client_list_workspaces_matches_exact_name_after_server_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_list_workspaces_in_organization(
        **kwargs: object,
    ) -> list[dict[str, object]]:
        captured.update(kwargs)
        return [
            {"name": "Production-old", "workspaceId": "workspace-production-old"},
            {"name": "Prod", "workspaceId": "workspace-prod"},
        ]

    monkeypatch.setattr(
        api_util,
        "list_workspaces_in_organization",
        fake_list_workspaces_in_organization,
    )

    result = CloudClient(
        bearer_token="token",
        organization_id="organization-id",
    ).list_workspaces(
        organization_id="organization-id",
        name="Prod",
        limit=1,
    )

    assert captured["name_contains"] == "Prod"
    assert captured["limit"] is None
    assert [workspace.name for workspace in result] == ["Prod"]


def test_cloud_client_create_workspace_uses_default_organization_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_organization_id = None

    def fake_create_workspace(
        *,
        organization_id: str | None = None,
        **_: object,
    ) -> models.WorkspaceResponse:
        nonlocal captured_organization_id
        captured_organization_id = organization_id
        return models.WorkspaceResponse(
            data_residency="auto",
            name="New workspace",
            notifications=models.NotificationsConfig(),
            workspace_id="workspace-id",
        )

    monkeypatch.setattr(api_util, "create_workspace", fake_create_workspace)

    workspace = CloudClient(
        bearer_token="token",
        organization_id="organization-id",
    ).create_workspace(name="New workspace")

    assert isinstance(workspace, CloudWorkspaceInfo)
    assert workspace.workspace_id == "workspace-id"
    assert captured_organization_id == "organization-id"


def test_cloud_client_list_workspaces_accepts_api_notification_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_list_workspaces_in_organization(
        **_: object,
    ) -> list[dict[str, object]]:
        return [
            {
                "workspaceId": "workspace-id",
                "name": "Workspace",
                "notifications": [{"sendOnSuccess": True}],
            }
        ]

    monkeypatch.setattr(
        api_util,
        "list_workspaces_in_organization",
        fake_list_workspaces_in_organization,
    )

    workspaces = CloudClient(
        bearer_token="token",
        organization_id="organization-id",
    ).list_workspaces(organization_id="organization-id")

    assert len(workspaces) == 1
    assert workspaces[0].notifications == [{"sendOnSuccess": True}]


def test_cloud_workspace_info_accepts_api_notification_mapping() -> None:
    workspace = CloudWorkspaceInfo.model_validate({
        "workspaceId": "workspace-id",
        "name": "Workspace",
        "notifications": {"sendOnSuccess": True},
    })

    assert workspace.notifications == {"sendOnSuccess": True}


def test_cloud_client_rename_workspace_forwards_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, object] = {}

    def fake_rename_workspace(**kwargs: object) -> models.WorkspaceResponse:
        captured_kwargs.update(kwargs)
        return models.WorkspaceResponse(
            data_residency="auto",
            name="Renamed workspace",
            notifications=models.NotificationsConfig(),
            workspace_id="workspace-id",
        )

    monkeypatch.setattr(api_util, "rename_workspace", fake_rename_workspace)

    workspace = CloudClient(bearer_token="token").rename_workspace(
        workspace_id="workspace-id",
        name="Renamed workspace",
    )

    assert isinstance(workspace, CloudWorkspaceInfo)
    assert workspace.name == "Renamed workspace"
    assert captured_kwargs["workspace_id"] == "workspace-id"
    assert captured_kwargs["name"] == "Renamed workspace"


def test_cloud_client_permanently_delete_workspace_forwards_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, object] = {}

    def fake_permanently_delete_workspace(**kwargs: object) -> None:
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(
        api_util,
        "permanently_delete_workspace",
        fake_permanently_delete_workspace,
    )

    CloudClient(bearer_token="token").permanently_delete_workspace(
        workspace_id="workspace-id",
        workspace_name="delete-me workspace",
        safe_mode=True,
    )

    assert captured_kwargs["workspace_id"] == "workspace-id"
    assert captured_kwargs["workspace_name"] == "delete-me workspace"
    assert captured_kwargs["safe_mode"] is True


def test_cloud_workspace_list_workspaces_forwards_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_limit = None

    def fake_list_workspaces(
        *,
        limit: int | None = None,
        **_: object,
    ) -> list[object]:
        nonlocal captured_limit
        captured_limit = limit
        return []

    monkeypatch.setattr(api_util, "list_workspaces", fake_list_workspaces)

    CloudWorkspace(workspace_id="workspace-id", bearer_token="token").list_workspaces(
        limit=3
    )

    assert captured_limit == 3


def test_cloud_workspace_rename_forwards_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, object] = {}

    def fake_rename_workspace(**kwargs: object) -> models.WorkspaceResponse:
        captured_kwargs.update(kwargs)
        return models.WorkspaceResponse(
            data_residency="auto",
            name="Renamed workspace",
            notifications=models.NotificationsConfig(),
            workspace_id="workspace-id",
        )

    monkeypatch.setattr(api_util, "rename_workspace", fake_rename_workspace)

    workspace = CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
    )

    result = workspace.rename("Renamed workspace")

    assert result is workspace
    assert captured_kwargs["workspace_id"] == "workspace-id"
    assert captured_kwargs["name"] == "Renamed workspace"


def test_cloud_workspace_permanently_delete_forwards_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, object] = {}

    def fake_permanently_delete_workspace(**kwargs: object) -> None:
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(
        api_util,
        "permanently_delete_workspace",
        fake_permanently_delete_workspace,
    )

    CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
    ).permanently_delete(workspace_name="delete-me workspace")

    assert captured_kwargs["workspace_id"] == "workspace-id"
    assert captured_kwargs["workspace_name"] == "delete-me workspace"
    assert captured_kwargs["safe_mode"] is True


def test_cloud_workspace_explicit_credentials_do_not_resolve_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = {
        constants.CLOUD_BEARER_TOKEN_ENV_VAR: SecretString("env-bearer-token"),
    }

    def fake_try_get_secret(
        secret_name: str,
        /,
        *,
        default: str | SecretString | None = None,
        **_: object,
    ) -> SecretString | str | None:
        return secrets.get(secret_name, default)

    monkeypatch.setattr(cloud_credentials, "try_get_secret", fake_try_get_secret)

    workspace = CloudWorkspace(
        workspace_id="workspace-id",
        client_id="client-id",
        client_secret="client-secret",
    )

    assert workspace.client_id == "client-id"
    assert workspace.client_secret == "client-secret"
    assert workspace.bearer_token is None


def test_cloud_client_get_organization_adds_missing_lookup_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "get_organization_info",
        lambda **_: (_ for _ in ()).throw(AirbyteError(message="Unavailable")),
    )
    monkeypatch.setattr(api_util, "list_organizations_for_user", lambda **_: [])

    with pytest.raises(AirbyteMissingResourceError) as exc_info:
        CloudClient(bearer_token="token").get_organization(
            organization_id="missing-org"
        )

    assert exc_info.value.resource_name_or_id == "missing-org"


def test_cloud_client_get_organization_uses_default_organization_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user",
        lambda **_: [
            models.OrganizationResponse(
                organization_id="default-org",
                organization_name="Default Org",
                email="test@example.com",
            )
        ],
    )
    monkeypatch.setattr(
        api_util,
        "get_organization_info",
        lambda **_: {
            "organizationId": "default-org",
            "organizationName": "Default Org",
            "email": "test@example.com",
        },
    )

    organization = CloudClient(
        bearer_token="token",
        organization_id="default-org",
    ).get_organization()

    assert organization.organization_id == "default-org"


@pytest.mark.parametrize(
    ("client_kwargs", "parent_organization_id", "permissions", "expected_id"),
    [
        pytest.param(
            {"organization_id": "configured-org"},
            None,
            None,
            "configured-org",
            id="configured-organization",
        ),
        pytest.param(
            {"workspace_id": "configured-workspace"},
            "workspace-parent-org",
            None,
            "workspace-parent-org",
            id="configured-workspace-parent",
        ),
        pytest.param(
            {},
            None,
            [{"organizationId": "membership-org"}],
            "membership-org",
            id="sole-membership",
        ),
    ],
)
def test_cloud_client_get_organization_resolves_default_context(
    monkeypatch: pytest.MonkeyPatch,
    client_kwargs: dict[str, str],
    parent_organization_id: str | None,
    permissions: list[dict[str, object]] | None,
    expected_id: str,
) -> None:
    monkeypatch.setattr(
        api_util, "get_user_id_from_bearer_token", lambda _: "auth-user-id"
    )
    monkeypatch.setattr(
        api_util, "get_user_by_auth_id", lambda *_, **__: {"userId": "user-id"}
    )
    monkeypatch.setattr(
        api_util,
        "list_permissions_for_user",
        lambda *_, **__: permissions or [],
    )
    monkeypatch.setattr(
        api_util,
        "get_workspace_organization_info",
        lambda **_: {"organizationId": parent_organization_id}
        if parent_organization_id is not None
        else pytest.fail("workspace parent lookup should not be called"),
    )
    monkeypatch.setattr(
        api_util,
        "get_organization_info",
        lambda **kwargs: {
            "organizationId": kwargs["organization_id"],
            "organizationName": "Organization",
        },
    )

    organization = CloudClient(bearer_token="token", **client_kwargs).get_organization()

    assert organization.organization_id == expected_id


def test_cloud_client_get_organization_rejects_ambiguous_default_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organization_ids = tuple(f"organization-{index}" for index in range(12))
    monkeypatch.setattr(
        CloudClient,
        "_get_membership_organization_ids",
        lambda _: organization_ids,
    )
    monkeypatch.setattr(
        api_util,
        "get_organization_info",
        lambda **kwargs: {
            "organizationName": f"Organization {kwargs['organization_id']}"
        },
    )

    with pytest.raises(PyAirbyteInputError) as exc_info:
        CloudClient(bearer_token="token").get_organization()

    error = exc_info.value
    assert "organization-0 (Organization organization-0)" in str(error)
    assert "organization-10" not in error.message
    assert error.context["organization_ids"] == list(organization_ids)
    assert len(error.context["organization_candidates"]) == 10
    assert error.context["total_candidates"] == 12


@pytest.mark.parametrize(
    ("client_kwargs", "failure", "membership_ids", "expected_id"),
    [
        pytest.param(
            {"workspace_id": "configured-workspace"},
            "workspace-parent",
            ("membership-org",),
            "membership-org",
            id="workspace-parent-failure-falls-back-to-membership",
        ),
        pytest.param(
            {},
            "membership",
            (),
            None,
            id="membership-failure-falls-back-to-no-organization",
        ),
    ],
)
def test_cloud_client_default_organization_handles_resolution_failures(
    monkeypatch: pytest.MonkeyPatch,
    client_kwargs: dict[str, str],
    failure: str,
    membership_ids: tuple[str, ...],
    expected_id: str | None,
) -> None:
    client = CloudClient(bearer_token="token", **client_kwargs)
    if failure == "workspace-parent":
        monkeypatch.setattr(
            client,
            "_get_workspace_parent_organization_id",
            _raise(PyAirbyteInputError(message="workspace lookup failed")),
        )
        monkeypatch.setattr(
            client,
            "_get_membership_organization_ids",
            lambda: membership_ids,
        )
    else:
        monkeypatch.setattr(
            client,
            "_get_membership_organization_ids",
            _raise(AirbyteError(message="membership failed")),
        )

    assert client._resolve_default_organization_id() == expected_id


def test_cloud_client_get_organization_uses_single_config_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_get_organization_info(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {
            "organizationId": "organization-id",
            "organizationName": "Organization",
            "email": "test@example.com",
        }

    monkeypatch.setattr(api_util, "get_organization_info", fake_get_organization_info)
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user",
        lambda **_: pytest.fail("full organization listing should not be called"),
    )

    organization = CloudClient(bearer_token="token").get_organization(
        organization_id="organization-id"
    )

    assert organization.organization_id == "organization-id"
    assert [call["organization_id"] for call in calls] == ["organization-id"]


@pytest.mark.parametrize(
    "organizations",
    [
        pytest.param([], id="empty"),
        pytest.param(
            [
                models.OrganizationResponse(
                    organization_id="organization-id",
                    organization_name="Organization",
                    email="test@example.com",
                )
            ],
            id="single",
        ),
        pytest.param(
            [
                models.OrganizationResponse(
                    organization_id="organization-id-1",
                    organization_name="Organization 1",
                    email="one@example.com",
                ),
                models.OrganizationResponse(
                    organization_id="organization-id-2",
                    organization_name="Organization 2",
                    email="two@example.com",
                ),
            ],
            id="multiple",
        ),
    ],
)
def test_cloud_client_list_organizations_returns_typed_resources(
    monkeypatch: pytest.MonkeyPatch,
    organizations: list[models.OrganizationResponse],
) -> None:
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user",
        lambda **_: organizations,
    )

    result = CloudClient(bearer_token="token").list_organizations()

    assert all(isinstance(organization, CloudOrganization) for organization in result)
    assert [organization.organization_id for organization in result] == [
        organization.organization_id for organization in organizations
    ]


@pytest.mark.parametrize(
    ("name_contains", "expected_ids"),
    [
        pytest.param("DEVELOP", ["organization-id-1"], id="name-filter"),
        pytest.param(None, ["organization-id-1"], id="limit-only"),
    ],
)
def test_cloud_client_list_organizations_uses_config_api_for_filter_or_limit(
    monkeypatch: pytest.MonkeyPatch,
    name_contains: str | None,
    expected_ids: list[str],
) -> None:
    captured: dict[str, object] = {}
    organizations = [
        models.OrganizationResponse(
            organization_id="organization-id-1",
            organization_name="Development",
            email="one@example.com",
        ),
        models.OrganizationResponse(
            organization_id="organization-id-2",
            organization_name="development-copy",
            email="two@example.com",
        ),
        models.OrganizationResponse(
            organization_id="organization-id-3",
            organization_name="Production",
            email="three@example.com",
        ),
    ]

    def fake_list_organizations_for_user_id(
        **kwargs: object,
    ) -> list[dict[str, object]]:
        captured.update(kwargs)
        filtered = organizations
        if kwargs["name_contains"] is not None:
            name_substring = str(kwargs["name_contains"]).casefold()
            filtered = [
                organization
                for organization in filtered
                if name_substring in (organization.organization_name or "").casefold()
            ]
        limit = kwargs["limit"]
        return [
            {
                "organizationId": organization.organization_id,
                "organizationName": organization.organization_name,
                "email": organization.email,
            }
            for organization in filtered[: limit if isinstance(limit, int) else None]
        ]

    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user_id",
        fake_list_organizations_for_user_id,
    )
    monkeypatch.setattr(
        api_util, "get_user_id_from_bearer_token", lambda _: "auth-user-id"
    )
    monkeypatch.setattr(
        api_util, "get_user_by_auth_id", lambda *_, **__: {"userId": "user-id"}
    )

    result = CloudClient(bearer_token="token").list_organizations(
        name_contains=name_contains,
        limit=1,
    )

    assert [organization.organization_id for organization in result] == expected_ids
    assert captured["name_contains"] == name_contains
    assert captured["limit"] == 1


def test_cloud_client_list_organizations_falls_back_to_public_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organizations = [
        models.OrganizationResponse(
            organization_id="organization-id",
            organization_name="Development",
            email="test@example.com",
        )
    ]
    monkeypatch.setattr(
        api_util,
        "get_user_id_from_bearer_token",
        lambda _: "auth-user-id",
    )
    monkeypatch.setattr(
        api_util,
        "get_user_by_auth_id",
        lambda *_, **__: {"userId": "user-id"},
    )
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user_id",
        lambda **_: (_ for _ in ()).throw(AirbyteError(message="Unavailable")),
    )
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user",
        lambda **_: organizations,
    )

    result = CloudClient(bearer_token="token").list_organizations(
        name_contains="develop",
        limit=1,
    )

    assert [organization.organization_id for organization in result] == [
        "organization-id"
    ]


def test_cloud_client_config_org_listing_reuses_authenticated_bearer_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issued_token = SecretString("issued-token")
    token_calls = 0
    captured_bearer_token: object = None

    def fake_get_bearer_token(**_: object) -> SecretString:
        nonlocal token_calls
        token_calls += 1
        return issued_token

    def fake_list_organizations_for_user_id(
        **kwargs: object,
    ) -> list[dict[str, object]]:
        nonlocal captured_bearer_token
        captured_bearer_token = kwargs["bearer_token"]
        return [{"organizationId": "organization-id"}]

    monkeypatch.setattr(api_util, "get_bearer_token", fake_get_bearer_token)
    monkeypatch.setattr(
        api_util,
        "get_user_id_from_bearer_token",
        lambda _: "auth-user-id",
    )
    monkeypatch.setattr(
        api_util,
        "get_user_by_auth_id",
        lambda *_, **__: {"userId": "user-id"},
    )
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user_id",
        fake_list_organizations_for_user_id,
    )

    result = CloudClient(
        client_id="client-id",
        client_secret="client-secret",
    ).list_organizations(limit=1)

    assert [organization.organization_id for organization in result] == [
        "organization-id"
    ]
    assert captured_bearer_token is issued_token
    assert token_calls == 1


def test_cloud_client_config_lookups_reuse_authenticated_bearer_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issued_token = SecretString("issued-token")
    token_calls = 0
    captured_bearer_tokens: list[object] = []

    def fake_get_bearer_token(**_: object) -> SecretString:
        nonlocal token_calls
        token_calls += 1
        return issued_token

    def fake_get_organization_info(**kwargs: object) -> dict[str, object]:
        captured_bearer_tokens.append(kwargs["bearer_token"])
        return {
            "organizationId": "organization-id",
            "organizationName": "Organization",
        }

    monkeypatch.setattr(api_util, "get_bearer_token", fake_get_bearer_token)
    monkeypatch.setattr(api_util, "get_organization_info", fake_get_organization_info)

    client = CloudClient(client_id="client-id", client_secret="client-secret")
    client.get_organization(organization_id="organization-id")
    client.get_organization(organization_id="organization-id")

    assert captured_bearer_tokens == [issued_token, issued_token]
    assert token_calls == 1


def test_cloud_client_get_organization_requires_context_without_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CloudClient, "_get_membership_organization_ids", lambda _: ())

    with pytest.raises(
        PyAirbyteInputError,
        match="Organization ID or organization name is required.",
    ):
        CloudClient(bearer_token="token").get_organization()


def test_cloud_client_list_organizations_has_no_default_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organizations = [
        models.OrganizationResponse(
            organization_id=f"organization-id-{index}",
            organization_name=f"Organization {index}",
            email=f"test-{index}@example.com",
        )
        for index in range(101)
    ]
    monkeypatch.setattr(
        api_util, "list_organizations_for_user", lambda **_: organizations
    )

    result = CloudClient(bearer_token="token").list_organizations()

    assert len(result) == 101
    assert result[-1].organization_id == "organization-id-100"


def test_cloud_client_list_organizations_reports_ambiguity_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organizations = [
        CloudOrganization(
            organization_id=f"organization-id-{index}",
            organization_name="Duplicate",
            email=f"test-{index}@example.com",
        )
        for index in range(11)
    ]
    client = CloudClient(bearer_token="token")
    monkeypatch.setattr(
        api_util,
        "get_user_id_from_bearer_token",
        lambda _: "auth-user-id",
    )
    monkeypatch.setattr(
        api_util,
        "get_user_by_auth_id",
        lambda *_, **__: {"userId": "user-id"},
    )
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user_id",
        lambda **_: (_ for _ in ()).throw(AirbyteError(message="Unavailable")),
    )
    monkeypatch.setattr(client, "_fetch_organizations", lambda: organizations)

    with pytest.raises(PyAirbyteInputError) as exc_info:
        client.get_organization(organization_name="Duplicate")

    error = exc_info.value
    assert "showing 10 of 11" in str(error)
    assert "organization-id-0" in str(error)
    assert "test-0@example.com" in str(error)
    assert "organization-id-10" not in str(error)
    assert error.context == {
        "organization_name": "Duplicate",
        "matching_organizations": [
            {
                "organization_id": f"organization-id-{index}",
                "email": f"test-{index}@example.com",
            }
            for index in range(10)
        ],
        "total_matches": 11,
    }


@pytest.mark.parametrize(
    (
        "client_kwargs",
        "request_kwargs",
        "permissions",
        "parent_organization_id",
        "expected_organization_id",
        "expected_limit",
        "org_scoped_result",
        "expected_result_count",
    ),
    [
        pytest.param(
            {},
            {"limit": 3, "workspace_id": "workspace-id"},
            None,
            "parent-organization-id",
            "parent-organization-id",
            3,
            None,
            0,
            id="explicit_workspace_parent",
        ),
        pytest.param(
            {"organization_id": "configured-organization-id"},
            {"organization_id": "configured-organization-id", "limit": 3},
            None,
            None,
            "configured-organization-id",
            3,
            None,
            0,
            id="configured_organization",
        ),
        pytest.param(
            {"workspace_id": "configured-workspace-id"},
            {"workspace_id": "configured-workspace-id", "limit": 3},
            None,
            "parent-organization-id",
            "parent-organization-id",
            3,
            None,
            0,
            id="configured_workspace_parent",
        ),
        pytest.param(
            {
                "organization_id": "configured-organization-id",
                "workspace_id": "configured-workspace-id",
            },
            {"organization_id": "configured-organization-id", "limit": 3},
            None,
            None,
            "configured-organization-id",
            3,
            None,
            0,
            id="configured_organization_precedes_workspace",
        ),
        pytest.param(
            {},
            {
                "limit": 3,
                "privilege_scope": WorkspacePrivilegeScope.ORGANIZATION_ADMIN,
            },
            [
                {"permissionType": "instance_admin"},
                {
                    "permissionType": "organization_member",
                    "organizationId": "organization-id",
                },
            ],
            None,
            "organization-id",
            3,
            None,
            0,
            id="single_membership_ignores_instance_admin",
        ),
        pytest.param(
            {},
            {
                "name_filter": lambda _: True,
                "privilege_scope": WorkspacePrivilegeScope.ORGANIZATION_ADMIN,
            },
            [
                {
                    "permissionType": "organization_member",
                    "organizationId": "organization-id",
                }
            ],
            None,
            "organization-id",
            None,
            [
                {"workspaceId": f"workspace-{index}", "name": f"Workspace {index}"}
                for index in range(101)
            ],
            101,
            id="explicit_lookup_is_unbounded",
        ),
    ],
)
def test_cloud_client_list_workspaces_resolves_organization_context(
    monkeypatch: pytest.MonkeyPatch,
    client_kwargs: dict[str, str],
    request_kwargs: dict[str, object],
    permissions: list[dict[str, object]] | None,
    parent_organization_id: str | None,
    expected_organization_id: str,
    expected_limit: int | None,
    org_scoped_result: list[dict[str, object]] | None,
    expected_result_count: int,
) -> None:
    captured = _patch_workspace_discovery(
        monkeypatch,
        permissions=permissions,
        parent_organization_id=parent_organization_id,
        org_scoped_result=org_scoped_result,
    )

    result = CloudClient(bearer_token="token", **client_kwargs).list_workspaces(
        **request_kwargs
    )

    assert captured["organization_id"] == expected_organization_id
    assert captured["limit"] == expected_limit
    assert len(result) == expected_result_count


def test_cloud_client_list_workspaces_resolves_single_membership_and_caches_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    permissions: list[dict[str, object]] = [
        {"permissionType": "instance_admin"},
        {
            "permissionType": "organization_member",
            "organizationId": "organization-id",
        },
    ]
    captured = _patch_workspace_discovery(monkeypatch, permissions=permissions)
    calls = {"user": 0, "permissions": 0}

    def fake_get_user_by_auth_id(*_: object, **__: object) -> dict[str, object]:
        calls["user"] += 1
        return {"userId": "user-id"}

    def fake_list_permissions_for_user(
        *_: object, **__: object
    ) -> list[dict[str, object]]:
        calls["permissions"] += 1
        return permissions

    monkeypatch.setattr(api_util, "get_user_by_auth_id", fake_get_user_by_auth_id)
    monkeypatch.setattr(
        api_util, "list_permissions_for_user", fake_list_permissions_for_user
    )
    client = CloudClient(bearer_token="token")
    client.list_workspaces(privilege_scope=WorkspacePrivilegeScope.ORGANIZATION_ADMIN)
    client.list_workspaces(privilege_scope=WorkspacePrivilegeScope.ORGANIZATION_ADMIN)

    assert captured["organization_id"] == "organization-id"
    assert calls == {"user": 1, "permissions": 1}


@pytest.mark.parametrize(
    ("permissions", "privilege_scope"),
    [
        pytest.param(
            [{"permissionType": "instance_admin"}],
            WorkspacePrivilegeScope.INSTANCE_ADMIN,
            id="instance-admin-scope",
        ),
        pytest.param(
            [{"permissionType": "instance_admin"}],
            WorkspacePrivilegeScope.ANY,
            id="any-scope-for-instance-admin",
        ),
    ],
)
def test_cloud_client_list_workspaces_uses_cross_organization_listing(
    monkeypatch: pytest.MonkeyPatch,
    permissions: list[dict[str, object]],
    privilege_scope: WorkspacePrivilegeScope,
) -> None:
    captured: dict[str, object] = {}

    _patch_workspace_discovery(
        monkeypatch,
        permissions=permissions,
    )

    def fake_list_workspaces(**kwargs: object) -> list[models.WorkspaceResponse]:
        captured.update(kwargs)
        return [
            models.WorkspaceResponse(
                data_residency="auto",
                name="Workspace",
                notifications=models.NotificationsConfig(),
                workspace_id="workspace-id",
            )
        ]

    monkeypatch.setattr(api_util, "list_workspaces", fake_list_workspaces)

    result = CloudClient(bearer_token="token").list_workspaces(
        privilege_scope=privilege_scope,
    )

    assert captured["workspace_id"] == ""
    assert captured["limit"] is None
    assert [workspace.workspace_id for workspace in result] == ["workspace-id"]


def test_mcp_get_cloud_client_uses_configured_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {
        mcp_cloud.MCP_CONFIG_BEARER_TOKEN: "token",
        mcp_cloud.MCP_CONFIG_WORKSPACE_ID: "workspace-id",
    }
    monkeypatch.setattr(
        mcp_cloud,
        "get_mcp_config",
        lambda _, key: config.get(key),
    )

    client = mcp_cloud._get_cloud_client(None)

    assert client.default_workspace_id == "workspace-id"


def test_mcp_describe_cloud_organization_resolves_without_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_organization_features(monkeypatch)

    class DiscoveryClient:
        def get_organization(
            self,
            *,
            organization_id: str | None = None,
            organization_name: str | None = None,
        ) -> CloudOrganization:
            assert organization_id is None
            assert organization_name is None
            return CloudOrganization(
                organization_id="organization-id",
                organization_name="Organization",
                email="test@example.com",
            )

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.describe_cloud_organization(
        None,
        organization_id=None,
        organization_name=None,
    )

    assert result.id == "organization-id"
    assert result.name == "Organization"


def test_cloud_client_get_organization_uses_unbounded_organization_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organizations = [
        CloudOrganization(
            organization_id="organization-id",
            organization_name="Organization",
            email="test@example.com",
        )
    ]
    client = CloudClient(bearer_token="token")
    monkeypatch.setattr(
        api_util,
        "list_organizations_for_user_id",
        lambda **_: (_ for _ in ()).throw(AirbyteError(message="Unavailable")),
    )
    monkeypatch.setattr(
        api_util,
        "get_organization_info",
        lambda **_: (_ for _ in ()).throw(AirbyteError(message="Unavailable")),
    )
    monkeypatch.setattr(client, "_fetch_organizations", lambda: organizations)

    result = client.get_organization(organization_id="organization-id")

    assert result is organizations[0]


@pytest.mark.parametrize(
    ("organizations_or_error", "expected_count", "expected_message"),
    [
        pytest.param([], 0, "No organizations", id="empty-organizations"),
        pytest.param(
            [
                CloudOrganization(
                    organization_id="organization-id-1",
                    organization_name="Organization 1",
                    email="test-1@example.com",
                )
            ],
            1,
            None,
            id="single-organization",
        ),
        pytest.param(
            [
                CloudOrganization(
                    organization_id="organization-id-1",
                    organization_name="Organization 1",
                    email="test-1@example.com",
                ),
                CloudOrganization(
                    organization_id="organization-id-2",
                    organization_name="Organization 2",
                    email="test-2@example.com",
                ),
            ],
            2,
            None,
            id="multiple-organizations",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 401}),
            0,
            "permission",
            id="unauthorized",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 403}),
            0,
            "permission",
            id="forbidden",
        ),
    ],
)
def test_mcp_list_cloud_organizations_discovery(
    monkeypatch: pytest.MonkeyPatch,
    organizations_or_error: list[CloudOrganization] | AirbyteError,
    expected_count: int,
    expected_message: str | None,
) -> None:
    _stub_organization_features(monkeypatch)

    class DiscoveryClient:
        def list_organizations(self, **_: object) -> list[CloudOrganization]:
            if isinstance(organizations_or_error, AirbyteError):
                raise organizations_or_error
            return organizations_or_error

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.list_cloud_organizations(None)

    assert len(result.organizations) == expected_count
    if expected_message is not None:
        assert expected_message in (result.message or "")
    else:
        assert result.message is None


def test_mcp_list_cloud_organizations_preserves_missing_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_organization_features(monkeypatch)

    class DiscoveryClient:
        def list_organizations(self, **_: object) -> list[CloudOrganization]:
            return [CloudOrganization(organization_id="organization-id")]

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.list_cloud_organizations(None)

    assert result.organizations == [
        mcp_cloud.CloudOrganizationResult(
            id="organization-id",
            name=None,
            email=None,
            external_access_enabled=False,
            search_indexing_enabled=False,
        )
    ]


def _seed_source(workspace: CloudWorkspace, source_id: str, name: str) -> CloudSource:
    source = CloudSource(workspace=workspace, connector_id=source_id)
    source._connector_info = CloudSourceInfo(  # noqa: SLF001
        source_id=source_id, name=name, definition_id="source-definition"
    )
    return source


def _seed_destination(
    workspace: CloudWorkspace, destination_id: str, definition_id: str
) -> CloudDestination:
    destination = CloudDestination(workspace=workspace, connector_id=destination_id)
    destination._connector_info = CloudDestinationInfo(  # noqa: SLF001
        destination_id=destination_id, name=destination_id, definition_id=definition_id
    )
    return destination


SNOWFLAKE_DEFINITION_ID = next(iter(SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS))


def _patch_workspace_connectors(
    monkeypatch: pytest.MonkeyPatch,
    workspace: CloudWorkspace,
    *,
    context_layer: bool = True,
    workspace_enabled: bool = True,
    source_status: dict[str, bool] | None = None,
) -> dict[str, int]:
    """Stub the Cloud listings and Context layer lookups for `workspace`.

    Returns a counter of Context layer calls so tests can assert on batching.
    """
    calls = {"list": 0, "inspect": 0, "workspace": 0}
    source_status = source_status or {}

    monkeypatch.setattr(
        workspace,
        "list_sources",
        lambda **_: [
            _seed_source(workspace, "source-1", "GitHub Issues"),
            _seed_source(workspace, "source-2", "Salesforce"),
            _seed_source(workspace, "source-3", "Jira"),
        ],
    )
    monkeypatch.setattr(
        workspace,
        "list_destinations",
        lambda **_: [
            _seed_destination(workspace, "snowflake", SNOWFLAKE_DEFINITION_ID),
            _seed_destination(workspace, "postgres", "not-a-passthrough-definition"),
        ],
    )
    monkeypatch.setattr(
        cloud_workspaces.deployment,
        "is_agents_api_available",
        lambda **_: context_layer,
    )

    def fake_get_agent_workspace(**_: object) -> dict[str, object]:
        calls["workspace"] += 1
        if not workspace_enabled:
            raise AirbyteError(context={"status_code": 403})
        return {"id": "workspace-id"}

    def fake_list_agent_connectors(**_: object) -> list[dict[str, object]]:
        calls["list"] += 1
        return [{"id": connector_id} for connector_id in source_status]

    def fake_inspect_agent_connector(
        *, connector_id: str, **_: object
    ) -> dict[str, object]:
        calls["inspect"] += 1
        return {
            "connector_id": connector_id,
            "context_store_readiness": {
                "configured_cache_entities": (
                    [{"entity": "issues"}] if source_status[connector_id] else []
                )
            },
        }

    monkeypatch.setattr(
        cloud_workspaces.agents_api_util,
        "get_agent_workspace",
        fake_get_agent_workspace,
    )
    monkeypatch.setattr(
        cloud_workspaces.agents_api_util,
        "list_agent_connectors",
        fake_list_agent_connectors,
    )
    monkeypatch.setattr(
        cloud_workspaces.agents_api_util,
        "inspect_agent_connector",
        fake_inspect_agent_connector,
    )
    return calls


@pytest.mark.parametrize(
    ("connector_type", "with_feature", "name_contains", "limit", "expected"),
    [
        pytest.param(
            None,
            None,
            None,
            None,
            [
                ("source-1", True, True),
                ("source-2", True, False),
                ("source-3", False, False),
                ("snowflake", True, False),
                ("postgres", False, False),
            ],
            id="all",
        ),
        pytest.param(
            ConnectorType.SOURCE,
            None,
            None,
            None,
            [
                ("source-1", True, True),
                ("source-2", True, False),
                ("source-3", False, False),
            ],
            id="sources",
        ),
        pytest.param(
            ConnectorType.DESTINATION,
            None,
            None,
            None,
            [("snowflake", True, False), ("postgres", False, False)],
            id="destinations",
        ),
        pytest.param(
            None,
            ConnectorFeature.EXTERNAL_ACCESS,
            None,
            None,
            [
                ("source-1", True, True),
                ("source-2", True, False),
                ("snowflake", True, False),
            ],
            id="external_access",
        ),
        pytest.param(
            None,
            ConnectorFeature.SEARCH_INDEXING,
            None,
            None,
            [("source-1", True, True)],
            id="search_indexing",
        ),
        pytest.param(
            None,
            ConnectorFeature.EXTERNAL_ACCESS,
            None,
            2,
            [("source-1", True, True), ("source-2", True, False)],
            id="limit_applies_after_feature_filter",
        ),
        pytest.param(
            None,
            None,
            "SALES",
            None,
            [("source-2", True, False)],
            id="name_contains_is_case_insensitive",
        ),
    ],
)
def test_cloud_workspace_list_connectors(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: ConnectorType | None,
    with_feature: ConnectorFeature | None,
    name_contains: str | None,
    limit: int | None,
    expected: list[tuple[str, bool, bool]],
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    calls = _patch_workspace_connectors(
        monkeypatch, workspace, source_status={"source-1": True, "source-2": False}
    )

    connectors = workspace.list_connectors(
        connector_type=connector_type,
        with_feature=with_feature,
        name_contains=name_contains,
        limit=limit,
    )

    assert [
        (c.connector_id, c.external_access_enabled, c.search_indexing_enabled)
        for c in connectors
    ] == expected
    assert all(
        isinstance(c, CloudSource if c.connector_type == "source" else CloudDestination)
        for c in connectors
    )
    # One Context layer listing per call at most, and one inspect per listed source.
    assert calls["list"] <= 1
    assert calls["inspect"] == 2 * calls["list"]
    assert calls["workspace"] <= 1


def test_cloud_workspace_list_connectors_rejects_non_positive_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    with pytest.raises(PyAirbyteInputError, match="`limit` must be greater than 0."):
        workspace.list_connectors(limit=0)


def test_cloud_workspace_features_false_without_context_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    calls = _patch_workspace_connectors(
        monkeypatch, workspace, context_layer=False, source_status={"source-1": True}
    )

    connectors = workspace.list_connectors()

    assert workspace.external_access_enabled is False
    assert workspace.search_indexing_enabled is False
    assert len(connectors) == 5
    assert not any(c.external_access_enabled for c in connectors)
    assert not any(c.search_indexing_enabled for c in connectors)
    assert (
        workspace.list_connectors(with_feature=ConnectorFeature.EXTERNAL_ACCESS) == []
    )
    assert calls == {"list": 0, "inspect": 0, "workspace": 0}


def test_cloud_connector_features_resolve_lazily_and_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    calls = _patch_workspace_connectors(
        monkeypatch, workspace, source_status={"source-1": True}
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")
    destination = _seed_destination(workspace, "snowflake", SNOWFLAKE_DEFINITION_ID)

    assert calls == {"list": 0, "inspect": 0, "workspace": 0}
    assert source.search_indexing_enabled is True
    assert source.external_access_enabled is True
    assert calls["list"] == 1
    assert calls["inspect"] == 1
    assert destination.external_access_enabled is True
    assert destination.search_indexing_enabled is False
    assert calls["workspace"] == 1


def test_cloud_destination_external_access_requires_enabled_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    _patch_workspace_connectors(monkeypatch, workspace, workspace_enabled=False)

    assert workspace.external_access_enabled is False
    destination = _seed_destination(workspace, "snowflake", SNOWFLAKE_DEFINITION_ID)
    assert destination.external_access_enabled is False


@pytest.mark.parametrize(
    ("with_feature", "expected_ids", "expected_flags"),
    [
        pytest.param(
            None,
            ["source-1", "source-2", "source-3"],
            [(True, True), (True, False), (False, False)],
            id="no_filter",
        ),
        pytest.param(
            ConnectorFeature.SEARCH_INDEXING,
            ["source-1"],
            [(True, True)],
            id="search_indexing",
        ),
    ],
)
def test_mcp_list_deployed_cloud_source_connectors_features(
    monkeypatch: pytest.MonkeyPatch,
    with_feature: ConnectorFeature | None,
    expected_ids: list[str],
    expected_flags: list[tuple[bool, bool]],
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    _patch_workspace_connectors(
        monkeypatch, workspace, source_status={"source-1": True, "source-2": False}
    )
    monkeypatch.setattr(mcp_cloud, "_get_cloud_workspace", lambda _ctx, _id: workspace)

    results = mcp_cloud.list_deployed_cloud_source_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=None,
        with_feature=with_feature,
    )

    assert [result.id for result in results] == expected_ids
    assert [
        (result.external_access_enabled, result.search_indexing_enabled)
        for result in results
    ] == expected_flags
    assert results[0].url.endswith("/source/source-1")


@pytest.mark.parametrize(
    ("with_feature", "expected_ids", "expected_flags"),
    [
        pytest.param(None, ["snowflake", "postgres"], [True, False], id="no_filter"),
        pytest.param(
            ConnectorFeature.EXTERNAL_ACCESS,
            ["snowflake"],
            [True],
            id="external_access",
        ),
        pytest.param(ConnectorFeature.SEARCH_INDEXING, [], [], id="search_indexing"),
    ],
)
def test_mcp_list_deployed_cloud_destination_connectors_features(
    monkeypatch: pytest.MonkeyPatch,
    with_feature: ConnectorFeature | None,
    expected_ids: list[str],
    expected_flags: list[bool],
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    _patch_workspace_connectors(monkeypatch, workspace)
    monkeypatch.setattr(mcp_cloud, "_get_cloud_workspace", lambda _ctx, _id: workspace)

    results = mcp_cloud.list_deployed_cloud_destination_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=None,
        with_feature=with_feature,
    )

    assert [result.id for result in results] == expected_ids
    assert [result.external_access_enabled for result in results] == expected_flags
    assert all(result.search_indexing_enabled is False for result in results)


@pytest.mark.parametrize(
    "list_tool",
    [
        pytest.param(mcp_cloud.list_deployed_cloud_source_connectors, id="sources"),
        pytest.param(
            mcp_cloud.list_deployed_cloud_destination_connectors, id="destinations"
        ),
    ],
)
@pytest.mark.parametrize("limit", [0, -1])
def test_mcp_list_deployed_cloud_connectors_rejects_non_positive_limit(
    monkeypatch: pytest.MonkeyPatch,
    list_tool: Callable[..., object],
    limit: int,
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    monkeypatch.setattr(mcp_cloud, "_get_cloud_workspace", lambda _ctx, _id: workspace)

    with pytest.raises(PyAirbyteInputError, match="`limit` must be greater than 0."):
        list_tool(None, workspace_id=None, name_contains=None, limit=limit)


def _make_workspace(
    monkeypatch: pytest.MonkeyPatch,
    *,
    organization_info: dict[str, object] | Exception,
    configured_organization_id: str | None = None,
) -> CloudWorkspace:
    """Return a `CloudWorkspace` whose organization lookup is stubbed."""
    workspace = CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        organization_id=configured_organization_id,
    )

    def fake_organization_info(self: CloudWorkspace) -> dict[str, object]:
        if isinstance(organization_info, Exception):
            raise organization_info
        return organization_info

    monkeypatch.setattr(
        CloudWorkspace, "_organization_info", property(fake_organization_info)
    )
    return workspace


@pytest.mark.parametrize(
    ("configured_organization_id", "organization_info", "expected"),
    [
        pytest.param(
            None,
            {"organizationId": "organization-id"},
            "organization-id",
            id="resolved",
        ),
        pytest.param(
            None,
            {"organizationId": "organization-id", "organizationName": None},
            "organization-id",
            id="resolved_without_name",
        ),
        pytest.param(
            "organization-id",
            {"organizationId": "organization-id"},
            "organization-id",
            id="configured_matches_lookup",
        ),
        pytest.param(
            "configured-organization-id",
            {},
            "configured-organization-id",
            id="configured_fills_missing_lookup",
        ),
        pytest.param(
            "configured-organization-id",
            AirbyteError(context={"status_code": 403}),
            "configured-organization-id",
            id="configured_without_lookup",
        ),
        pytest.param(None, {}, None, id="missing"),
        pytest.param(
            None, AirbyteError(context={"status_code": 403}), None, id="forbidden"
        ),
        pytest.param(None, requests.ConnectionError("offline"), None, id="transport"),
        pytest.param(
            None, NotImplementedError("custom api root"), None, id="custom_api_root"
        ),
    ],
)
def test_cloud_workspace_resolve_agents_organization_id(
    monkeypatch: pytest.MonkeyPatch,
    configured_organization_id: str | None,
    organization_info: dict[str, object] | Exception,
    expected: str | None,
) -> None:
    workspace = _make_workspace(
        monkeypatch,
        organization_info=organization_info,
        configured_organization_id=configured_organization_id,
    )

    assert workspace._resolve_agents_organization_id() == expected


def test_cloud_workspace_resolve_agents_organization_id_rejects_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(
        monkeypatch,
        organization_info={"organizationId": "organization-id"},
        configured_organization_id="other-organization-id",
    )

    with pytest.raises(PyAirbyteInputError, match="does not match"):
        workspace._resolve_agents_organization_id()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(None, True, id="enabled"),
        pytest.param(AirbyteError(context={"status_code": 403}), False, id="forbidden"),
        pytest.param(AirbyteError(context={"status_code": 404}), False, id="not_found"),
        pytest.param(
            AirbyteError(context={"status_code": 500}), None, id="server_error"
        ),
        pytest.param(requests.ConnectionError("offline"), None, id="transport"),
    ],
)
def test_cloud_workspace_external_access_enabled(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception | None,
    expected: bool | None,
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )
    monkeypatch.setattr(
        cloud_workspaces.deployment, "is_agents_api_available", lambda **_: True
    )

    def fake_get_agent_workspace(
        *, workspace_id: str, credentials: object, organization_id: str | None
    ) -> dict[str, object]:
        assert workspace_id == "workspace-id"
        assert organization_id == "organization-id"
        if error is not None:
            raise error
        return {"id": workspace_id}

    monkeypatch.setattr(
        cloud_workspaces.agents_api_util,
        "get_agent_workspace",
        fake_get_agent_workspace,
    )

    if expected is None:
        with pytest.raises(type(error)):
            _ = workspace.external_access_enabled
    else:
        assert workspace.external_access_enabled is expected
        assert workspace.search_indexing_enabled is expected


@pytest.mark.parametrize(
    ("list_error", "connectors", "expected"),
    [
        pytest.param(
            None,
            {
                "cached": [{"entity": "issues"}],
                "not_cached": [],
                "no_readiness": None,
            },
            {"cached": True, "not_cached": False, "no_readiness": False},
            id="mixed_connectors",
        ),
        pytest.param(
            None,
            {"inspect_fails": AirbyteError(context={"status_code": 500})},
            AirbyteError,
            id="inspect_fails",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 403}),
            {},
            {},
            id="list_forbidden",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 404}),
            {},
            {},
            id="list_not_found",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 500}),
            {},
            AirbyteError,
            id="list_server_error",
        ),
        pytest.param(
            requests.ConnectionError("offline"),
            {},
            requests.ConnectionError,
            id="transport",
        ),
        pytest.param(
            None,
            {"malformed_inspect": {"connector_id": 123}},
            ValidationError,
            id="malformed_inspect_payload",
        ),
        pytest.param(
            [{"name": "missing-id"}],
            {},
            ValidationError,
            id="malformed_list_payload",
        ),
    ],
)
def test_cloud_workspace_list_source_search_indexing_status(
    monkeypatch: pytest.MonkeyPatch,
    list_error: Exception | list[dict[str, object]] | None,
    connectors: dict[str, list[dict[str, str]] | dict[str, object] | Exception | None],
    expected: dict[str, bool] | type[Exception],
) -> None:
    workspace = _make_workspace(
        monkeypatch, organization_info={"organizationId": "organization-id"}
    )

    def fake_list_agent_connectors(
        *, workspace_id: str, credentials: object, organization_id: str | None
    ) -> list[dict[str, object]]:
        assert workspace_id == "workspace-id"
        assert organization_id == "organization-id"
        if isinstance(list_error, Exception):
            raise list_error
        if list_error is not None:
            return list_error
        return [{"id": connector_id} for connector_id in connectors]

    def fake_inspect_agent_connector(
        *, connector_id: str, credentials: object, organization_id: str | None
    ) -> dict[str, object]:
        assert organization_id == "organization-id"
        outcome = connectors[connector_id]
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, dict):
            return outcome
        if outcome is None:
            return {"connector_id": connector_id}
        return {
            "connector_id": connector_id,
            "context_store_readiness": {"configured_cache_entities": outcome},
        }

    monkeypatch.setattr(
        cloud_workspaces.agents_api_util,
        "list_agent_connectors",
        fake_list_agent_connectors,
    )
    monkeypatch.setattr(
        cloud_workspaces.agents_api_util,
        "inspect_agent_connector",
        fake_inspect_agent_connector,
    )

    if isinstance(expected, dict):
        assert workspace._list_source_search_indexing_status() == expected  # noqa: SLF001
    else:
        with pytest.raises(expected):
            workspace._list_source_search_indexing_status()  # noqa: SLF001


@pytest.mark.parametrize(
    ("workspaces_or_error", "expect_message", "organization_name"),
    [
        pytest.param(
            [
                CloudWorkspaceInfo(
                    workspaceId="workspace-id",
                    name="Workspace",
                    organizationId="organization-id",
                ),
                CloudWorkspaceInfo(
                    workspaceId="workspace-without-org",
                    name="Workspace without organization",
                    organizationId=None,
                ),
            ],
            False,
            "Organization",
            id="org-less-public-api",
        ),
        pytest.param(
            [
                CloudWorkspaceInfo(
                    workspaceId="workspace-id",
                    name="Workspace",
                    organizationId="organization-id",
                ),
                CloudWorkspaceInfo(
                    workspaceId="workspace-without-org",
                    name="Workspace without organization",
                    organizationId=None,
                ),
            ],
            False,
            None,
            id="org-less-public-api-without-organization-name",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 401}),
            True,
            None,
            id="unauthorized",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 403}),
            True,
            None,
            id="forbidden",
        ),
    ],
)
def test_mcp_list_cloud_workspaces_discovery(
    monkeypatch: pytest.MonkeyPatch,
    workspaces_or_error: list[CloudWorkspaceInfo] | AirbyteError,
    expect_message: bool,
    organization_name: str | None,
) -> None:
    captured_organization_id: str | None = "unset"

    class DiscoveryClient:
        organization_id: str | None = None

        def list_workspaces(
            self, *, organization_id: str | None = None, **_: object
        ) -> list[CloudWorkspaceInfo]:
            nonlocal captured_organization_id
            captured_organization_id = organization_id
            if isinstance(workspaces_or_error, AirbyteError):
                raise workspaces_or_error
            return workspaces_or_error

        def get_organization(self, *, organization_id: str) -> CloudOrganization:
            return CloudOrganization(
                organization_id=organization_id,
                organization_name=organization_name,
            )

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.list_cloud_workspaces(
        None,
        organization_id=None,
        organization_name=None,
        name_contains=None,
        limit=None,
        privilege_scope=WorkspacePrivilegeScope.MEMBER_OF,
    )

    assert captured_organization_id is None
    if expect_message:
        assert result.workspaces == []
        assert "permission" in (result.message or "")
    else:
        assert result.workspaces[0].workspace_id == "workspace-id"
        assert result.workspaces[1].organization_id is None
        assert result.workspaces[0].organization_name == organization_name
        assert result.workspaces[1].organization_name is None
        assert result.message == (
            "Resolved organization Organization (organization-id) for these credentials."
            if organization_name is not None
            else "Resolved organization organization-id for these credentials."
        )


def test_mcp_list_cloud_organizations_forwards_filter_and_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_organization_features(monkeypatch)
    captured: dict[str, object] = {}

    class DiscoveryClient:
        def list_organizations(self, **kwargs: object) -> list[CloudOrganization]:
            captured.update(kwargs)
            return [
                CloudOrganization(
                    organization_id="organization-id",
                    organization_name="Development",
                    email="test@example.com",
                )
            ]

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.list_cloud_organizations(
        None,
        name_contains="develop",
        limit=1,
    )

    assert captured == {"name_contains": "develop", "with_feature": None, "limit": 1}
    assert len(result.organizations) == 1
    assert (
        result.message == "Showing the first 1 organizations; more may exist. "
        "Pass `name_contains` to narrow the search, or a larger `limit`."
    )


@pytest.mark.parametrize(
    ("with_feature", "expected_fragment"),
    [
        pytest.param(None, "Verify the credentials", id="no_filter"),
        pytest.param(
            ConnectorFeature.EXTERNAL_ACCESS,
            "have `external_access` enabled",
            id="feature_filter",
        ),
    ],
)
def test_mcp_list_cloud_organizations_empty_message(
    monkeypatch: pytest.MonkeyPatch,
    with_feature: ConnectorFeature | None,
    expected_fragment: str,
) -> None:
    class DiscoveryClient:
        def list_organizations(self, **_: object) -> list[CloudOrganization]:
            return []

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.list_cloud_organizations(None, with_feature=with_feature)

    assert result.organizations == []
    assert expected_fragment in (result.message or "")


@pytest.mark.parametrize(
    ("context_layer", "error", "expected"),
    [
        pytest.param(False, None, False, id="no_context_layer"),
        pytest.param(True, None, True, id="enabled"),
        pytest.param(
            True, AirbyteError(context={"status_code": 403}), False, id="forbidden"
        ),
        pytest.param(
            True, AirbyteError(context={"status_code": 404}), False, id="not_found"
        ),
        pytest.param(
            True, AirbyteError(context={"status_code": 500}), None, id="server_error"
        ),
        pytest.param(True, requests.ConnectionError("offline"), None, id="transport"),
    ],
)
def test_cloud_organization_feature_flags(
    monkeypatch: pytest.MonkeyPatch,
    context_layer: bool,
    error: Exception | None,
    expected: bool | None,
) -> None:
    calls = 0

    def fake_list_agent_workspaces(
        *, credentials: object, organization_id: str | None
    ) -> list[dict[str, object]]:
        nonlocal calls
        calls += 1
        assert organization_id == "organization-id"
        if error is not None:
            raise error
        return [{"id": "workspace-id"}]

    monkeypatch.setattr(
        cloud_organizations.deployment,
        "is_agents_api_available",
        lambda **_: context_layer,
    )
    monkeypatch.setattr(
        cloud_organizations.agents_api_util,
        "list_agent_workspaces",
        fake_list_agent_workspaces,
    )
    organization = CloudOrganization("organization-id", bearer_token="token")

    if expected is None:
        with pytest.raises(type(error)):
            _ = organization.external_access_enabled
        return

    assert organization.external_access_enabled is expected
    assert organization.search_indexing_enabled is expected
    assert calls == (1 if context_layer else 0)


@pytest.mark.parametrize(
    ("with_feature", "limit", "expected_ids"),
    [
        pytest.param(None, None, ["disabled", "enabled"], id="no_filter"),
        pytest.param(
            ConnectorFeature.EXTERNAL_ACCESS, None, ["enabled"], id="external_access"
        ),
        pytest.param(
            ConnectorFeature.SEARCH_INDEXING, None, ["enabled"], id="search_indexing"
        ),
        pytest.param(
            ConnectorFeature.EXTERNAL_ACCESS, 1, ["enabled"], id="limit_after_filter"
        ),
    ],
)
def test_cloud_client_list_organizations_with_feature(
    monkeypatch: pytest.MonkeyPatch,
    with_feature: ConnectorFeature | None,
    limit: int | None,
    expected_ids: list[str],
) -> None:
    organizations = [
        CloudOrganization("disabled", bearer_token="token"),
        CloudOrganization("enabled", bearer_token="token"),
    ]
    client = CloudClient(bearer_token="token")
    monkeypatch.setattr(client, "_fetch_organizations", lambda: organizations)
    monkeypatch.setattr(
        cloud_organizations.deployment, "is_agents_api_available", lambda **_: True
    )

    def fake_list_agent_workspaces(
        *, credentials: object, organization_id: str | None
    ) -> list[dict[str, object]]:
        if organization_id == "disabled":
            raise AirbyteError(context={"status_code": 403})
        return [{"id": "workspace-id"}]

    monkeypatch.setattr(
        cloud_organizations.agents_api_util,
        "list_agent_workspaces",
        fake_list_agent_workspaces,
    )

    result = client.list_organizations(with_feature=with_feature, limit=limit)

    assert [organization.organization_id for organization in result] == expected_ids


def test_mcp_list_cloud_organizations_reports_feature_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_organization_features(monkeypatch, enabled=True)
    captured: dict[str, object] = {}

    class DiscoveryClient:
        def list_organizations(self, **kwargs: object) -> list[CloudOrganization]:
            captured.update(kwargs)
            return [CloudOrganization("organization-id", bearer_token="token")]

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.list_cloud_organizations(
        None, with_feature=ConnectorFeature.EXTERNAL_ACCESS
    )

    assert captured["with_feature"] is ConnectorFeature.EXTERNAL_ACCESS
    assert result.organizations[0].external_access_enabled is True
    assert result.organizations[0].search_indexing_enabled is True


def test_cloud_organization_fetch_returns_cached_info_after_refresh_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses: list[dict[str, object] | Exception] = [
        {"organizationName": "cached"},
        RuntimeError("temporary error"),
    ]

    def fake_get_organization_info(**_: object) -> dict[str, object]:
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(api_util, "get_organization_info", fake_get_organization_info)
    organization = CloudOrganization("organization-id", bearer_token="token")

    assert organization._fetch_organization_info() == {"organizationName": "cached"}  # noqa: SLF001
    assert organization._fetch_organization_info(force_refresh=True) == {  # noqa: SLF001
        "organizationName": "cached"
    }


def test_cloud_organization_get_billing_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        api_util,
        "get_organization_info",
        lambda **_: {
            "billing": {
                "paymentStatus": "okay",
                "subscriptionStatus": "subscribed",
            }
        },
    )
    organization = CloudOrganization(organization_id="organization-id")
    result = organization.get_billing_status()
    assert result.payment_status == "okay"
    assert result.subscription_status == "subscribed"
    assert result.is_account_locked is False


def test_cloud_organization_get_billing_status_requires_billing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util, "get_organization_info", lambda **_: {"organizationId": "org-1"}
    )
    organization = CloudOrganization(organization_id="organization-id")
    with pytest.raises(AirbyteError, match="billing details"):
        organization.get_billing_status()


def test_cloud_organization_get_billing_status_wraps_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def get_organization_info(**_: object) -> None:
        raise requests.ConnectionError("reset")

    monkeypatch.setattr(api_util, "get_organization_info", get_organization_info)
    organization = CloudOrganization(organization_id="organization-id")
    with pytest.raises(AirbyteError, match="Failed to retrieve organization billing"):
        organization.get_billing_status()
