# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import os
from collections.abc import Callable
from typing import NoReturn

import pytest
from airbyte_api import models

from airbyte import constants
from airbyte._util import api_util
from airbyte.cloud import _credentials as cloud_credentials
from airbyte.cloud.client import CloudClient
from airbyte.cloud.models import CloudWorkspaceInfo
from airbyte.cloud.organizations import CloudOrganization
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

    CloudClient(bearer_token="token").list_workspaces(
        limit=3,
        all_organizations=True,
    )

    assert captured_limit == 3


@pytest.mark.parametrize(
    ("client_kwargs", "request_kwargs", "failure", "expected_message"),
    [
        pytest.param(
            {},
            {},
            "membership",
            None,
            id="membership-failure-falls-back",
        ),
        pytest.param(
            {"workspace_id": "configured-workspace-id"},
            {},
            "workspace-parent",
            None,
            id="configured-workspace-parent-failure-falls-back",
        ),
        pytest.param(
            {},
            {"workspace_id": "explicit-workspace-id"},
            "workspace-parent",
            "workspace lookup failed",
            id="explicit-workspace-failure-propagates",
        ),
    ],
)
def test_cloud_client_list_workspaces_handles_ambient_resolution_failures(
    monkeypatch: pytest.MonkeyPatch,
    client_kwargs: dict[str, str],
    request_kwargs: dict[str, str],
    failure: str,
    expected_message: str | None,
) -> None:
    captured: dict[str, object] = {}

    def fake_list_workspaces(**kwargs: object) -> list[object]:
        captured.update(kwargs)
        return []

    client = CloudClient(bearer_token="token", **client_kwargs)
    if failure == "membership":
        monkeypatch.setattr(
            client,
            "_get_membership_organization_ids",
            _raise(AirbyteError(message="membership failed")),
        )
    else:
        monkeypatch.setattr(
            client,
            "_get_workspace_parent_organization_id",
            _raise(PyAirbyteInputError(message="workspace lookup failed")),
        )
        monkeypatch.setattr(client, "_get_membership_organization_ids", lambda: ())
    monkeypatch.setattr(api_util, "list_workspaces", fake_list_workspaces)

    if expected_message is None:
        assert client.list_workspaces(**request_kwargs) == []
        assert captured["workspace_id"] == ""
    else:
        with pytest.raises(PyAirbyteInputError, match=expected_message):
            client.list_workspaces(**request_kwargs)
        assert captured == {}


@pytest.mark.parametrize(
    ("request_kwargs", "expected_message"),
    [
        pytest.param(
            {"all_organizations": True, "organization_id": "organization-id"},
            "all_organizations option cannot be combined",
            id="all-organizations-with-organization",
        ),
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

    result = CloudClient(bearer_token="token").list_workspaces(
        name_contains="TARGET",
        limit=1,
        all_organizations=True,
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
    ).list_workspaces()

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
            {"limit": 3},
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
            {"limit": 3},
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
            {"limit": 3},
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
            {"limit": 3},
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
            {"name_filter": lambda _: True},
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
    client.list_workspaces()
    client.list_workspaces()

    assert captured["organization_id"] == "organization-id"
    assert calls == {"user": 1, "permissions": 1}


def test_cloud_client_list_workspaces_rejects_ambiguous_memberships_with_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_workspace_discovery(
        monkeypatch,
        permissions=[
            {
                "permissionType": "organization_member",
                "organizationId": "organization-1",
            },
            {
                "permissionType": "organization_member",
                "organizationId": "organization-2",
            },
        ],
    )

    def fake_get_organization_info(**kwargs: object) -> dict[str, object]:
        if kwargs["organization_id"] == "organization-2":
            raise AirbyteError(message="Forbidden")
        return {"organizationName": "Organization 1"}

    monkeypatch.setattr(api_util, "get_organization_info", fake_get_organization_info)

    with pytest.raises(PyAirbyteInputError) as exc_info:
        CloudClient(bearer_token="token").list_workspaces()

    assert exc_info.value.context == {
        "organization_ids": ["organization-1", "organization-2"],
        "organization_candidates": [
            {
                "organization_id": "organization-1",
                "organization_name": "Organization 1",
            },
            {"organization_id": "organization-2", "organization_name": None},
        ],
        "total_candidates": 2,
    }


@pytest.mark.parametrize(
    ("permissions", "all_organizations"),
    [
        pytest.param([], False, id="zero_memberships"),
        pytest.param(None, True, id="explicit_opt_in"),
    ],
)
def test_cloud_client_list_workspaces_uses_cross_organization_listing(
    monkeypatch: pytest.MonkeyPatch,
    permissions: list[dict[str, object]] | None,
    all_organizations: bool,
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
        all_organizations=all_organizations,
    )

    assert captured["workspace_id"] == ""
    assert captured["limit"] is None
    assert [workspace.workspace_id for workspace in result] == ["workspace-id"]


@pytest.mark.parametrize(
    ("candidates", "expected_ids", "expected_names", "has_retry_guidance"),
    [
        pytest.param(
            [
                {
                    "organization_id": "organization-1",
                    "organization_name": None,
                },
                {
                    "organization_id": "organization-2",
                    "organization_name": "Organization 2",
                },
            ],
            ["organization-1", "organization-2"],
            [None, "Organization 2"],
            True,
            id="with_available_organizations",
        ),
        pytest.param([], [], [], False, id="without_available_organizations"),
    ],
)
def test_mcp_list_cloud_workspaces_reports_available_organizations(
    monkeypatch: pytest.MonkeyPatch,
    candidates: list[dict[str, str | None]],
    expected_ids: list[str],
    expected_names: list[str | None],
    has_retry_guidance: bool,
) -> None:
    class DiscoveryClient:
        def list_workspaces(self, **_: object) -> list[CloudWorkspaceInfo]:
            message = (
                "Multiple organization memberships were found."
                if candidates
                else "No organization membership was found."
            )
            raise PyAirbyteInputError(
                message=message,
                context={
                    "organization_candidates": candidates,
                },
            )

    monkeypatch.setattr(mcp_cloud, "_get_cloud_client", lambda _: DiscoveryClient())

    result = mcp_cloud.list_cloud_workspaces(
        None,
        organization_id=None,
        organization_name=None,
        name_contains=None,
        limit=None,
    )

    assert result.workspaces == []
    assert result.available_organizations is not None
    assert all(
        isinstance(candidate, mcp_cloud.CloudOrganizationResult)
        for candidate in result.available_organizations
    )
    assert [
        candidate.id for candidate in result.available_organizations
    ] == expected_ids
    assert [
        candidate.name for candidate in result.available_organizations
    ] == expected_names
    retry_guidance = (
        "Retry with an explicit organization ID from the provided list of the "
        "available organizations."
    )
    if has_retry_guidance:
        assert retry_guidance in (result.message or "")
    else:
        assert result.message == "No organization membership was found."


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
        )
    ]


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

    assert captured == {"name_contains": "develop", "limit": 1}
    assert len(result.organizations) == 1
    assert (
        result.message == "Showing the first 1 organizations; more may exist. "
        "Pass `name_contains` to narrow the search, or a larger `limit`."
    )


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
