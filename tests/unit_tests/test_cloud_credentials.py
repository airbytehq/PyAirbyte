# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import os

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

    CloudClient(bearer_token="token").list_workspaces(limit=3)

    assert captured_limit == 3


def test_cloud_client_list_workspaces_applies_name_contains_to_non_org_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_filter = None

    def fake_list_workspaces(
        *,
        name_filter: object = None,
        **_: object,
    ) -> list[object]:
        nonlocal captured_filter
        captured_filter = name_filter
        return []

    monkeypatch.setattr(api_util, "list_workspaces", fake_list_workspaces)

    CloudClient(bearer_token="token").list_workspaces(name_contains="target")

    assert captured_filter is not None
    assert captured_filter("a-target-workspace")
    assert not captured_filter("other-workspace")


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

    organization = CloudClient(
        bearer_token="token",
        organization_id="default-org",
    ).get_organization()

    assert organization.organization_id == "default-org"


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


def test_cloud_client_get_organization_reuses_list_organizations(
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
    monkeypatch.setattr(client, "list_organizations", lambda: organizations)

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
        def list_organizations(self) -> list[CloudOrganization]:
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


@pytest.mark.parametrize(
    ("workspaces_or_error", "expect_message"),
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
            id="org-less-public-api",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 401}),
            True,
            id="unauthorized",
        ),
        pytest.param(
            AirbyteError(context={"status_code": 403}),
            True,
            id="forbidden",
        ),
    ],
)
def test_mcp_list_cloud_workspaces_discovery(
    monkeypatch: pytest.MonkeyPatch,
    workspaces_or_error: list[CloudWorkspaceInfo] | AirbyteError,
    expect_message: bool,
) -> None:
    captured_organization_id: str | None = "unset"

    class DiscoveryClient:
        def list_workspaces(
            self, *, organization_id: str | None = None, **_: object
        ) -> list[CloudWorkspaceInfo]:
            nonlocal captured_organization_id
            captured_organization_id = organization_id
            if isinstance(workspaces_or_error, AirbyteError):
                raise workspaces_or_error
            return workspaces_or_error

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
        assert result.message is None


def test_mcp_resolve_organization_id_skips_lookup_when_id_provided() -> None:
    class FailingClient:
        def get_organization(self, **_: object) -> object:
            pytest.fail("get_organization should not be called")

    resolved_organization_id = mcp_cloud._resolve_organization_id(  # noqa: SLF001
        organization_id="organization-id",
        organization_name=None,
        client=FailingClient(),
    )

    assert resolved_organization_id == "organization-id"


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
