# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import pytest

from airbyte import constants
from airbyte._util import api_util
from airbyte.cloud import _credentials as cloud_credentials
from airbyte.cloud.client import CloudClient
from airbyte.cloud.organizations import CloudOrganization
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
            {"name": "miss"},
            {"name": "target-one"},
            {"name": "target-two"},
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
    assert result == [{"name": "target-one"}]


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
