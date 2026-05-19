# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path
import sys

import pytest
import yaml

from airbyte import constants
from airbyte._util import api_util
from airbyte.cloud import _credentials as cloud_credentials
from airbyte.cloud.client import CloudClient
from airbyte.cloud.organizations import CloudOrganization
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString


def test_login_with_client_credentials_writes_bearer_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials_file_path = tmp_path / "credentials"

    def fake_get_bearer_token(
        *,
        client_id: SecretString,
        client_secret: SecretString,
        api_root: str,
    ) -> str:
        assert str(client_id) == "test-client-id"
        assert str(client_secret) == "test-client-secret"
        assert api_root == "https://api.example.com/v1"
        return "test-bearer-token"

    monkeypatch.setattr(cloud_credentials, "get_bearer_token", fake_get_bearer_token)

    result = cloud_credentials._AirbyteCredentials(
        client_id=SecretString("test-client-id"),
        client_secret=SecretString("test-client-secret"),
        bearer_token=None,
        public_api_root="https://api.example.com/v1",
        config_api_root="https://config.example.com/api/v1",
    ).login(credentials_file_path=credentials_file_path)

    saved_credentials = yaml.safe_load(
        credentials_file_path.read_text(encoding="utf-8")
    )
    assert result.credentials_file_path == credentials_file_path
    assert result.airbyte_api_root == "https://api.example.com/v1"
    assert result.config_api_root == "https://config.example.com/api/v1"
    assert saved_credentials == {
        "airbyte_api_root": "https://api.example.com/v1",
        "bearer_token": "test-bearer-token",
        "config_api_root": "https://config.example.com/api/v1",
    }
    if sys.platform != "win32":
        assert credentials_file_path.stat().st_mode & 0o777 == 0o600


def test_login_without_client_credentials_raises_interactive_flow_error() -> None:
    with pytest.raises(PyAirbyteInputError, match="Interactive Airbyte Cloud login"):
        cloud_credentials._AirbyteCredentials(
            client_id=None,
            client_secret=None,
            bearer_token=None,
            public_api_root=constants.CLOUD_API_ROOT,
            config_api_root=constants.CLOUD_CONFIG_API_ROOT,
        ).login()


def test_login_with_partial_client_credentials_raises() -> None:
    with pytest.raises(PyAirbyteInputError, match="Client ID and client secret"):
        cloud_credentials._AirbyteCredentials(
            client_id=SecretString("test-client-id"),
            client_secret=None,
            bearer_token=None,
            public_api_root=constants.CLOUD_API_ROOT,
            config_api_root=constants.CLOUD_CONFIG_API_ROOT,
        ).login()


def test_self_managed_login_requires_both_api_roots() -> None:
    with pytest.raises(
        PyAirbyteInputError, match="Self-managed login requires both API roots"
    ):
        cloud_credentials._AirbyteCredentials(
            client_id=SecretString("test-client-id"),
            client_secret=SecretString("test-client-secret"),
            bearer_token=None,
            public_api_root="https://api.example.com/v1",
            config_api_root=None,
        ).login()


def test_login_with_client_credentials_uses_cloud_default_roots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials_file_path = tmp_path / "credentials"

    def fake_get_bearer_token(
        *,
        client_id: SecretString,
        client_secret: SecretString,
        api_root: str,
    ) -> str:
        assert str(client_id) == "test-client-id"
        assert str(client_secret) == "test-client-secret"
        assert api_root == constants.CLOUD_API_ROOT
        return "test-bearer-token"

    monkeypatch.setattr(cloud_credentials, "get_bearer_token", fake_get_bearer_token)

    result = cloud_credentials._AirbyteCredentials(
        client_id=SecretString("test-client-id"),
        client_secret=SecretString("test-client-secret"),
        bearer_token=None,
        public_api_root=constants.CLOUD_API_ROOT,
        config_api_root=None,
    ).login(credentials_file_path=credentials_file_path)

    assert result.airbyte_api_root == constants.CLOUD_API_ROOT
    assert result.config_api_root == constants.CLOUD_CONFIG_API_ROOT


def test_cloud_client_login_uses_cloud_default_roots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials_file_path = tmp_path / "credentials"

    def fake_get_bearer_token(
        *,
        client_id: SecretString,
        client_secret: SecretString,
        api_root: str,
    ) -> str:
        assert str(client_id) == "test-client-id"
        assert str(client_secret) == "test-client-secret"
        assert api_root == constants.CLOUD_API_ROOT
        return "test-bearer-token"

    monkeypatch.setattr(cloud_credentials, "get_bearer_token", fake_get_bearer_token)

    result = CloudClient(
        client_id=SecretString("test-client-id"),
        client_secret=SecretString("test-client-secret"),
    ).login(credentials_file_path=credentials_file_path)

    assert result.airbyte_api_root == constants.CLOUD_API_ROOT
    assert result.config_api_root == constants.CLOUD_CONFIG_API_ROOT


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

    credentials = cloud_credentials._AirbyteCredentials.from_auth()

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
