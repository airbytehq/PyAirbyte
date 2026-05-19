# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import json
from pathlib import Path
import sys
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import pytest
import responses

from airbyte.cloud import credentials as cloud_credentials
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.secrets.base import SecretString


def _read_saved_settings(credentials_file_path: Path) -> dict[str, object]:
    parsed = json.loads(credentials_file_path.read_text(encoding="utf-8"))
    settings = parsed["settings"]
    assert isinstance(settings, dict)
    return settings


def _clear_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in (
        cloud_credentials.BEARER_TOKEN_ENV_VAR,
        cloud_credentials.CLOUD_BEARER_TOKEN_ENV_VAR,
        cloud_credentials.CLIENT_ID_ENV_VAR,
        cloud_credentials.CLOUD_CLIENT_ID_ENV_VAR,
        cloud_credentials.CLIENT_SECRET_ENV_VAR,
        cloud_credentials.CLOUD_CLIENT_SECRET_ENV_VAR,
        cloud_credentials.PUBLIC_API_ROOT_ENV_VAR,
        cloud_credentials.CLOUD_API_ROOT_ENV_VAR,
        cloud_credentials.CONFIG_API_ROOT_ENV_VAR,
        cloud_credentials.CLOUD_CONFIG_API_ROOT_ENV_VAR,
        cloud_credentials.WORKSPACE_ID_ENV_VAR,
        cloud_credentials.CLOUD_WORKSPACE_ID_ENV_VAR,
        cloud_credentials.ORGANIZATION_ID_ENV_VAR,
        cloud_credentials.CLOUD_ORGANIZATION_ID_ENV_VAR,
    ):
        monkeypatch.delenv(env_var, raising=False)


def test_login_with_client_credentials_writes_shared_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credentials_file_path = tmp_path / ".airbyte-cli" / "settings.json"

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

    result = cloud_credentials.login_with_client_credentials(
        client_id="test-client-id",
        client_secret="test-client-secret",
        organization_id="test-org-id",
        airbyte_api_root="https://api.example.com/v1",
        config_api_root="https://config.example.com/api/v1",
        credentials_file_path=credentials_file_path,
    )

    saved_settings = _read_saved_settings(credentials_file_path)
    assert result.credentials_file_path == credentials_file_path
    assert result.airbyte_api_root == "https://api.example.com/v1"
    assert result.config_api_root == "https://config.example.com/api/v1"
    assert result.organization_id == "test-org-id"
    assert saved_settings == {
        "credentials": {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        },
        "organization_id": "test-org-id",
        "airbyte_api_root": "https://api.example.com/v1",
        "config_api_root": "https://config.example.com/api/v1",
        "workspace": "default",
        "telemetry_enabled": True,
        "version_check_enabled": True,
    }
    if sys.platform != "win32":
        assert credentials_file_path.stat().st_mode & 0o777 == 0o600
        assert credentials_file_path.parent.stat().st_mode & 0o777 == 0o700


def test_resolve_cloud_credentials_reads_shared_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_cloud_env(monkeypatch)
    credentials_file_path = tmp_path / ".airbyte-cli" / "settings.json"
    credentials_file_path.parent.mkdir()
    credentials_file_path.write_text(
        json.dumps({
            "settings": {
                "credentials": {
                    "client_id": "file-client-id",
                    "client_secret": "file-client-secret",
                },
                "organization_id": "file-org-id",
            }
        }),
        encoding="utf-8",
    )

    credentials = cloud_credentials.resolve_cloud_credentials(
        credentials_file_path=credentials_file_path,
    )

    assert str(credentials.client_id) == "file-client-id"
    assert str(credentials.client_secret) == "file-client-secret"
    assert credentials.organization_id == "file-org-id"


def _simulate_keycloak_redirect(url: str) -> bool:
    parsed_url = urlparse(url)
    query = parse_qs(parsed_url.query)
    redirect_uri = query["redirect_uri"][0]
    state = query["state"][0]
    with urlopen(
        f"{redirect_uri}?code=test-auth-code&state={state}", timeout=5
    ) as response:
        assert response.status == 200
    return True


def _request_body(call_index: int) -> str:
    body = responses.calls[call_index].request.body
    if isinstance(body, bytes):
        return body.decode()
    if isinstance(body, str):
        return body
    raise AssertionError("Expected request body.")


@responses.activate
def test_login_with_browser_bootstraps_credentials(tmp_path: Path) -> None:
    credentials_file_path = tmp_path / ".airbyte-cli" / "settings.json"
    credentials_file_path.parent.mkdir()
    credentials_file_path.write_text(
        json.dumps({
            "settings": {
                "credentials": {
                    "client_id": "previous-client-id",
                    "client_secret": "previous-client-secret",
                },
                "organization_id": "previous-org-id",
                "workspace": "Existing Workspace",
                "allow_destructive": True,
                "telemetry_enabled": False,
                "is_internal_user": True,
                "version_check_enabled": False,
            }
        }),
        encoding="utf-8",
    )
    api_root = cloud_credentials.AIRBYTE_AI_API_ROOT
    responses.add(
        responses.POST,
        f"{cloud_credentials.KEYCLOAK_BASE_URL}/protocol/openid-connect/token",
        json={"access_token": "keycloak-access-token"},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{api_root}/internal/account/enrollment-status",
        json={"is_enrolled": True, "organization_id": "initial-org-id"},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{api_root}/internal/account/organizations",
        json={
            "organizations": [
                {"id": "selected-org-id", "organization_name": "Selected Org"}
            ]
        },
        status=200,
    )
    responses.add(
        responses.POST,
        f"{api_root}/internal/account/applications",
        json={
            "client_id": "airbyte-client-id",
            "client_secret": "airbyte-client-secret",
        },
        status=200,
    )

    result = cloud_credentials.login_with_browser(
        credentials_file_path=credentials_file_path,
        open_url=_simulate_keycloak_redirect,
        timeout_seconds=5,
    )

    saved_settings = _read_saved_settings(credentials_file_path)
    assert result.credentials_file_path == credentials_file_path
    assert result.airbyte_api_root == api_root
    assert result.organization_id == "selected-org-id"
    assert saved_settings == {
        "credentials": {
            "client_id": "airbyte-client-id",
            "client_secret": "airbyte-client-secret",
        },
        "organization_id": "selected-org-id",
        "airbyte_api_root": api_root,
        "config_api_root": cloud_credentials.CLOUD_CONFIG_API_ROOT,
        "workspace": "Existing Workspace",
        "allow_destructive": True,
        "telemetry_enabled": False,
        "is_internal_user": True,
        "version_check_enabled": False,
    }
    token_request = parse_qs(_request_body(0))
    token_headers = responses.calls[0].request.headers
    assert token_request["client_id"] == [cloud_credentials.KEYCLOAK_CLIENT_ID]
    assert token_request["grant_type"] == ["authorization_code"]
    assert token_request["code"] == ["test-auth-code"]
    assert "code_verifier" in token_request
    assert token_headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert token_headers["User-Agent"] == cloud_credentials.PYAIRBYTE_USER_AGENT
    assert (
        responses.calls[1].request.headers["Authorization"]
        == "Bearer keycloak-access-token"
    )
    assert (
        responses.calls[2].request.headers["Authorization"]
        == "Bearer keycloak-access-token"
    )
    assert responses.calls[2].request.headers["X-Organization-Id"] == "initial-org-id"
    assert (
        responses.calls[3].request.headers["Authorization"]
        == "Bearer keycloak-access-token"
    )
    assert responses.calls[3].request.headers["X-Organization-Id"] == "selected-org-id"


@responses.activate
def test_login_with_browser_token_exchange_non_json_error(tmp_path: Path) -> None:
    credentials_file_path = tmp_path / ".airbyte-cli" / "settings.json"
    responses.add(
        responses.POST,
        f"{cloud_credentials.KEYCLOAK_BASE_URL}/protocol/openid-connect/token",
        body="<!doctype html><title>403</title>",
        content_type="text/html; charset=UTF-8",
        status=403,
    )

    with pytest.raises(AirbyteError, match="Keycloak token exchange failed"):
        cloud_credentials.login_with_browser(
            credentials_file_path=credentials_file_path,
            open_url=_simulate_keycloak_redirect,
            timeout_seconds=5,
        )


@responses.activate
def test_login_with_browser_uses_organization_override(tmp_path: Path) -> None:
    credentials_file_path = tmp_path / ".airbyte-cli" / "settings.json"
    api_root = cloud_credentials.AIRBYTE_AI_API_ROOT
    responses.add(
        responses.POST,
        f"{cloud_credentials.KEYCLOAK_BASE_URL}/protocol/openid-connect/token",
        json={"access_token": "keycloak-access-token"},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{api_root}/internal/account/enrollment-status",
        json={"is_enrolled": True, "organization_id": "initial-org-id"},
        status=200,
    )
    responses.add(
        responses.POST,
        f"{api_root}/internal/account/applications",
        json={
            "client_id": "airbyte-client-id",
            "client_secret": "airbyte-client-secret",
        },
        status=200,
    )

    result = cloud_credentials.login_with_browser(
        organization_id="override-org-id",
        credentials_file_path=credentials_file_path,
        open_url=_simulate_keycloak_redirect,
        timeout_seconds=5,
    )

    assert result.organization_id == "override-org-id"
    assert len(responses.calls) == 3
    assert "X-Organization-Id" not in responses.calls[1].request.headers
    assert (
        responses.calls[2].request.headers["Authorization"]
        == "Bearer keycloak-access-token"
    )
    assert responses.calls[2].request.headers["X-Organization-Id"] == "override-org-id"


def test_login_without_client_credentials_raises_client_credentials_error() -> None:
    with pytest.raises(PyAirbyteInputError, match="Client ID and client secret"):
        cloud_credentials.login_with_client_credentials()


def test_login_with_partial_client_credentials_raises() -> None:
    with pytest.raises(PyAirbyteInputError, match="Client ID and client secret"):
        cloud_credentials.login_with_client_credentials(client_id="test-client-id")


def test_self_managed_login_requires_both_api_roots() -> None:
    with pytest.raises(
        PyAirbyteInputError, match="Self-managed login requires both API roots"
    ):
        cloud_credentials.login_with_client_credentials(
            client_id="test-client-id",
            client_secret="test-client-secret",
            airbyte_api_root="https://api.example.com/v1",
        )
