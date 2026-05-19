# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path
import sys
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import pytest
import responses
import yaml

from airbyte.cloud import credentials as cloud_credentials
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

    result = cloud_credentials.login_with_client_credentials(
        client_id="test-client-id",
        client_secret="test-client-secret",
        airbyte_api_root="https://api.example.com/v1",
        config_api_root="https://config.example.com/api/v1",
        credentials_file_path=credentials_file_path,
    )

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


def _simulate_keycloak_redirect(url: str) -> bool:
    parsed_url = urlparse(url)
    query = parse_qs(parsed_url.query)
    redirect_uri = query["redirect_uri"][0]
    state = query["state"][0]
    with urlopen(f"{redirect_uri}?code=test-auth-code&state={state}", timeout=5) as response:
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
    credentials_file_path = tmp_path / "credentials"
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
        json={"client_id": "airbyte-client-id", "client_secret": "airbyte-client-secret"},
        status=200,
    )

    result = cloud_credentials.login_with_browser(
        credentials_file_path=credentials_file_path,
        open_url=_simulate_keycloak_redirect,
        timeout_seconds=5,
    )

    saved_credentials = yaml.safe_load(
        credentials_file_path.read_text(encoding="utf-8")
    )
    assert result.credentials_file_path == credentials_file_path
    assert result.airbyte_api_root == api_root
    assert result.organization_id == "selected-org-id"
    assert saved_credentials == {
        "airbyte_api_root": api_root,
        "client_id": "airbyte-client-id",
        "client_secret": "airbyte-client-secret",
        "config_api_root": cloud_credentials.CLOUD_CONFIG_API_ROOT,
        "organization_id": "selected-org-id",
    }
    token_request = parse_qs(_request_body(0))
    assert token_request["client_id"] == [cloud_credentials.KEYCLOAK_CLIENT_ID]
    assert token_request["grant_type"] == ["authorization_code"]
    assert token_request["code"] == ["test-auth-code"]
    assert "code_verifier" in token_request
    assert responses.calls[2].request.headers["X-Organization-Id"] == "initial-org-id"
    assert responses.calls[3].request.headers["X-Organization-Id"] == "selected-org-id"


@responses.activate
def test_login_with_browser_uses_organization_override(tmp_path: Path) -> None:
    credentials_file_path = tmp_path / "credentials"
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
        json={"client_id": "airbyte-client-id", "client_secret": "airbyte-client-secret"},
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
