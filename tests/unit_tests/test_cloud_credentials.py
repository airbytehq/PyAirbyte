# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path
import sys

import pytest
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


def test_login_without_client_credentials_raises_interactive_flow_error() -> None:
    with pytest.raises(PyAirbyteInputError, match="Interactive Airbyte Cloud login"):
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
