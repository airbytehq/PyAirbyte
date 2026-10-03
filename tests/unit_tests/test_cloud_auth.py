# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Cloud authentication settings resolution."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from airbyte import constants
from airbyte.cloud import _auth
from airbyte.exceptions import PyAirbyteSecretNotFoundError
from airbyte.secrets import config as secrets_config
from airbyte.secrets.base import SecretManager, SecretString


_CLOUD_SETTINGS_ENV_VARS = (
    constants.AIRBYTE_CLIENT_ID_ENV_VAR,
    constants.AIRBYTE_CLIENT_SECRET_ENV_VAR,
    constants.AIRBYTE_BEARER_TOKEN_ENV_VAR,
    constants.AIRBYTE_WORKSPACE_ID_ENV_VAR,
    constants.AIRBYTE_ORGANIZATION_ID_ENV_VAR,
    constants.CLOUD_CLIENT_ID_ENV_VAR,
    constants.CLOUD_CLIENT_SECRET_ENV_VAR,
    constants.CLOUD_BEARER_TOKEN_ENV_VAR,
    constants.CLOUD_WORKSPACE_ID_ENV_VAR,
    constants.CLOUD_ORGANIZATION_ID_ENV_VAR,
    constants.CLOUD_API_ROOT_ENV_VAR,
    constants.CLOUD_CONFIG_API_ROOT_ENV_VAR,
)


@pytest.fixture(autouse=True)
def clear_cloud_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in _CLOUD_SETTINGS_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


@pytest.mark.parametrize(
    ("resolver", "env_var", "input_value", "expected"),
    [
        pytest.param(
            _auth.resolve_cloud_client_id,
            constants.CLOUD_CLIENT_ID_ENV_VAR,
            "explicit-client-id",
            "explicit-client-id",
            id="client-id",
        ),
        pytest.param(
            _auth.resolve_cloud_workspace_id,
            constants.CLOUD_WORKSPACE_ID_ENV_VAR,
            "explicit-workspace-id",
            "explicit-workspace-id",
            id="workspace-id",
        ),
        pytest.param(
            _auth.resolve_cloud_api_url,
            constants.CLOUD_API_ROOT_ENV_VAR,
            "https://explicit.example",
            "https://explicit.example",
            id="api-url",
        ),
    ],
)
def test_explicit_cloud_auth_input_precedes_environment(
    monkeypatch: pytest.MonkeyPatch,
    resolver: Callable[[str], SecretString | str],
    env_var: str,
    input_value: str,
    expected: str,
) -> None:
    monkeypatch.setenv(env_var, "https://environment.example")

    assert str(resolver(input_value)) == expected


@pytest.mark.parametrize(
    ("resolver", "input_value"),
    [
        (_auth.resolve_cloud_bearer_token, "explicit-token"),
        (_auth.resolve_cloud_client_secret, "explicit-secret"),
        (_auth.resolve_cloud_client_id, "explicit-client-id"),
        (_auth.resolve_cloud_workspace_id, "explicit-workspace-id"),
        (_auth.resolve_cloud_api_url, "https://explicit.example"),
        (_auth.resolve_cloud_config_api_url, "https://explicit-config.example"),
    ],
)
def test_explicit_cloud_auth_input_skips_settings(
    monkeypatch: pytest.MonkeyPatch,
    resolver: Callable[[str], SecretString | str | None],
    input_value: str,
) -> None:
    monkeypatch.setattr(
        _auth,
        "AirbyteCloudSettings",
        lambda: pytest.fail("Explicit values should not load AirbyteCloudSettings"),
    )

    assert str(resolver(input_value)) == input_value


def test_missing_client_id_raises_secret_not_found(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(PyAirbyteSecretNotFoundError) as exc_info:
        _auth.resolve_cloud_client_id()

    assert exc_info.value.secret_name == constants.CLOUD_CLIENT_ID_ENV_VAR
    assert exc_info.value.sources == ["env", "dotenv"]


def test_cloud_auth_does_not_consult_registered_secret_managers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class UnexpectedSecretManager(SecretManager):
        def get_secret(self, secret_name: str) -> None:
            pytest.fail(f"Unexpected secret manager lookup for {secret_name}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(secrets_config, "_SECRETS_SOURCES", [])
    secrets_config.register_secret_manager(UnexpectedSecretManager())

    with pytest.raises(PyAirbyteSecretNotFoundError):
        _auth.resolve_cloud_client_id()
