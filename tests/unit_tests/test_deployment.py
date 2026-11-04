# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for deployment detection helpers."""

from __future__ import annotations

import pytest

from airbyte._util import deployment
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CONFIG_API_ROOT,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
)

_CLOUD_ENV_VARS = (
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
)


def _clear_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in _CLOUD_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


def test_is_airbyte_cloud_for_default_roots() -> None:
    """Recognize public Cloud roots, including trailing slashes."""
    assert deployment.is_airbyte_cloud()
    assert deployment.is_airbyte_cloud(
        public_api_root=f"{CLOUD_API_ROOT}/",
        config_api_root=f"{CLOUD_CONFIG_API_ROOT}/",
    )


def test_is_airbyte_cloud_for_custom_api_root() -> None:
    """Reject a custom public API root."""
    assert not deployment.is_airbyte_cloud(
        public_api_root="https://airbyte.example.com/api/public/v1",
    )


def test_is_airbyte_cloud_for_custom_config_root() -> None:
    """Reject a custom Config API root."""
    assert not deployment.is_airbyte_cloud(
        config_api_root="https://airbyte.example.com/api/v1",
    )


def test_cloud_api_environment_override_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the Cloud API environment override before the explicit root."""
    monkeypatch.setenv(
        CLOUD_API_ROOT_ENV_VAR, "https://airbyte.example.com/api/public/v1"
    )

    assert not deployment.is_airbyte_cloud(public_api_root=CLOUD_API_ROOT)


@pytest.mark.parametrize("override", ["", "   "])
def test_blank_agents_api_override_is_unset(
    monkeypatch: pytest.MonkeyPatch,
    override: str,
) -> None:
    """Treat blank Agents API overrides as unset."""
    monkeypatch.setenv("AIRBYTE_AGENTS_API_URL", override)

    assert deployment.get_agents_api_root_override() is None
    assert not deployment.is_agents_api_available(
        public_api_root="https://airbyte.example.com/api/public/v1",
    )


def test_get_deployment_mode_is_none_without_cloud_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report None when no Cloud credentials or API roots are configured."""
    _clear_cloud_env(monkeypatch)

    assert deployment.get_deployment_mode() is None


def test_get_deployment_mode_for_cloud_credentials_and_default_roots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report CLOUD when Cloud credentials are set and roots are not overridden."""
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv(CLOUD_CLIENT_ID_ENV_VAR, "test-client-id")

    assert deployment.get_deployment_mode() == "CLOUD"


def test_get_deployment_mode_for_custom_api_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report OSS when the public Cloud API root is overridden."""
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv(
        CLOUD_API_ROOT_ENV_VAR, "https://airbyte.example.com/api/public/v1"
    )

    assert deployment.get_deployment_mode() == "OSS"


def test_get_deployment_mode_for_custom_config_api_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report OSS when only the Config API root is overridden."""
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv(
        CLOUD_CONFIG_API_ROOT_ENV_VAR, "https://airbyte.example.com/api/v1"
    )

    assert deployment.get_deployment_mode() == "OSS"
