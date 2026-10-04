# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for PyAirbyte telemetry env flags."""

from __future__ import annotations

import pytest

from airbyte._util import telemetry
from airbyte.constants import (
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
)

_CLOUD_ENV_VARS = (
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
)


@pytest.fixture(autouse=True)
def clear_env_flags_cache() -> None:
    """Reset the cached env flags around each test."""
    telemetry.get_env_flags.cache_clear()
    yield
    telemetry.get_env_flags.cache_clear()


def _clear_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in _CLOUD_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


def test_env_flags_omit_deployment_without_cloud_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omit the DEPLOYMENT flag when no Cloud configuration is present."""
    _clear_cloud_env(monkeypatch)

    assert "DEPLOYMENT" not in telemetry.get_env_flags()


def test_env_flags_report_cloud_deployment_with_cloud_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report CLOUD deployment when Cloud credentials are set and roots are default."""
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv(CLOUD_CLIENT_ID_ENV_VAR, "test-client-id")

    assert telemetry.get_env_flags()["DEPLOYMENT"] == "CLOUD"


def test_env_flags_report_oss_deployment_for_custom_api_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report OSS deployment when the Cloud API root is overridden."""
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv(
        CLOUD_API_ROOT_ENV_VAR, "https://airbyte.example.com/api/public/v1"
    )

    assert telemetry.get_env_flags()["DEPLOYMENT"] == "OSS"
