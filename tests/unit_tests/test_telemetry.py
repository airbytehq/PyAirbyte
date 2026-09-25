# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for PyAirbyte telemetry env flags."""

from __future__ import annotations

import pytest

from airbyte._util import telemetry
from airbyte.constants import CLOUD_API_ROOT_ENV_VAR, CLOUD_CONFIG_API_ROOT_ENV_VAR


@pytest.fixture(autouse=True)
def clear_env_flags_cache() -> None:
    """Reset the cached env flags around each test."""
    telemetry.get_env_flags.cache_clear()
    yield
    telemetry.get_env_flags.cache_clear()


def test_env_flags_report_cloud_deployment_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report CLOUD deployment when no Cloud API roots are overridden."""
    monkeypatch.delenv(CLOUD_API_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(CLOUD_CONFIG_API_ROOT_ENV_VAR, raising=False)

    assert telemetry.get_env_flags()["DEPLOYMENT"] == "CLOUD"


def test_env_flags_report_oss_deployment_for_custom_api_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report OSS deployment when the Cloud API root is overridden."""
    monkeypatch.delenv(CLOUD_CONFIG_API_ROOT_ENV_VAR, raising=False)
    monkeypatch.setenv(
        CLOUD_API_ROOT_ENV_VAR, "https://airbyte.example.com/api/public/v1"
    )

    assert telemetry.get_env_flags()["DEPLOYMENT"] == "OSS"
