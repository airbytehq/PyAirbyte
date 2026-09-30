# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for deployment detection helpers."""

from __future__ import annotations

import pytest

from airbyte._util import deployment
from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT


def test_is_public_cloud_for_default_roots() -> None:
    """Recognize public Cloud roots, including trailing slashes."""
    assert deployment.is_public_cloud()
    assert deployment.is_public_cloud(
        public_api_root=f"{CLOUD_API_ROOT}/",
        config_api_root=f"{CLOUD_CONFIG_API_ROOT}/",
    )


def test_is_public_cloud_for_custom_api_root() -> None:
    """Reject a custom public API root."""
    assert not deployment.is_public_cloud(
        public_api_root="https://airbyte.example.com/api/public/v1",
    )


def test_is_public_cloud_for_custom_config_root() -> None:
    """Reject a custom Config API root."""
    assert not deployment.is_public_cloud(
        config_api_root="https://airbyte.example.com/api/v1",
    )


def test_cloud_api_environment_override_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the Cloud API environment override before the explicit root."""
    monkeypatch.setenv(
        "AIRBYTE_CLOUD_API_URL", "https://airbyte.example.com/api/public/v1"
    )

    assert not deployment.is_public_cloud(public_api_root=CLOUD_API_ROOT)


def test_is_agents_api_available() -> None:
    """True for public Cloud roots or an explicit Config API root; False otherwise."""
    assert deployment.is_agents_api_available()
    assert deployment.is_agents_api_available(
        public_api_root=CLOUD_API_ROOT,
        config_api_root=CLOUD_CONFIG_API_ROOT,
    )
    assert deployment.is_agents_api_available(
        public_api_root="https://airbyte.example.com/api/public/v1",
        config_api_root="https://airbyte.example.com/api/v1",
    )
    assert not deployment.is_agents_api_available(
        public_api_root="https://airbyte.example.com/api/public/v1",
    )


@pytest.mark.parametrize("override", ["", "   "])
def test_blank_config_api_override_is_unset(
    monkeypatch: pytest.MonkeyPatch,
    override: str,
) -> None:
    """Treat blank Config API overrides as unset."""
    monkeypatch.setenv("AIRBYTE_CLOUD_CONFIG_API_URL", override)

    assert not deployment.is_agents_api_available(
        public_api_root="https://airbyte.example.com/api/public/v1",
    )


def test_config_api_env_override_enables_agents_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An `AIRBYTE_CLOUD_CONFIG_API_URL` override enables the Context layer."""
    monkeypatch.setenv(
        "AIRBYTE_CLOUD_CONFIG_API_URL", "https://config.example.com/api/v1"
    )

    assert deployment.is_agents_api_available(
        public_api_root="https://airbyte.example.com/api/public/v1",
    )
