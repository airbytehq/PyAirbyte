# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for deployment detection helpers."""

from __future__ import annotations

import pytest

from airbyte._util import deployment
from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT


def test_get_overridden_cloud_api_roots() -> None:
    """Return only non-public, non-blank Cloud API roots."""
    assert (
        deployment.get_overridden_cloud_api_roots(
            public_api_root=None,
            config_api_root=None,
        )
        == {}
    )
    assert (
        deployment.get_overridden_cloud_api_roots(
            public_api_root="",
            config_api_root=f"{CLOUD_CONFIG_API_ROOT}/",
        )
        == {}
    )
    assert deployment.get_overridden_cloud_api_roots(
        public_api_root="https://airbyte.example.com/api/public/v1",
        config_api_root=CLOUD_CONFIG_API_ROOT,
    ) == {"api_root": "https://airbyte.example.com/api/public/v1"}


def test_is_agents_api_available(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recognize public Cloud roots and explicit Agents API roots."""
    monkeypatch.delenv("AIRBYTE_AGENTS_API_URL", raising=False)
    assert deployment.is_agents_api_available(
        public_api_root=CLOUD_API_ROOT,
        config_api_root=None,
    )
    assert not deployment.is_agents_api_available(
        public_api_root="https://airbyte.example.com/api/public/v1",
        config_api_root=None,
    )

    monkeypatch.setenv("AIRBYTE_AGENTS_API_URL", "https://agents.example.com/api/v1")
    assert deployment.is_agents_api_available(
        public_api_root="https://airbyte.example.com/api/public/v1",
        config_api_root=None,
    )


@pytest.mark.parametrize(
    ("public_api_root", "config_api_root"),
    [
        pytest.param("   ", None, id="whitespace_public_root"),
        pytest.param(None, "   ", id="whitespace_config_root"),
    ],
)
def test_whitespace_cloud_roots_are_unset(
    monkeypatch: pytest.MonkeyPatch,
    public_api_root: str | None,
    config_api_root: str | None,
) -> None:
    """Whitespace-only Cloud API roots count as unset."""
    monkeypatch.delenv("AIRBYTE_AGENTS_API_URL", raising=False)

    assert (
        deployment.get_overridden_cloud_api_roots(
            public_api_root=public_api_root,
            config_api_root=config_api_root,
        )
        == {}
    )
    assert deployment.is_agents_api_available(
        public_api_root=public_api_root,
        config_api_root=config_api_root,
    )
