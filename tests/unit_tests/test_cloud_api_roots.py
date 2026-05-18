# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for cloud API root resolution."""

from __future__ import annotations

import pytest

from airbyte._util import api_util
from airbyte.cloud import CloudWorkspace
from airbyte.secrets.base import SecretString


@pytest.mark.parametrize(
    "api_root,config_api_root,expected",
    [
        pytest.param(
            api_util.CLOUD_API_ROOT,
            None,
            api_util.CLOUD_CONFIG_API_ROOT,
            id="default_cloud_root",
        ),
        pytest.param(
            "https://example.airbyte.com/api/public/v1",
            None,
            "https://example.airbyte.com/api/v1",
            id="infer_self_managed_root",
        ),
        pytest.param(
            "https://example.airbyte.com/airbyte/api/public/v1/",
            None,
            "https://example.airbyte.com/airbyte/api/v1",
            id="infer_self_managed_root_with_prefix",
        ),
        pytest.param(
            "http://localhost:8000/api/public/v1",
            None,
            "http://localhost:8000/api/v1",
            id="infer_local_self_managed_root",
        ),
        pytest.param(
            "https://example.airbyte.com/custom/public",
            "https://example.airbyte.com/custom/config",
            "https://example.airbyte.com/custom/config",
            id="explicit_config_root",
        ),
        pytest.param(
            "https://example.airbyte.com/custom/public",
            "https://example.airbyte.com/custom/config/",
            "https://example.airbyte.com/custom/config",
            id="explicit_config_root_trailing_slash",
        ),
    ],
)
def test_get_config_api_root(
    monkeypatch: pytest.MonkeyPatch,
    api_root: str,
    config_api_root: str | None,
    expected: str,
) -> None:
    monkeypatch.delenv(api_util.CLOUD_CONFIG_API_ROOT_ENV_VAR, raising=False)

    assert (
        api_util.get_config_api_root(api_root, config_api_root=config_api_root)
        == expected
    )


def test_get_config_api_root_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        api_util.CLOUD_CONFIG_API_ROOT_ENV_VAR,
        "https://example.airbyte.com/env/config/",
    )

    assert (
        api_util.get_config_api_root("https://example.airbyte.com/custom/public")
        == "https://example.airbyte.com/env/config"
    )


def test_get_config_api_root_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(api_util.CLOUD_CONFIG_API_ROOT_ENV_VAR, raising=False)

    with pytest.raises(NotImplementedError):
        api_util.get_config_api_root("https://example.airbyte.com/custom/public")


def test_cloud_workspace_constructor_requires_keyword_arguments() -> None:
    with pytest.raises(TypeError, match="positional"):
        CloudWorkspace("workspace-id", bearer_token=SecretString("token"))  # type: ignore[misc]

    workspace = CloudWorkspace(
        workspace_id="workspace-id", bearer_token=SecretString("token")
    )

    assert workspace.workspace_id == "workspace-id"
