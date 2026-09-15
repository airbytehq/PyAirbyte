# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Deployment detection for public Cloud and Agents API availability.

Cloud API root overrides are the only current signal for Cloud versus self-managed deployments.
An explicit `AIRBYTE_AGENTS_API_URL` counts as an available Agents API because a proxied URL may
still point to hosted Airbyte Cloud.
"""

from __future__ import annotations

from airbyte._util.api_util import get_config_api_root
from airbyte.cloud.auth import resolve_cloud_api_url, resolve_cloud_config_api_url
from airbyte.constants import AGENTS_API_ROOT_ENV_VAR, CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT
from airbyte.secrets.util import try_get_secret


def get_agents_api_root_override() -> str | None:
    """Return the configured Agents API root override, if it is non-blank."""
    value = try_get_secret(AGENTS_API_ROOT_ENV_VAR, default=None)
    text = str(value).strip() if value is not None else ""
    return text.rstrip("/") or None


def is_public_cloud(
    *,
    public_api_root: str | None = None,
    config_api_root: str | None = None,
) -> bool:
    """Return whether the effective Cloud API roots are public Airbyte Cloud."""
    api_root = resolve_cloud_api_url(public_api_root).rstrip("/")
    if api_root != CLOUD_API_ROOT.rstrip("/"):
        return False
    resolved_config = get_config_api_root(
        api_root,
        config_api_root=resolve_cloud_config_api_url(config_api_root),
    )
    return resolved_config.rstrip("/") == CLOUD_CONFIG_API_ROOT.rstrip("/")


def is_agents_api_available(
    *,
    public_api_root: str | None = None,
    config_api_root: str | None = None,
) -> bool:
    """Return whether an Agents API exists for these Cloud API roots.

    True when `AIRBYTE_AGENTS_API_URL` is set explicitly, or when the roots are the public
    Airbyte Cloud roots (which have the hosted Agents API).
    """
    return bool(get_agents_api_root_override()) or is_public_cloud(
        public_api_root=public_api_root,
        config_api_root=config_api_root,
    )
