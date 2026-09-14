# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Deployment detection for public Cloud and Agents API availability.

Cloud API root overrides are the only current signal for Cloud versus self-managed deployments.
An explicit `AIRBYTE_AGENTS_API_URL` counts as an available Agents API because a proxied URL may
still point to hosted Airbyte Cloud.
"""

from __future__ import annotations

from airbyte.constants import (
    AGENTS_API_ROOT_ENV_VAR,
    CLOUD_API_ROOT,
    CLOUD_CONFIG_API_ROOT,
)
from airbyte.secrets.util import try_get_secret


def get_overridden_cloud_api_roots(
    *,
    public_api_root: str | None,
    config_api_root: str | None,
) -> dict[str, str]:
    """Return the Cloud API roots that point away from public Airbyte Cloud.

    Keys are `"api_root"` / `"config_api_root"`; blank or `None` values count as the public
    default, and a trailing `/` is ignored.
    """
    overridden: dict[str, str] = {}
    for name, value, default in (
        ("api_root", public_api_root, CLOUD_API_ROOT),
        ("config_api_root", config_api_root, CLOUD_CONFIG_API_ROOT),
    ):
        text = value.strip().rstrip("/") if value else ""
        if text and text != default:
            overridden[name] = text
    return overridden


def get_agents_api_root_override() -> str | None:
    """Return the configured Agents API root override, if it is non-blank."""
    value = try_get_secret(AGENTS_API_ROOT_ENV_VAR, default=None)
    text = str(value).strip() if value is not None else ""
    return text.rstrip("/") or None


def is_agents_api_available(
    *,
    public_api_root: str | None,
    config_api_root: str | None,
) -> bool:
    """Return whether an Agents API exists for these Cloud API roots.

    True when `AIRBYTE_AGENTS_API_URL` is set explicitly, or when the roots are the public
    Airbyte Cloud roots (which have the hosted Agents API).
    """
    if get_agents_api_root_override():
        return True
    return not get_overridden_cloud_api_roots(
        public_api_root=public_api_root,
        config_api_root=config_api_root,
    )
