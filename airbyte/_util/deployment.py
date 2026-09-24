# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Deployment detection helpers for optional Airbyte Cloud API surfaces.

The Cloud Config API hosts the Context layer endpoints that back the Agents and
direct-access features. Those endpoints exist on public Airbyte Cloud, and on
deployments that explicitly configure a Config API root; for any other root override the
features are hidden.
"""

from __future__ import annotations

from airbyte._util.api_util import get_config_api_root
from airbyte.cloud.auth import resolve_cloud_api_url, resolve_cloud_config_api_url
from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT


def is_public_cloud(
    *,
    public_api_root: str | None = None,
    config_api_root: str | None = None,
) -> bool:
    """Return whether the given roots match the public Airbyte Cloud deployment.

    When omitted, each root falls back to its environment variable and then the default.
    `AIRBYTE_CLOUD_API_URL`/`AIRBYTE_CLOUD_CONFIG_API_URL` overrides that point at a
    custom deployment (for example a self-hosted Airbyte instance or a regional cloud)
    therefore cause this to return `False`.
    """
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
    """Return whether a Context layer API exists for the given Cloud API roots.

    The Context layer endpoints live on the Cloud Config API, so this is `True` when the
    roots are the public Airbyte Cloud roots, or when an explicit Config API root is in
    effect — either the `config_api_root` argument or `AIRBYTE_CLOUD_CONFIG_API_URL`.
    """
    if is_public_cloud(public_api_root=public_api_root, config_api_root=config_api_root):
        return True
    resolved_config_api_root = resolve_cloud_config_api_url(config_api_root)
    return bool(resolved_config_api_root and resolved_config_api_root.strip())
