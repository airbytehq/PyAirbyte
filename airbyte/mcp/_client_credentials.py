# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Opt-in client-credentials transport auth for the MCP server.

The provider-neutral exchange middleware lives in `fastmcp_extensions`. This
module owns only the branded env var names and passes deployment-supplied
values to `wrap_client_credentials`.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fastmcp_extensions import wrap_client_credentials


if TYPE_CHECKING:
    from starlette.types import ASGIApp


ALLOW_CLIENT_CREDENTIALS_ENV = "AIRBYTE_MCP_AUTH_ALLOW_CLIENT_CREDENTIALS"
TOKEN_URL_ENV = "AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL"

_TRUTHY = frozenset({"1", "true", "t", "yes", "y", "on"})


def client_credentials_enabled() -> bool:
    """Return whether the opt-in client-credentials grant is enabled."""
    return (
        os.getenv(ALLOW_CLIENT_CREDENTIALS_ENV, "").strip().lower() in _TRUTHY
    )


def _token_url() -> str:
    """Return the deployment-supplied client-credentials token endpoint."""
    return os.getenv(TOKEN_URL_ENV, "").strip()


def wrap_if_enabled(app: ASGIApp) -> ASGIApp:
    """Wrap `app` with client-credentials exchange when the flag is set."""
    enabled = client_credentials_enabled()
    token_url = _token_url()
    if enabled and not token_url:
        raise ValueError(
            f"{TOKEN_URL_ENV} must be set when {ALLOW_CLIENT_CREDENTIALS_ENV} is enabled"
        )
    return wrap_client_credentials(app, enabled=enabled, token_url=token_url)
