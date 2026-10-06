# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Opt-in client-credentials transport auth for the MCP server.

The headless bearer path requires a short-lived token. This module adds an
opt-in path for clients that send durable credentials on every request. The
client can use `Client-Id` / `Client-Secret` headers or HTTP Basic auth.

Environment variables:

- `AIRBYTE_MCP_AUTH_ALLOW_CLIENT_CREDENTIALS`: enables the opt-in path.
- `AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL`: overrides the Airbyte Cloud
  application-token endpoint.

The provider-neutral exchange middleware lives in `fastmcp_extensions`. This
module owns the branded env var names and Airbyte token endpoint policy, then
passes those values to `wrap_client_credentials`. The credential path is off
by default. It exchanges the inbound credentials for a bearer token and
rewrites the request, but it does not verify that token. When enabled, also
configure the bearer verifier so issuer, audience, and expiry are checked and
requests with no credentials are rejected. Use TLS and prevent intermediaries
from logging the credential headers.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fastmcp_extensions import wrap_client_credentials


if TYPE_CHECKING:
    from collections.abc import Mapping

    from starlette.types import ASGIApp


# Opt-in flag. Off by default: accepting long-lived credentials at the transport
# is a deliberate escalation, so a deployment must explicitly turn it on.
ALLOW_CLIENT_CREDENTIALS_ENV = "AIRBYTE_MCP_AUTH_ALLOW_CLIENT_CREDENTIALS"

# Airbyte token endpoint that mints an application access token from a
# `client_id` / `client_secret`. Override for self-hosted deployments.
TOKEN_URL_ENV = "AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL"
AIRBYTE_CLOUD_TOKEN_URL = "https://api.airbyte.com/v1/applications/token"

_TRUTHY = frozenset({"1", "true", "t", "yes", "y", "on"})


def client_credentials_enabled(env: Mapping[str, str] | None = None) -> bool:
    """Return whether the opt-in client-credentials grant is enabled."""
    source = env if env is not None else os.environ
    return source.get(ALLOW_CLIENT_CREDENTIALS_ENV, "").strip().lower() in _TRUTHY


def _token_url() -> str:
    """Return the token endpoint, defaulting to Airbyte Cloud.

    A blank or whitespace-only override is treated as unset so the Airbyte
    Cloud default still applies.
    """
    return os.getenv(TOKEN_URL_ENV, "").strip() or AIRBYTE_CLOUD_TOKEN_URL


def wrap_if_enabled(app: ASGIApp) -> ASGIApp:
    """Wrap `app` with client-credentials exchange when the flag is set."""
    return wrap_client_credentials(
        app,
        enabled=client_credentials_enabled(),
        token_url=_token_url(),
    )
