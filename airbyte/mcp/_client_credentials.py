# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Opt-in HTTP Basic client-credentials transport auth for the MCP server.

The headless bearer path verifies an already-minted, short-lived (~15 min) JWT.
That works for MCP clients that run the OAuth flow and refresh tokens
automatically, but not for a truly headless agent that can only set a *static*
`Authorization` header value and cannot re-mint on a timer.

This module bridges that gap, behind an opt-in flag. When enabled, the server
accepts the long-lived `client_id` / `client_secret` presented on the inbound MCP
request via standard HTTP Basic auth
(`Authorization: Basic base64(client_id:client_secret)`, the same credential
encoding OAuth's `client_secret_basic` uses). The server then runs a
client-credentials exchange against the Airbyte token endpoint to obtain a
short-lived access token and rewrites the request to `Authorization: Bearer
<token>` so the existing `JWTVerifier` validates it unchanged. The agent thus
presents a durable credential once; the server owns the short-lived-token churn.

The provider-neutral exchange middleware lives in `fastmcp_extensions`
(`wrap_client_credentials`). This module owns only the Airbyte-specific policy:
the opt-in env toggle and the Airbyte Cloud token endpoint (overridable for
self-hosted deployments). It resolves those to plain values and hands them to
the generic library, so no Airbyte literal or env-var name leaks into the lib.
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
# `client_id` / `client_secret`. Defaults to Airbyte Cloud; overridable for
# self-hosted deployments pointing at their own Airbyte instance.
AIRBYTE_CLOUD_TOKEN_URL = "https://api.airbyte.com/v1/applications/token"
TOKEN_URL_ENV = "AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL"

_TRUTHY = frozenset({"1", "true", "t", "yes", "y", "on"})


def client_credentials_enabled(env: Mapping[str, str] | None = None) -> bool:
    """Return whether the opt-in HTTP Basic client-credentials grant is enabled."""
    source = env if env is not None else os.environ
    return source.get(ALLOW_CLIENT_CREDENTIALS_ENV, "").strip().lower() in _TRUTHY


def _token_url() -> str:
    """Return the token endpoint, defaulting to Airbyte Cloud.

    A blank or whitespace-only override is treated as unset so the Airbyte Cloud
    default still applies, rather than POSTing to an invalid URL and failing every
    Basic-auth request closed.
    """
    return os.getenv(TOKEN_URL_ENV, "").strip() or AIRBYTE_CLOUD_TOKEN_URL


def wrap_if_enabled(app: ASGIApp) -> ASGIApp:
    """Wrap `app` with the client-credentials exchange when the flag is set.

    Returns `app` unchanged when the opt-in flag is unset, so the standard
    bearer/OIDC transport auth is the only path. When enabled, wraps `app` as the
    outermost ASGI layer (via `fastmcp_extensions.wrap_client_credentials`) so the
    Basic-to-Bearer rewrite happens before FastMCP's auth verifier runs.
    """
    return wrap_client_credentials(
        app,
        enabled=client_credentials_enabled(),
        token_url=_token_url(),
    )
