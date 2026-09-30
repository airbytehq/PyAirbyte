# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Best-effort user consent prompts for destructive MCP tools.

Destructive tools ask the user to confirm via MCP elicitation when the client can answer,
and proceed without asking when it cannot (fail-open). This is a courtesy prompt, not a
safety boundary: clients that should never delete resources must hide destructive tools,
for example with the `X-MCP-No-Destructive-Tools: 1` request header or a client-side
tool block.

Two elicitation mechanisms are supported:

- 2026-07-28 and later protocols use multi round-trip requests (SEP-2322): the tool returns
  an `InputRequiredResult`, and the client retries the call with the user's answer. This
  works on the stateless hosted HTTP server.
- Earlier protocols use a server-initiated `elicitation/create` request, which only works
  on stateful transports (for example stdio) where the client declared the capability.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio.from_thread
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server.elicitation import AcceptedElicitation
from mcp.shared.exceptions import MCPError
from mcp.types import ElicitRequest, ElicitRequestFormParams, ElicitResult, InputRequiredResult


if TYPE_CHECKING:
    from mcp.types import ClientCapabilities


_MRTR_MIN_PROTOCOL_VERSION = "2026-07-28"
"""First MCP protocol version that supports `InputRequiredResult` (SEP-2322)."""

_CONSENT_REQUEST_KEY = "confirm_permanent_delete"
_CONSENT_FIELD = "confirm"
_CONSENT_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        _CONSENT_FIELD: {
            "type": "boolean",
            "title": "Confirm permanent deletion",
        },
    },
    "required": [_CONSENT_FIELD],
}


def request_destructive_consent(
    ctx: Context,
    message: str,
) -> bool | InputRequiredResult:
    """Ask the user to confirm a destructive action, proceeding when the client cannot ask.

    Returns `True` to proceed, `False` when the user declined or cancelled, or an
    `InputRequiredResult` that the tool must return as-is so the client can prompt the user
    and retry the call with the answer.

    Must be called from a sync tool body, which FastMCP runs in a worker thread.
    """
    if not isinstance(ctx, Context) or ctx.request_context is None:
        return True

    if ctx.request_context.protocol_version >= _MRTR_MIN_PROTOCOL_VERSION:
        return _request_consent_via_mrtr(ctx, message)

    return _request_consent_via_elicit(ctx, message)


def _client_supports_elicitation(ctx: Context) -> bool:
    """Return whether the client declared the elicitation capability."""
    capabilities: ClientCapabilities | None = ctx.session.client_capabilities
    return capabilities is not None and capabilities.elicitation is not None


def _request_consent_via_mrtr(
    ctx: Context,
    message: str,
) -> bool | InputRequiredResult:
    """Request consent with a multi round-trip `InputRequiredResult` (SEP-2322)."""
    responses = ctx.input_responses
    response = responses.get(_CONSENT_REQUEST_KEY) if responses else None
    if isinstance(response, ElicitResult):
        return response.action == "accept" and (response.content or {}).get(_CONSENT_FIELD) is True

    if not _client_supports_elicitation(ctx):
        return True

    return InputRequiredResult(
        input_requests={
            _CONSENT_REQUEST_KEY: ElicitRequest(
                params=ElicitRequestFormParams(
                    message=message,
                    requested_schema=_CONSENT_SCHEMA,
                ),
            ),
        },
    )


def _request_consent_via_elicit(
    ctx: Context,
    message: str,
) -> bool:
    """Request consent with a server-initiated elicitation (pre-2026-07-28 protocols)."""
    if not _client_supports_elicitation(ctx):
        return True

    try:
        result = anyio.from_thread.run(ctx.elicit, message, bool)
    except (ToolError, MCPError):
        # The transport has no back-channel (e.g. stateless HTTP) or the client
        # could not render the prompt: proceed without consent.
        return True

    return isinstance(result, AcceptedElicitation) and result.data is True
