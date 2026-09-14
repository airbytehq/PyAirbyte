# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Trusted-execution guards for Airbyte MCP backend helpers.

Trusted execution is the master gate for the MCP server's *trusted-machine* capabilities:
local filesystem access, local connector installation/execution, and server-side secret
resolution. It is controlled solely by the `AIRBYTE_MCP_TRUSTED_EXECUTION` server
environment variable (`airbyte.constants.MCP_TRUSTED_EXECUTION_ENV_VAR`) and defaults to
*off* on every transport.

`fastmcp_extensions` already hides trusted-machine tools from the tool listing when the
gate is off, but that is a *visibility* control. The guards here are an independent
*function-layer* control: backend helpers call `raise_if_untrusted_execution_context` so a direct
call hard-fails when the gate is disabled, even if a future registration mistake left the
tool visible. Because the two layers are independent, a mistake in either one alone cannot
expose a trusted-machine capability to an untrusted (for example hosted HTTP) caller.
"""

from __future__ import annotations

import os

from airbyte.constants import MCP_TRUSTED_EXECUTION_ENV_VAR
from airbyte.exceptions import AirbyteTrustedExecutionRequiredError


_TRUTHY_VALUES = frozenset({"1", "true", "yes"})


def is_trusted_execution_enabled() -> bool:
    """Return whether trusted execution is enabled for the MCP server.

    Reads `AIRBYTE_MCP_TRUSTED_EXECUTION` from the server environment only. A value of
    `1`/`true`/`yes` (case-insensitive) enables it; anything else -- including unset --
    leaves it disabled.
    """
    return os.environ.get(MCP_TRUSTED_EXECUTION_ENV_VAR, "0").strip().lower() in _TRUTHY_VALUES


def raise_if_untrusted_execution_context(feature: str) -> None:
    """Hard-fail when `feature` is invoked while trusted execution is disabled.

    Call this from any backend helper that exposes a trusted-machine capability (local
    filesystem access, connector installation/execution, or server-side secret
    resolution). It raises `airbyte.exceptions.AirbyteTrustedExecutionRequiredError`
    when the gate is off, independently of whether the corresponding tool was hidden from
    the listing.
    """
    if not is_trusted_execution_enabled():
        raise AirbyteTrustedExecutionRequiredError(feature=feature)
