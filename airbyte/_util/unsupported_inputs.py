# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Startup guards for removed PyAirbyte environment variables."""

import os

from airbyte.constants import CLOUD_MCP_SAFE_MODE_ENV_VAR
from airbyte.exceptions import AirbyteLibInputError


UNSUPPORTED_ENV_VARS: dict[str, str] = {
    CLOUD_MCP_SAFE_MODE_ENV_VAR: (
        "Remove `AIRBYTE_CLOUD_MCP_SAFE_MODE` and consider using one of these "
        "alternatives:\n"
        "- `AIRBYTE_CLOUD_MCP_ALLOW_PIPELINE_CHANGES=0`: block changes to "
        "sources, destinations, and connections.\n"
        "- `AIRBYTE_CLOUD_MCP_ALLOW_EXTERNAL_ACCESS=0`: block SQL and API "
        "passthrough tools.\n"
        "- `AIRBYTE_CLOUD_MCP_READONLY_MODE=1`: only expose read-only tools.\n"
        "- `AIRBYTE_CLOUD_MCP_ALLOW_DESTRUCTIVE_TOOLS=0`: hide destructive tools.\n"
        "- `AIRBYTE_CLOUD_MCP_DISABLE_DELETE_GUARD=1`: allow deletions without the name marker.\n"
        "- `AIRBYTE_CLOUD_MCP_LOCAL_DEMO_MODE=1`: only allow changes to "
        "resources created in this session (local mode only; not recommended "
        "except for demos).\n"
    ),
}
"""Removed PyAirbyte environment variables, mapped to replacement guidance.

Checked at startup of the `pyab` CLI and both MCP servers. Startup fails if any
listed name is set.
"""


def check_unsupported_env_vars() -> None:
    """Fail fast if a removed PyAirbyte env var is set.

    Raises `AirbyteLibInputError` on the first name in `UNSUPPORTED_ENV_VARS`
    present in the environment with any value, including an empty string.
    """
    for name, guidance in UNSUPPORTED_ENV_VARS.items():
        if name in os.environ:
            raise AirbyteLibInputError(
                message=f"`{name}` is no longer supported.",
                guidance=guidance,
            )
