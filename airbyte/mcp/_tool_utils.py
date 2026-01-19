# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP tool utility functions for safe mode.

This module provides safe mode functionality for MCP tools, allowing
tracking of resources created during a session to prevent accidental
deletion of pre-existing resources.
"""

from __future__ import annotations

import os


AIRBYTE_CLOUD_MCP_SAFE_MODE = os.environ.get("AIRBYTE_CLOUD_MCP_SAFE_MODE", "1").strip() != "0"
"""Whether safe mode is enabled for cloud operations.

When enabled (default), destructive operations are only allowed on resources
created during the current session.
"""

AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET = bool(os.environ.get("AIRBYTE_CLOUD_WORKSPACE_ID", "").strip())
"""Whether the AIRBYTE_CLOUD_WORKSPACE_ID environment variable is set.

When set, the workspace_id parameter is hidden from cloud tools.
"""

_GUIDS_CREATED_IN_SESSION: set[str] = set()


class SafeModeError(Exception):
    """Raised when a tool is blocked by safe mode restrictions."""

    pass


def register_guid_created_in_session(guid: str) -> None:
    """Register a GUID as created in this session.

    Args:
        guid: The GUID to register
    """
    _GUIDS_CREATED_IN_SESSION.add(guid)


def check_guid_created_in_session(guid: str) -> None:
    """Check if a GUID was created in this session.

    This is a no-op if AIRBYTE_CLOUD_MCP_SAFE_MODE is set to "0".

    Raises SafeModeError if the GUID was not created in this session and
    AIRBYTE_CLOUD_MCP_SAFE_MODE is set to 1.

    Args:
        guid: The GUID to check
    """
    if AIRBYTE_CLOUD_MCP_SAFE_MODE and guid not in _GUIDS_CREATED_IN_SESSION:
        raise SafeModeError(
            f"Cannot perform destructive operation on '{guid}': "
            f"Object was not created in this session. "
            f"AIRBYTE_CLOUD_MCP_SAFE_MODE is set to '1'."
        )
