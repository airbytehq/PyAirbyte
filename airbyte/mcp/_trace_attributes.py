# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Shared trace attributes for external MCP query tools."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte._direct_connectors.models import ExternalApiReadOnlyAction, ExternalSearchType


if TYPE_CHECKING:
    from collections.abc import Mapping

_AGENT_ACTION_VALUES: dict[str, frozenset[str]] = {
    "execute_external_api_query": frozenset(member.value for member in ExternalApiReadOnlyAction),
    "execute_external_sql_query": frozenset({"sql_select"}),
    "execute_external_search_query": frozenset(
        f"search_{member.value}" for member in ExternalSearchType
    ),
}
_ENTITY_TYPE_ACTIONS = frozenset(member.value for member in ExternalApiReadOnlyAction)
_MAX_ENTITY_TYPE_LENGTH = 256


def agent_action_attributes(tool_name: str, arguments: Mapping[str, object]) -> dict[str, str]:
    """Return bounded action and requested entity attributes for a query tool."""
    if tool_name == "execute_external_sql_query":
        action: object = "sql_select"
    elif tool_name == "execute_external_search_query":
        search_type = arguments.get("search_type", ExternalSearchType.HYBRID.value)
        action = f"search_{search_type}" if isinstance(search_type, str) else None
    else:
        action = arguments.get("action", ExternalApiReadOnlyAction.LIST.value)

    if not isinstance(action, str) or action not in _AGENT_ACTION_VALUES.get(
        tool_name, frozenset()
    ):
        return {}

    attributes = {"agent.action": action}
    if tool_name == "execute_external_api_query" and action in _ENTITY_TYPE_ACTIONS:
        entity_type = arguments.get("entity_type")
        if (
            isinstance(entity_type, str)
            and 0 < len(entity_type) <= _MAX_ENTITY_TYPE_LENGTH
            and entity_type.isprintable()
            and entity_type == entity_type.strip()
        ):
            attributes["agent.entity_type"] = entity_type
    return attributes
