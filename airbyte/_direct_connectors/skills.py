# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Paging helpers for the Agents API skills listing."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte._direct_connectors import api_util as _api_util
from airbyte._direct_connectors.models import AgentSkillList


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from airbyte._direct_connectors.models import AgentSkillInfo
    from airbyte.cloud._credentials import _AirbyteCredentials


def iter_skill_pages(
    fetch_page: Callable[[str | None], AgentSkillList],
) -> Iterator[AgentSkillInfo]:
    """Yield skills across pages, following `next_cursor` until it is `None`.

    Stops early if the server returns a blank cursor or one already seen, rather than
    requesting the same page forever.
    """
    cursor: str | None = None
    seen_cursors: set[str] = set()
    while True:
        page = fetch_page(cursor)
        yield from page.data
        cursor = page.next_cursor
        if cursor is None or not cursor.strip() or cursor in seen_cursors:
            return
        seen_cursors.add(cursor)


def iter_skill_infos(
    *,
    credentials: _AirbyteCredentials,
    workspace_id: str | None = None,
) -> Iterator[AgentSkillInfo]:
    """Yield all skills available to a workspace or organization, following pagination."""
    return iter_skill_pages(
        lambda cursor: AgentSkillList.model_validate(
            _api_util.list_agent_skills(
                credentials=credentials,
                organization_id=credentials.organization_id,
                workspace_id=workspace_id,
                cursor=cursor,
            )
        )
    )
