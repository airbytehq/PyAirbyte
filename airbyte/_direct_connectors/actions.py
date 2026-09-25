# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Connector action names and request-param helpers for direct connector execution.

This module is a leaf: it must not import anything from `airbyte.cloud`, so
`airbyte.cloud.connectors` can use these names without creating an import cycle.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from airbyte.exceptions import PyAirbyteInputError


DOWNLOAD_RESPONSE_TYPE_PARAM = "_airbyte_response_type"
"""The `api_args` key selecting the download action's response type; PyAirbyte forces `json`."""


class AgentReadAction(str, Enum):
    """Connector actions that only read data.

    The `search` action is the connector's native API search, parallel to `get` and `list`.
    The `sql_select` action runs one read-only SQL statement (or `SHOW TABLES`) on the query
    engine behind a destination connector. Pass `sql` and `sql_dialect` (and optionally
    `dry_run`) in `api_args`; `entity_type` is ignored for this action.

    The `download` action fetches one file's content. PyAirbyte always requests the JSON
    response type, so the content arrives as text (Markdown for rich documents) or base64
    rather than as a byte stream.
    """

    LIST = "list"
    GET = "get"
    SEARCH = "search"
    DOWNLOAD = "download"
    SQL_SELECT = "sql_select"


class AgentWriteAction(str, Enum):
    """Connector actions that create, update, or delete data."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


AgentAction = AgentReadAction | AgentWriteAction
"""Every connector action accepted by `execute()`."""


_PAGINATION_ARGS: dict[str, str] = {"page_size": "limit", "cursor": "cursor"}
"""Pagination conveniences PyAirbyte merges into the connector's `params`.

Maps the PyAirbyte argument name to the connector's own `params` key: the Agents API calls
page size `limit`, which PyAirbyte does not expose under that name because `limit` reads as
a cap on the whole result set rather than on one page.
"""


def _build_params(
    *,
    api_args: dict[str, Any] | None,
    page_size: int | None,
    cursor: str | None,
) -> dict[str, Any]:
    """Merge the pagination conveniences into the connector-specific `api_args`."""
    params: dict[str, Any] = dict(api_args or {})
    pagination: dict[str, Any] = {"page_size": page_size, "cursor": cursor}

    conflicts = sorted(
        name
        for name, param_key in _PAGINATION_ARGS.items()
        if pagination[name] is not None and param_key in params
    )
    if conflicts:
        raise PyAirbyteInputError(
            message="Pagination arguments were provided twice.",
            guidance=(
                "Pass each of `page_size` and `cursor` either as a keyword argument or "
                "within `api_args`, but not both. Note that `page_size` is sent to the "
                "connector as `limit`."
            ),
            context={"duplicated_args": conflicts},
        )

    params.update(
        {
            param_key: pagination[name]
            for name, param_key in _PAGINATION_ARGS.items()
            if pagination[name] is not None
        }
    )
    return params
