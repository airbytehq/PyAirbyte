# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""ASGI middleware for extracting authentication headers in MCP HTTP/SSE modes."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airbyte.mcp._request_context import (
    CLOUD_CLIENT_ID_CVAR,
    CLOUD_CLIENT_SECRET_CVAR,
    CLOUD_WORKSPACE_ID_CVAR,
)


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, MutableMapping

logger = logging.getLogger(__name__)


class HeaderAuthMiddleware:
    """ASGI middleware that extracts Airbyte Cloud authentication from HTTP headers.

    This middleware runs only in HTTP/SSE modes of the MCP server. It extracts
    authentication values from HTTP headers and stores them in ContextVars for
    the duration of the request.

    Supported headers (case-insensitive):
    - X-Airbyte-Cloud-Client-Id or Airbyte-Cloud-Client-Id
    - X-Airbyte-Cloud-Client-Secret or Airbyte-Cloud-Client-Secret
    - X-Airbyte-Cloud-Workspace-Id or Airbyte-Cloud-Workspace-Id
    """

    def __init__(self, app: Callable) -> None:
        """Initialize the middleware.

        Args:
            app: The ASGI application to wrap
        """
        self.app = app

    async def __call__(
        self,
        scope: MutableMapping,
        receive: Callable[[], Awaitable[MutableMapping]],
        send: Callable[[MutableMapping], Awaitable[None]],
    ) -> None:
        """Process the ASGI request.

        Args:
            scope: ASGI scope dictionary
            receive: ASGI receive callable
            send: ASGI send callable
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = scope.get("headers", [])
        header_dict = {name.decode().lower(): value.decode() for name, value in headers}

        client_id = self._get_header_value(
            header_dict, ["x-airbyte-cloud-client-id", "airbyte-cloud-client-id"]
        )
        client_secret = self._get_header_value(
            header_dict, ["x-airbyte-cloud-client-secret", "airbyte-cloud-client-secret"]
        )
        workspace_id = self._get_header_value(
            header_dict, ["x-airbyte-cloud-workspace-id", "airbyte-cloud-workspace-id"]
        )

        tokens = []
        try:
            if client_id:
                token = CLOUD_CLIENT_ID_CVAR.set(client_id)
                tokens.append((CLOUD_CLIENT_ID_CVAR, token))
                logger.debug("Set cloud client ID from HTTP header")

            if client_secret:
                token = CLOUD_CLIENT_SECRET_CVAR.set(client_secret)
                tokens.append((CLOUD_CLIENT_SECRET_CVAR, token))
                logger.debug("Set cloud client secret from HTTP header")

            if workspace_id:
                token = CLOUD_WORKSPACE_ID_CVAR.set(workspace_id)
                tokens.append((CLOUD_WORKSPACE_ID_CVAR, token))
                logger.debug("Set cloud workspace ID from HTTP header")

            await self.app(scope, receive, send)

        finally:
            for cvar, token in tokens:
                cvar.reset(token)

    def _get_header_value(self, header_dict: dict[str, str], header_names: list[str]) -> str | None:
        """Get a header value by trying multiple possible header names.

        Args:
            header_dict: Dictionary of lowercase header names to values
            header_names: List of possible header names to try (lowercase)

        Returns:
            The header value if found, otherwise None
        """
        for name in header_names:
            if name in header_dict:
                return header_dict[name]
        return None
