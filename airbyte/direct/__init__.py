# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Direct connectors: entity/action operations on deployed Airbyte sources.

> ## ⚠️ Experimental Interface
>
> **The Airbyte direct connector interfaces are experimental.** Class names, method
> signatures, and result models may change or be removed without notice between minor
> versions of PyAirbyte. Pin an exact PyAirbyte version if you depend on them.

A direct connector exposes entity/action style operations — `execute`, `list_entities`,
`iter_entities`, `search_entities`, `get_entity`, `create_entity`, `update_entity`, and
`delete_entity` — on a single deployed connector, as opposed to the batch record
replication that `Source.read` and `airbyte.cloud` syncs provide.

```python
from airbyte.cloud import CloudWorkspace

workspace = CloudWorkspace.from_env()
cloud_source = workspace.get_source("...")
direct = cloud_source.as_direct_connector()
for entity in direct.iter_entities("issues"):
    print(entity)
```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from airbyte.agents.connectors import AgentConnector
from airbyte.exceptions import AirbyteDirectConnectorNotSupportedError


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte.agents.models import AgentConnectorDetails, AgentExecuteResult


@runtime_checkable
class DirectConnector(Protocol):
    """Entity/action interface satisfied by every direct connector implementation."""

    @property
    def connector_id(self) -> str | None:
        """The connector ID."""
        ...

    @property
    def name(self) -> str | None:
        """The connector name, if known."""
        ...

    def inspect(self, *, force_refresh: bool = False) -> AgentConnectorDetails:
        """Return connector metadata, optionally bypassing any cached result."""
        ...

    def execute(  # noqa: PLR0913  # Mirrors `AgentConnector.execute()`.
        self,
        entity_type: str,
        action: str,
        api_args: dict[str, Any] | None = None,
        *,
        select_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        skip_truncation: bool = True,
        intent: str | None = None,
    ) -> AgentExecuteResult:
        """Execute a single action against one entity type on this connector."""
        ...

    def list_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `list` action, which returns a page of entities of `entity_type`."""
        ...

    def iter_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        *,
        limit: int | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `list_entities()`.
    ) -> Iterator[dict[str, Any]]:
        """Yield entities of `entity_type`, following the connector's pagination cursor."""
        ...

    def search_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `search` action, which returns matching entities of `entity_type`."""
        ...

    def get_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `get` action, which returns a single entity of `entity_type`."""
        ...

    def create_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `create` action, which creates an entity of `entity_type`."""
        ...

    def update_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `update` action, which updates an entity of `entity_type`."""
        ...

    def delete_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `delete` action, which deletes an entity of `entity_type`."""
        ...


class HostedDirectConnector(AgentConnector):
    """A direct connector executed by the hosted Airbyte Agents API.

    Get one from `CloudSource.as_direct_connector()` rather than constructing it directly.
    """


__all__ = [
    "AirbyteDirectConnectorNotSupportedError",
    "DirectConnector",
    "HostedDirectConnector",
]
