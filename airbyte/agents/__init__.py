# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for the Airbyte Agents platform.

> ## ⚠️ Experimental Interface
>
> **The Airbyte Agents Python interfaces are experimental.** Class names, method signatures,
> and result models may change or be removed without notice between minor versions of
> PyAirbyte. Pin an exact PyAirbyte version if you depend on them.

Airbyte Agents connectors expose read and write actions on individual entities, executed
one action at a time, rather than the batch record replication that `airbyte.cloud`
provides. This module is that interface.

Airbyte Cloud credentials authenticate against the Agents API, so the `AIRBYTE_CLOUD_*`
variables are reused. Set `AIRBYTE_AGENTS_API_URL` to override the Agents API root when
connecting through a proxy or local development endpoint.

## Usage Examples

Read a page of entities from a connector:

```python
from airbyte import agents

workspace = agents.AgentWorkspace.from_env()
connector = workspace.get_connector("GitHub")  # by ID or name (case insensitive)

result = connector.list_entities(
    "issues",
    api_args={"repository": "airbytehq/PyAirbyte"},
    page_size=50,
)
print(result.status, result.has_next_page)
for entity in result.entities:
    print(entity["title"])
```

Pass `result.end_cursor` back as `cursor` to page through manually:

```python
cursor = None
while True:
    result = connector.list_entities(
        "issues",
        api_args={"repository": "airbytehq/PyAirbyte"},
        cursor=cursor,
    )
    print(len(result.entities))
    if not result.has_next_page:
        break
    cursor = result.end_cursor
```

Discover what a connector supports, and what an organization can reach:

```python
organization = agents.AgentOrganization.from_env()
for workspace in organization.list_workspaces():
    print(workspace.workspace_id, workspace.name)

print(connector.inspect().integration_name)
```

Convert between the Cloud and Agents domains:

```python
from airbyte.cloud import CloudWorkspace

cloud_workspace = CloudWorkspace.from_env()
agent_workspace = agents.AgentWorkspace.from_cloud_workspace(cloud_workspace)
back_to_cloud = agent_workspace.as_cloud_workspace()
```
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte._direct_connectors.models import (
    AgentWorkspaceInfo,
    CloudContextLayerConnectorDetails,
    CloudContextStoreEntity,
    CloudContextStoreReadiness,
    CloudDirectConnectorInfo,
    DirectAccessGuidance,
    DirectAccessGuidanceIndexEntry,
    DirectAccessGuidanceSection,
    ExternalApiConnectorMetadata,
    ExternalApiExecuteResult,
    ExternalApiExecutionMetadata,
)
from airbyte.agents.connectors import AgentConnector
from airbyte.agents.organizations import AgentOrganization
from airbyte.agents.skills import AgentSkill
from airbyte.agents.workspaces import AgentWorkspace


# Submodules imported here for documentation reasons: https://github.com/mitmproxy/pdoc/issues/757
if TYPE_CHECKING:
    # ruff: noqa: TC004
    from airbyte.agents import (
        connectors,
        organizations,
        skills,
        workspaces,
    )


__all__ = [
    # Submodules
    "connectors",
    "organizations",
    "skills",
    "workspaces",
    # Classes
    "AgentConnector",
    "AgentOrganization",
    "AgentSkill",
    "AgentWorkspace",
    "AgentWorkspaceInfo",
    "CloudContextLayerConnectorDetails",
    "CloudContextStoreEntity",
    "CloudContextStoreReadiness",
    "CloudDirectConnectorInfo",
    "DirectAccessGuidance",
    "DirectAccessGuidanceIndexEntry",
    "DirectAccessGuidanceSection",
    "ExternalApiConnectorMetadata",
    "ExternalApiExecuteResult",
    "ExternalApiExecutionMetadata",
]
