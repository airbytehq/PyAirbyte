# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for the Airbyte Agents platform.

Airbyte Agents connectors expose read and write actions on individual entities, executed
one action at a time, rather than the batch record replication that `airbyte.cloud`
provides. This module is that interface.

Airbyte Cloud credentials authenticate against the Agents API, so no Agents-specific
credentials or environment variables exist: the `AIRBYTE_CLOUD_*` variables are reused.

## Usage Examples

Execute an action against a connector:

```python
from airbyte import agents

workspace = agents.AgentWorkspace.from_env()
connector = workspace.get_connector(name="GitHub")

result = connector.list_entities(
    "issues",
    api_args={"repository": "airbytehq/PyAirbyte"},
    limit=50,
)
for entity in result.entities:
    print(entity["title"])
```

Page through results using the cursor the connector reports:

```python
cursor: str | None = None
while True:
    result = connector.list_entities(
        "issues", api_args={"repository": "airbytehq/PyAirbyte"}, cursor=cursor
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

print(connector.describe().source_definition_name)
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

from airbyte.agents.connectors import AgentConnector
from airbyte.agents.models import (
    AgentConnectorDetails,
    AgentConnectorInfo,
    AgentConnectorMetadata,
    AgentContextStoreEntity,
    AgentContextStoreReadiness,
    AgentExecuteResult,
    AgentExecutionMetadata,
    AgentWorkspaceInfo,
)
from airbyte.agents.organizations import AgentOrganization
from airbyte.agents.workspaces import AgentWorkspace


__all__ = [
    "AgentConnector",
    "AgentConnectorDetails",
    "AgentConnectorInfo",
    "AgentConnectorMetadata",
    "AgentContextStoreEntity",
    "AgentContextStoreReadiness",
    "AgentExecuteResult",
    "AgentExecutionMetadata",
    "AgentOrganization",
    "AgentWorkspace",
    "AgentWorkspaceInfo",
]
