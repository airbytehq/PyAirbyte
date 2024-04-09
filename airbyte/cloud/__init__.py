# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

You can use this module to interact with Airbyte Cloud, OSS, and Enterprise.

Usage example:

```python
import airbyte as ab
from airbyte import cloud

workspace = cloud.CloudWorkspace(
    workspace_id="123",
    api_key=ab.get_secret("AIRBYTE_CLOUD_API_KEY"),
)

source = ab.get_source("source-faker", config={})
source.check()

workspace.deploy_source(source)
```
"""

from __future__ import annotations

from airbyte.cloud import connections, sync_results, workspaces
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.sync_results import SyncResult
from airbyte.cloud.workspaces import CloudWorkspace


__all__ = [
    # Submodules
    "workspaces",
    "connections",
    "sync_results",
    # Classes
    "CloudWorkspace",
    "CloudConnection",
    "SyncResult",
]
