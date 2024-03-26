# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

You can use this module to interact with Airbyte Cloud, OSS, and Enterprise.

Usage example:

```python
import airbyte as ab
from airbyte import cloud

workspace = cloud.CloudWorkspace(
    workspace_id="123",
    api_key=ab.get_secret("AIRBYTE_API_KEY"),
)

source = ab.get_source("source-faker", config={})
source.check()

workspace.deploy_source(source)
```
"""

from __future__ import annotations

from airbyte.cloud._workspaces import CloudWorkspace


__all__ = [
    "CloudWorkspace",
]
