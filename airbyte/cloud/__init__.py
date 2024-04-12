# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

You can use this module to interact with Airbyte Cloud, OSS, and Enterprise.

## Examples

### Basic Usage Example:

```python
import airbyte as ab
from airbyte import cloud

# Initialize an Airbyte Cloud workspace object
workspace = cloud.CloudWorkspace(
    workspace_id="123",
    api_key=ab.get_secret("AIRBYTE_CLOUD_API_KEY"),
)

# Run a sync job on Airbyte Cloud
connection = workspace.get_connection(connection_id="456")
sync_result = connection.run_sync()
print(sync_result.get_job_status())
```

### Example Read From Cloud Destination:

If your destination is supported, you can read records directly from the
`SyncResult` object. Currently this is supported in Snowflake and BigQuery only.


```python
# Assuming we've already created a `connection` object...

# Get the latest job result and print the stream names
sync_result = connection.get_sync_result()
print(sync_result.stream_names)

# Get a dataset from the sync result
dataset: CachedDataset = sync_result.get_dataset("users")

# Get a SQLAlchemy table to use in SQL queries...
users_table = dataset.to_sql_table()
print(f"Table name: {users_table.name}")

# Or iterate over the dataset directly
for record in dataset:
    print(record)
```

ℹ️ **Experimental Features**

You can use the `airbyte.cloud.experimental` module to access experimental features.
These additional features are subject to change and may not be available in all environments.
"""  # noqa: RUF002  # Allow emoji

from __future__ import annotations

from airbyte.cloud import connections, constants, sync_results, workspaces
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.constants import JobStatusEnum
from airbyte.cloud.sync_results import SyncResult
from airbyte.cloud.workspaces import CloudWorkspace


__all__ = [
    # Submodules
    "workspaces",
    "connections",
    "constants",
    "sync_results",
    # Classes
    "CloudWorkspace",
    "CloudConnection",
    "SyncResult",
    # Enums
    "JobStatusEnum",
]
