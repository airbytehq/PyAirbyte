# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""A Databricks (Unity Catalog) implementation of the PyAirbyte cache.

Data is written to Delta tables in a Unity Catalog schema through a SQL warehouse (or cluster), with
no Docker or JVM required. Batches are staged in a Unity Catalog volume and loaded with
`COPY INTO`.

Requires the optional `databricks` extra: `pip install 'airbyte[databricks]'`.

## Usage Example

### Personal access token:

```python
import airbyte as ab
from airbyte.caches import DatabricksCache

cache = DatabricksCache(
    server_hostname="dbc-a1b2345c-d6e7.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/abc123def456",
    catalog="main",
    schema_name="airbyte_raw",
    access_token=ab.get_secret("DATABRICKS_TOKEN"),
)
```

### OAuth M2M (service principal):

```python
import airbyte as ab
from airbyte.caches import DatabricksCache

cache = DatabricksCache(
    server_hostname="dbc-a1b2345c-d6e7.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/abc123def456",
    catalog="main",
    schema_name="airbyte_raw",
    client_id=ab.get_secret("DATABRICKS_CLIENT_ID"),
    client_secret=ab.get_secret("DATABRICKS_CLIENT_SECRET"),
)
```

## Permissions

The principal needs `USE CATALOG` on the catalog, and `USE SCHEMA`, `CREATE TABLE` and
`CREATE VOLUME` on the schema (or `CREATE SCHEMA` on the catalog if the schema does not exist yet).
Files are staged in the `staging_volume` volume (default `airbyte_staging`), which is created if
missing; to use an existing volume, grant `READ VOLUME` and `WRITE VOLUME` on it instead of
`CREATE VOLUME`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from airbyte_api.models import DestinationDatabricks

from airbyte._processors.sql.databricks import DatabricksConfig, DatabricksSqlProcessor
from airbyte.caches._utils._cache_to_dest import (
    databricks_cache_to_destination_configuration,
)
from airbyte.caches.base import CacheBase


if TYPE_CHECKING:
    from airbyte.shared.sql_processor import SqlProcessorBase


class DatabricksCache(DatabricksConfig, CacheBase):
    """Configuration for the Databricks cache."""

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = DatabricksSqlProcessor

    paired_destination_name: ClassVar[str | None] = "destination-databricks"
    paired_destination_config_class: ClassVar[type | None] = DestinationDatabricks

    @property
    def paired_destination_config(self) -> DestinationDatabricks:
        """Return the `destination-databricks` configuration for this cache."""
        return databricks_cache_to_destination_configuration(cache=self)


# Expose the Cache class and also the Config class.
__all__ = [
    "DatabricksCache",
    "DatabricksConfig",
]
