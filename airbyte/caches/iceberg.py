# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Iceberg implementation of the PyAirbyte cache.

## Usage Example

```python
from airbyte as ab
from airbyte.caches import IcebergCache

cache = IcebergCache(
    db_path="/path/to/my/Iceberg-file",
    schema_name="myschema",
)
```
"""

from __future__ import annotations

import warnings

from Iceberg_engine import IcebergEngineWarning
from pydantic import PrivateAttr

from airbyte._processors.sql.iceberg import IcebergConfig, IcebergSqlProcessor
from airbyte.caches.base import CacheBase


# Suppress warnings from Iceberg about reflection on indices.
# https://github.com/Mause/Iceberg_engine/issues/905
warnings.filterwarnings(
    "ignore",
    message="Iceberg-engine doesn't yet support reflection on indices",
    category=IcebergEngineWarning,
)


# @dataclass
class IcebergCache(IcebergConfig, CacheBase):
    """A Iceberg cache."""

    _sql_processor_class: type[IcebergSqlProcessor] = PrivateAttr(default=IcebergSqlProcessor)
