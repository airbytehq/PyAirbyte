# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""An Apache Iceberg implementation of the PyAirbyte cache.

## Usage Example - Local SQLite Catalog

For local development, the Iceberg cache uses a SQLite database to store table metadata
and writes Parquet files to a local warehouse directory:

```python
import airbyte as ab
from airbyte.caches import IcebergCache

cache = IcebergCache(
    warehouse_path="/path/to/warehouse",
    namespace="my_namespace",
)
```

## Usage Example - REST Catalog

For production use with REST-based catalogs (AWS Glue, Apache Polaris, etc.):

```python
import airbyte as ab
from airbyte.caches import IcebergCache

cache = IcebergCache(
    catalog_type="rest",
    catalog_uri="https://my-catalog.example.com",
    namespace="my_namespace",
    catalog_credential=ab.get_secret("ICEBERG_CATALOG_CREDENTIAL"),
    warehouse_path="s3://my-bucket/warehouse",
)
```

## Reading Data

Once data is cached, you can read it using PyIceberg or any Iceberg-compatible query engine:

```python
# Using PyIceberg directly
catalog = cache.get_catalog()
table = catalog.load_table(("my_namespace", "my_stream"))
df = table.scan().to_pandas()

# Or use the cache's built-in methods
records = cache.get_records("my_stream")
df = records.to_pandas()
```
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import pyarrow.dataset as ds
from pyiceberg.exceptions import NoSuchTableError

from airbyte._processors.sql.iceberg import IcebergConfig, IcebergProcessor
from airbyte.caches.base import CacheBase
from airbyte.constants import DEFAULT_ARROW_MAX_CHUNK_SIZE
from airbyte.datasets._sql import CachedDataset


if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

    from airbyte.shared.sql_processor import SqlProcessorBase


class IcebergCache(IcebergConfig, CacheBase):
    """A cache implementation that stores data in Apache Iceberg format.

    This cache uses PyIceberg to write data as Parquet files with Iceberg metadata,
    enabling efficient querying with any Iceberg-compatible query engine.

    The cache supports both local SQLite catalogs (for development) and REST catalogs
    (for production use with services like AWS Glue, Apache Polaris, etc.).
    """

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = IcebergProcessor

    # Iceberg doesn't have a paired Airbyte destination yet
    paired_destination_name: ClassVar[str | None] = None
    paired_destination_config_class: ClassVar[type | None] = None

    def get_catalog(self) -> Catalog:
        """Get the Iceberg catalog instance.

        This provides direct access to the PyIceberg catalog for advanced operations.
        """
        return super().get_catalog()

    def get_pandas_dataframe(
        self,
        stream_name: str,
    ) -> pd.DataFrame:
        """Return a Pandas DataFrame with the stream's data.

        This method reads data directly from the Iceberg table using PyIceberg's
        scan functionality, which is optimized for reading Parquet files.
        """
        table_name = self._read_processor.get_sql_table_name(stream_name)
        catalog = self.get_catalog()

        try:
            iceberg_table = catalog.load_table((self.namespace, table_name))
        except NoSuchTableError as e:
            raise KeyError(f"Stream '{stream_name}' not found in cache") from e

        return iceberg_table.scan().to_pandas()

    def get_arrow_dataset(
        self,
        stream_name: str,
        *,
        max_chunk_size: int = DEFAULT_ARROW_MAX_CHUNK_SIZE,  # noqa: ARG002
    ) -> ds.Dataset:
        """Return an Arrow Dataset with the stream's data.

        This method provides efficient access to the underlying Parquet files
        through PyArrow's dataset interface.
        """
        table_name = self._read_processor.get_sql_table_name(stream_name)
        catalog = self.get_catalog()

        try:
            iceberg_table = catalog.load_table((self.namespace, table_name))
        except NoSuchTableError as e:
            raise KeyError(f"Stream '{stream_name}' not found in cache") from e

        # Get the Arrow table from Iceberg and convert to dataset
        arrow_table: pa.Table = iceberg_table.scan().to_arrow()
        return ds.dataset(arrow_table)

    def get_iceberg_table(self, stream_name: str) -> Table:
        """Get the PyIceberg Table object for direct manipulation.

        This provides access to the underlying Iceberg table for advanced operations
        like time travel queries, schema evolution, or partition management.

        Args:
            stream_name: The name of the stream/table to retrieve.

        Returns:
            The PyIceberg Table object.

        Raises:
            KeyError: If the stream doesn't exist in the cache.
        """
        table_name = self._read_processor.get_sql_table_name(stream_name)
        catalog = self.get_catalog()

        try:
            return catalog.load_table((self.namespace, table_name))
        except NoSuchTableError as e:
            raise KeyError(f"Stream '{stream_name}' not found in cache") from e

    def get_records(
        self,
        stream_name: str,
    ) -> CachedDataset:
        """Get records from the cache as a CachedDataset.

        Note: For Iceberg caches, this returns a CachedDataset that wraps
        the Iceberg table. For more efficient access to Iceberg-specific
        features, use get_iceberg_table() or get_pandas_dataframe() directly.
        """
        return CachedDataset(self, stream_name)

    def _get_warehouse_path_for_stream(self, stream_name: str) -> Path:
        """Get the warehouse path for a specific stream's data files.

        This is useful for understanding where the Parquet files are stored.
        """
        table_name = self._read_processor.get_sql_table_name(stream_name)
        warehouse = self.warehouse_path
        if isinstance(warehouse, str):
            warehouse = Path(warehouse)
        return warehouse / self.namespace / table_name


# Expose the Cache class and also the Config class.
__all__ = [
    "IcebergCache",
    "IcebergConfig",
]
