# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""An Apache Iceberg implementation of the cache using PyIceberg."""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa
from overrides import overrides
from pydantic import Field
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IcebergType,
    LongType,
    StringType,
    TimestamptzType,
)

from airbyte._util.name_normalizers import LowerCaseNormalizer
from airbyte._writers.jsonl import JsonlWriter
from airbyte.constants import (
    AB_EXTRACTED_AT_COLUMN,
    AB_META_COLUMN,
    AB_RAW_ID_COLUMN,
    DEFAULT_CACHE_ROOT,
)
from airbyte.secrets.base import SecretString
from airbyte.shared.sql_processor import SqlConfig, SqlProcessorBase


if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.typedef import Identifier


# Default warehouse path for local Iceberg storage
DEFAULT_ICEBERG_WAREHOUSE = Path(DEFAULT_CACHE_ROOT) / "iceberg_warehouse"
DEFAULT_ICEBERG_CATALOG_DB = Path(DEFAULT_CACHE_ROOT) / "iceberg_catalog.db"


class IcebergConfig(SqlConfig):
    """Configuration for the Iceberg cache.

    This configuration supports both local SQLite catalogs (for development)
    and REST catalogs (for production use with services like AWS Glue, Polaris, etc.).

    ## Local SQLite Catalog (Default)

    For local development, PyIceberg uses a SQLite database to store table metadata
    and writes Parquet files to a local warehouse directory:

    ```python
    from airbyte.caches import IcebergCache

    cache = IcebergCache(
        warehouse_path="/path/to/warehouse",
        catalog_uri="sqlite:////path/to/catalog.db",
        namespace="my_namespace",
    )
    ```

    ## REST Catalog

    For production use with REST-based catalogs (AWS Glue, Apache Polaris, etc.):

    ```python
    from airbyte.caches import IcebergCache

    cache = IcebergCache(
        catalog_type="rest",
        catalog_uri="https://my-catalog.example.com",
        namespace="my_namespace",
        catalog_credential="my-api-key",
        warehouse_path="s3://my-bucket/warehouse",
    )
    ```
    """

    catalog_type: str = Field(default="sql")
    """The type of Iceberg catalog to use. Options: 'sql' (SQLite), 'rest', 'glue', 'hive'."""

    catalog_uri: str | None = Field(default=None)
    """The URI for the catalog. For SQLite: 'sqlite:///path/to/db'. For REST: the catalog URL."""

    catalog_name: str = Field(default="default")
    """The name of the catalog."""

    catalog_credential: SecretString | None = Field(default=None)
    """Credentials for the catalog (e.g., API key for REST catalogs)."""

    warehouse_path: Path | str = Field(default=DEFAULT_ICEBERG_WAREHOUSE)
    """The path to the Iceberg warehouse directory (local) or URI (S3, GCS, etc.)."""

    namespace: str = Field(default="airbyte_raw")
    """The Iceberg namespace (similar to a database schema)."""

    # Override schema_name to use namespace instead
    schema_name: str = Field(default="airbyte_raw")
    """Alias for namespace - kept for compatibility with SqlConfig."""

    table_prefix: str | None = Field(default="")
    """A prefix to add to created table names."""

    _catalog: Catalog | None = None
    """Cached catalog instance."""

    def model_post_init(self, _context: object, /) -> None:
        """Sync schema_name with namespace after initialization.

        This handles backward compatibility by allowing users to set either
        'namespace' (preferred) or 'schema_name' (legacy SqlConfig compatibility).
        """
        fields_set = self.model_fields_set
        namespace_set = "namespace" in fields_set
        schema_name_set = "schema_name" in fields_set

        if namespace_set and schema_name_set and self.namespace != self.schema_name:
            raise ValueError(
                f"namespace ({self.namespace!r}) and schema_name ({self.schema_name!r}) "
                "must match when both are provided"
            )

        if namespace_set and not schema_name_set:
            # User set namespace, sync to schema_name
            self.schema_name = self.namespace
        elif schema_name_set and not namespace_set:
            # User set schema_name (legacy), sync to namespace
            self.namespace = self.schema_name

    def _get_catalog_uri(self) -> str:
        """Get the catalog URI, using default if not specified."""
        if self.catalog_uri:
            return self.catalog_uri
        # Default to SQLite catalog in the cache directory
        return f"sqlite:///{DEFAULT_ICEBERG_CATALOG_DB}"

    def _get_warehouse_path(self) -> str:
        """Get the warehouse path as a string."""
        warehouse = self.warehouse_path
        if isinstance(warehouse, Path):
            # Ensure the directory exists for local warehouses
            warehouse.mkdir(parents=True, exist_ok=True)
            return str(warehouse.absolute())
        return warehouse

    def get_catalog(self) -> Catalog:
        """Get or create the Iceberg catalog instance."""
        if self._catalog is not None:
            return self._catalog

        catalog_config: dict[str, Any] = {
            "warehouse": self._get_warehouse_path(),
        }

        if self.catalog_type == "sql":
            catalog_config["uri"] = self._get_catalog_uri()
            self._catalog = SqlCatalog(self.catalog_name, **catalog_config)
        else:
            # For REST, Glue, Hive, etc., use the generic load_catalog
            catalog_config["type"] = self.catalog_type
            catalog_config["uri"] = self._get_catalog_uri()
            if self.catalog_credential:
                catalog_config["credential"] = str(self.catalog_credential)
            self._catalog = load_catalog(self.catalog_name, **catalog_config)

        return self._catalog

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return a SQLAlchemy URL for the catalog database.

        Note: This is primarily used for catalog metadata storage with SQLite catalogs.
        The actual data is stored in Parquet files, not in the SQL database.

        For non-SQL catalogs, we return a unique placeholder that includes catalog
        attributes to avoid config_hash collisions between different catalogs.
        """
        if self.catalog_type == "sql":
            return SecretString(self._get_catalog_uri())
        # For non-SQL catalogs, include unique attributes to avoid hash collisions
        return SecretString(
            f"iceberg://{self.catalog_type}:{self._get_catalog_uri()}:{self._get_warehouse_path()}"
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database (namespace in Iceberg terms)."""
        return self.namespace


class IcebergTypeConverter:
    """Convert JSON schema types to Iceberg types."""

    @staticmethod
    def json_schema_to_iceberg_type(
        json_schema_property_def: dict[str, str | dict | list],
    ) -> IcebergType:
        """Convert a JSON schema property definition to an Iceberg type."""
        json_type = json_schema_property_def.get("type")

        # Handle array types (e.g., ["string", "null"])
        if isinstance(json_type, list):
            # Filter out "null" and use the first non-null type
            non_null_types = [t for t in json_type if t != "null"]
            json_type = non_null_types[0] if non_null_types else "string"

        # Map JSON schema types to Iceberg types
        type_mapping: dict[str, IcebergType] = {
            "string": StringType(),
            "integer": LongType(),
            "number": DoubleType(),
            "boolean": BooleanType(),
            "object": StringType(),  # Store complex objects as JSON strings
            "array": StringType(),  # Store arrays as JSON strings
        }

        # Check for format hints
        json_format = json_schema_property_def.get("format")
        if json_format in {"date-time", "datetime"}:
            return TimestamptzType()

        return type_mapping.get(str(json_type), StringType())


class IcebergProcessor(SqlProcessorBase):
    """An Iceberg implementation of the cache processor.

    This processor writes data to Apache Iceberg tables using PyIceberg.
    Data is stored as Parquet files with Iceberg metadata for efficient querying.
    """

    file_writer_class = JsonlWriter  # pyrefly: ignore[bad-override]
    supports_merge_insert = (
        False  # pyrefly: ignore[bad-override]  # Iceberg has its own merge semantics
    )

    normalizer = LowerCaseNormalizer

    sql_config: IcebergConfig

    type_converter_class = IcebergTypeConverter  # pyrefly: ignore[bad-assignment]
    type_converter: IcebergTypeConverter  # pyrefly: ignore[bad-override]

    @property
    def catalog(self) -> Catalog:
        """Get the Iceberg catalog."""
        return self.sql_config.get_catalog()

    @property
    def namespace(self) -> str:
        """Get the Iceberg namespace."""
        return self.sql_config.namespace

    def _get_table_identifier(self, table_name: str) -> Identifier:
        """Get the full table identifier including namespace."""
        return (self.namespace, table_name)

    def _get_iceberg_schema(self, stream_name: str) -> Schema:
        """Build an Iceberg schema from the stream's JSON schema."""
        properties = self.catalog_provider.get_stream_properties(stream_name)
        fields: list[NestedField] = []
        field_id = 1

        for property_name, json_schema_property_def in properties.items():
            clean_prop_name = self.normalizer.normalize(property_name)
            iceberg_type = self.type_converter.json_schema_to_iceberg_type(
                json_schema_property_def,
            )
            fields.append(
                NestedField(
                    field_id=field_id,
                    name=clean_prop_name,
                    field_type=iceberg_type,
                    required=False,
                )
            )
            field_id += 1

        # Add Airbyte internal columns
        fields.extend(
            [
                NestedField(
                    field_id=field_id,
                    name=AB_RAW_ID_COLUMN,
                    field_type=StringType(),
                    required=False,
                ),
                NestedField(
                    field_id=field_id + 1,
                    name=AB_EXTRACTED_AT_COLUMN,
                    field_type=TimestamptzType(),
                    required=False,
                ),
                NestedField(
                    field_id=field_id + 2,
                    name=AB_META_COLUMN,
                    field_type=StringType(),  # Store as JSON string
                    required=False,
                ),
            ]
        )

        return Schema(*fields)

    @overrides
    def _ensure_schema_exists(self) -> None:
        """Ensure the Iceberg namespace exists."""
        with contextlib.suppress(NamespaceAlreadyExistsError):
            self.catalog.create_namespace(self.namespace)

    def _table_exists_iceberg(self, table_name: str) -> bool:
        """Check if an Iceberg table exists."""
        try:
            self.catalog.load_table(self._get_table_identifier(table_name))
        except NoSuchTableError:
            return False
        else:
            return True

    def _create_iceberg_table(self, stream_name: str, table_name: str) -> Table:
        """Create an Iceberg table for the given stream."""
        schema = self._get_iceberg_schema(stream_name)
        return self.catalog.create_table(
            identifier=self._get_table_identifier(table_name),
            schema=schema,
        )

    def _get_or_create_iceberg_table(self, stream_name: str, table_name: str) -> Table:
        """Get an existing Iceberg table or create a new one."""
        try:
            return self.catalog.load_table(self._get_table_identifier(table_name))
        except NoSuchTableError:
            return self._create_iceberg_table(stream_name, table_name)

    @overrides
    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,  # noqa: ARG002  # Required by base class interface
    ) -> str:
        """Write files to a new Iceberg table.

        Unlike SQL databases, we write directly to the final table in Iceberg
        since Iceberg handles transactions and versioning natively.
        """
        table_name = self.get_sql_table_name(stream_name)

        # Get or create the Iceberg table
        iceberg_table = self._get_or_create_iceberg_table(stream_name, table_name)

        # Read all JSONL files and combine into a single DataFrame
        dataframes = []
        for file_path in files:
            file_df = pd.read_json(file_path, lines=True)
            dataframes.append(file_df)

        if not dataframes:
            return table_name

        combined_df = pd.concat(dataframes, ignore_index=True)

        # Normalize column names
        combined_df.columns = pd.Index(
            [self.normalizer.normalize(col) for col in combined_df.columns]
        )

        # Get the expected columns from the Iceberg schema
        expected_columns = {field.name for field in iceberg_table.schema().fields}

        # Remove columns not in the schema
        columns_to_drop = [col for col in combined_df.columns if col not in expected_columns]
        if columns_to_drop:
            combined_df = combined_df.drop(columns=columns_to_drop)

        # Add missing columns with None values
        for col in expected_columns:
            if col not in combined_df.columns:
                combined_df[col] = None

        # Serialize dict/list columns to JSON strings to match Iceberg StringType schema
        # When pd.read_json reads JSONL, nested objects/arrays stay as Python dict/list,
        # which causes PyArrow to infer struct/list types that mismatch Iceberg's StringType
        for col in combined_df.columns:
            if combined_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                combined_df[col] = combined_df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                )

        # Convert to PyArrow table using the Iceberg table's Arrow schema for type alignment
        arrow_table = pa.Table.from_pandas(
            combined_df,
            schema=iceberg_table.schema().as_arrow(),
            preserve_index=False,
        )

        # Append to the Iceberg table
        iceberg_table.append(arrow_table)

        return table_name

    @overrides
    def _ensure_final_table_exists(
        self,
        stream_name: str,
        *,
        create_if_missing: bool = True,
    ) -> str:
        """Ensure the final Iceberg table exists."""
        table_name = self.get_sql_table_name(stream_name)

        if not self._table_exists_iceberg(table_name):
            if create_if_missing:
                self._create_iceberg_table(stream_name, table_name)
            else:
                raise NoSuchTableError(f"Table {table_name} does not exist")

        return table_name

    @overrides
    def _drop_temp_table(
        self,
        table_name: str,  # Required by base class interface
        *,
        if_exists: bool = True,  # Required by base class interface
    ) -> None:
        """No-op for Iceberg since we don't use temp tables.

        In the Iceberg implementation, we write directly to the final table
        (not a temp table) since Iceberg handles transactions natively.
        The base class may call this method with the final table name after
        _write_files_to_new_table, so we must NOT drop anything here to
        avoid data loss.
        """
        # Intentionally a no-op - we don't use temp tables in Iceberg
        pass

    @overrides
    def _append_temp_table_to_final_table(
        self,
        temp_table_name: str,
        final_table_name: str,
        stream_name: str,
    ) -> None:
        """Append data from temp table to final table.

        Note: In the Iceberg implementation, we write directly to the final table,
        so this method is a no-op. The data is already in the final table.
        """
        # In Iceberg, we write directly to the final table, so this is a no-op
        pass

    @overrides
    def _swap_temp_table_with_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Replace the final table with the temp table.

        For Iceberg, we use the overwrite operation instead of swapping tables.
        """
        # In Iceberg, the write already happened to the final table
        # For a true replace, we would need to use overwrite mode during write
        pass
