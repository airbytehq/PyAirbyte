# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""An Apache Iceberg implementation of the cache using PyIceberg.

.. warning::
    **Experimental Feature**: The Iceberg cache is experimental and not necessarily
    stable. During this preview period, features may change or be removed, and breaking
    changes may be introduced without advanced notice.

This module provides the core implementation for writing data to Apache Iceberg tables,
including configuration, type conversion, and data processing. It supports both local
SQLite catalogs (for development) and REST catalogs (for production use with services
like AWS Glue, Apache Polaris, etc.).

Key components:
    `IcebergConfig`: Configuration class for Iceberg cache settings.
    `IcebergTypeConverter`: Converts JSON schema types to Iceberg types.
    `IcebergProcessor`: Processes and writes data to Iceberg tables.

Type handling modes:
    `nested_types`: Preserves nested structure as StructType/ListType for better query
        performance. Stricter - will fail on incompatible schemas.
    `as_json_strings`: Stringifies all complex objects to JSON. More permissive but
        requires JSON parsing to access nested fields.
"""

from __future__ import annotations

import contextlib
import gzip
import json
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from overrides import overrides
from pydantic import Field, model_validator
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IcebergType,
    ListType,
    LongType,
    StringType,
    StructType,
    TimestamptzType,
    TimeType,
)

from airbyte._util.name_normalizers import LowerCaseNormalizer
from airbyte._writers.jsonl import JsonlWriter
from airbyte.constants import (
    AB_EXTRACTED_AT_COLUMN,
    AB_META_COLUMN,
    AB_RAW_ID_COLUMN,
    DEFAULT_CACHE_ROOT,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString
from airbyte.shared.sql_processor import SqlConfig, SqlProcessorBase


if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.typedef import Identifier


# Default warehouse path for local Iceberg storage
DEFAULT_ICEBERG_WAREHOUSE = Path(DEFAULT_CACHE_ROOT) / "iceberg_warehouse"
DEFAULT_ICEBERG_CATALOG_DB = Path(DEFAULT_CACHE_ROOT) / "iceberg_catalog.db"


class ObjectTypingMode(str, Enum):
    """Mode for handling complex object types in Iceberg schemas.

    - `nested_types`: Always type nested fields as structs. Provides better query performance
      but cannot handle all data types (will fail early on incompatible schemas).
    - `as_json_strings`: Always stringify complex objects to JSON. Can write any data but
      requires JSON parsing to access nested fields.
    """

    NESTED_TYPES = "nested_types"
    AS_JSON_STRINGS = "as_json_strings"


class AdditionalPropertiesMode(str, Enum):
    """Mode for handling additional (undeclared) properties in objects.

    Only applicable when object_typing is set to 'nested_types'.

    - `fail`: Fail if we encounter an undeclared property at runtime.
    - `ignore`: Silently drop additional properties.
    - `stringify`: Create a placeholder column `_additional_properties_json` to hold
      additional property data as a JSON string.
    """

    FAIL = "fail"
    IGNORE = "ignore"
    STRINGIFY = "stringify"


class AnyOfPropertiesMode(str, Enum):
    """Mode for handling anyOf/oneOf union types in schemas.

    Only applicable when object_typing is set to 'nested_types'.

    - `fail`: Fail if we encounter anyOf types in the schema.
    - `branch`: Store each type option as a separate subcolumn within a struct.
      E.g., column `sometimes_int` declared as `anyOf(str, int)` becomes
      `sometimes_int: {str_val: str | None, int_val: int | None}`.
      Note: Only compatible with simple types; complex anyOf types will fail.
    - `stringify`: Store nondeterministic (anyOf) types as JSON strings.
    """

    FAIL = "fail"
    BRANCH = "branch"
    STRINGIFY = "stringify"


# Mapping from JSON schema type to branch subcolumn name
SIMPLE_ANYOF_BRANCH_NAMES: dict[str, str] = {
    "string": "str_val",
    "integer": "int_val",
    "number": "num_val",
    "boolean": "bool_val",
}

# Suffix for columns that have been stringified to JSON
JSON_COLUMN_SUFFIX = "__json"

# Name for the additional properties placeholder column
ADDITIONAL_PROPERTIES_COLUMN = "_additional_properties"


def get_json_column_name(base_name: str) -> str:
    """Get the column name for a stringified JSON column.

    When a complex type is stringified to JSON, we append a suffix to indicate
    that the column contains JSON data that may need parsing.

    Args:
        base_name: The original column/property name.

    Returns:
        The column name with the JSON suffix appended.

    Example:
        >>> get_json_column_name("user_data")
        'user_data__json'
        >>> get_json_column_name("_additional_properties")
        '_additional_properties__json'
    """
    return f"{base_name}{JSON_COLUMN_SUFFIX}"


def get_additional_properties_column_name() -> str:
    """Get the column name for storing additional (undeclared) properties.

    Returns:
        The column name for additional properties, with JSON suffix.

    Example:
        >>> get_additional_properties_column_name()
        '_additional_properties__json'
    """
    return get_json_column_name(ADDITIONAL_PROPERTIES_COLUMN)


def convert_value_to_arrow_type(  # noqa: PLR0911, PLR0912
    value: Any,  # noqa: ANN401
    arrow_type: pa.DataType,
) -> Any:  # noqa: ANN401
    """Recursively convert a Python value to match the expected Arrow type.

    This function handles the conversion of Python dicts/lists to match
    the expected Arrow struct/list types, and properly handles null values.

    Args:
        value: The Python value to convert.
        arrow_type: The expected PyArrow type.

    Returns:
        The converted value that can be used to build an Arrow array.
    """
    # Handle null values - return None for any type
    if value is None:
        return None

    # Handle struct types (Python dict -> Arrow struct)
    if pa.types.is_struct(arrow_type):
        if not isinstance(value, dict):
            # If we expected a struct but got something else, try to serialize to JSON
            return json.dumps(value) if isinstance(value, (dict, list)) else value

        # Convert each field in the struct
        result = {}
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            field_value = value.get(field.name)
            result[field.name] = convert_value_to_arrow_type(field_value, field.type)
        return result

    # Handle list types (Python list -> Arrow list)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        if not isinstance(value, list):
            # If we expected a list but got something else, return as-is or serialize
            return json.dumps(value) if isinstance(value, (dict, list)) else value

        # Convert each element in the list
        element_type = arrow_type.value_type
        return [convert_value_to_arrow_type(item, element_type) for item in value]

    # Handle string types - serialize dicts/lists to JSON
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value) if value is not None else None

    # Handle numeric types - try to convert strings to numbers
    if pa.types.is_integer(arrow_type):
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            # Try to convert string to int
            with contextlib.suppress(ValueError):
                return int(value)
            # Try float first then int (for "123.0" -> 123)
            with contextlib.suppress(ValueError):
                return int(float(value))
        return None  # Return None for unconvertible values

    if pa.types.is_floating(arrow_type):
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Try to convert string to float
            with contextlib.suppress(ValueError):
                return float(value)
        return None  # Return None for unconvertible values

    # Handle boolean - explicit handling for string representations
    if pa.types.is_boolean(arrow_type):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            # Handle common string representations of boolean values
            return value.lower() in {"true", "1", "yes"}
        return bool(value)

    # Handle timestamp types - parse ISO format strings
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, str):
            # Use fromisoformat which handles most ISO 8601 formats
            with contextlib.suppress(ValueError):
                parsed = datetime.fromisoformat(value)
                # Ensure timezone-aware (default to UTC if naive)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            # If parsing fails, return as-is and let PyArrow try
        return value

    # Handle date types - parse ISO format strings
    if pa.types.is_date(arrow_type):
        if isinstance(value, str):
            with contextlib.suppress(ValueError):
                return date.fromisoformat(value[:10])  # Take just the date part
        return value

    # Handle time types
    if pa.types.is_time(arrow_type):
        return value

    # Default: return as-is
    return value


def convert_record_to_arrow_schema(
    record: dict[str, Any],
    arrow_schema: pa.Schema,
    normalizer: type[LowerCaseNormalizer],
) -> dict[str, Any]:
    """Convert a record (Python dict) to match the expected Arrow schema.

    This function normalizes field names and converts values to match
    the expected Arrow types.

    Args:
        record: The input record as a Python dict.
        arrow_schema: The expected PyArrow schema.
        normalizer: The name normalizer to use.

    Returns:
        A dict with normalized field names and converted values.
    """
    result: dict[str, Any] = {}

    # First, normalize the record keys
    normalized_record = {normalizer.normalize(k): v for k, v in record.items()}

    # Convert each field according to the schema
    for field in arrow_schema:
        field_name = field.name
        value = normalized_record.get(field_name)
        result[field_name] = convert_value_to_arrow_type(value, field.type)

    return result


class IcebergConfig(SqlConfig):
    """Configuration for the Iceberg cache.

    This configuration supports both local SQLite catalogs (for development)
    and REST catalogs (for production use with services like AWS Glue, Polaris, etc.).

    .. warning::
        **Experimental Feature**: The Iceberg cache is experimental and not necessarily
        stable. During this preview period, features may change or be removed, and
        breaking changes may be introduced without advanced notice.

    Local SQLite Catalog (Default)
    ------------------------------

    For local development, PyIceberg uses a SQLite database to store table metadata
    and writes Parquet files to a local warehouse directory::

        from airbyte.caches import IcebergCache

        cache = IcebergCache(
            warehouse_path="/path/to/warehouse",
            catalog_uri="sqlite:////path/to/catalog.db",
            namespace="my_namespace",
        )

    REST Catalog with S3 Storage
    ----------------------------

    For production use with REST-based catalogs and S3 storage::

        from airbyte.caches import IcebergCache

        cache = IcebergCache(
            catalog_type="rest",
            catalog_uri="https://my-catalog.example.com",
            namespace="my_namespace",
            catalog_credential="my-api-key",
            warehouse_path="s3://my-bucket/warehouse",
            # S3 configuration
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            s3_bucket_name="my-bucket",
            s3_bucket_region="us-east-1",
        )

    AWS Glue Catalog
    ----------------

    For AWS Glue-based catalogs::

        from airbyte.caches import IcebergCache

        cache = IcebergCache(
            catalog_type="glue",
            namespace="my_database",
            warehouse_path="s3://my-bucket/warehouse",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            s3_bucket_region="us-east-1",
            glue_id="123456789012",  # AWS Account ID
        )
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
    """The Iceberg namespace for data tables (similar to a database schema)."""

    # For Iceberg, schema_name is always "main" because SQLite (used for internal
    # catalog/state tables) doesn't support schemas. The namespace field is used
    # for Iceberg table organization instead.
    schema_name: str = Field(default="main")
    """Schema for internal SQLAlchemy tables. Always 'main' for SQLite compatibility."""

    table_prefix: str | None = Field(default="")
    """A prefix to add to created table names."""

    # S3 Configuration
    aws_access_key_id: SecretString | None = Field(default=None)
    """AWS Access Key ID for S3 and Glue operations."""

    aws_secret_access_key: SecretString | None = Field(default=None)
    """AWS Secret Access Key for S3 and Glue operations."""

    s3_bucket_name: str | None = Field(default=None)
    """The name of the S3 bucket for Iceberg data storage."""

    s3_bucket_region: str | None = Field(default=None)
    """The AWS region of the S3 bucket (e.g., 'us-east-1', 'eu-west-1')."""

    s3_endpoint: str | None = Field(default=None)
    """Custom S3 endpoint URL for S3-compatible storage (e.g., MinIO)."""

    # Glue-specific configuration
    glue_id: str | None = Field(default=None)
    """AWS Account ID for Glue catalog operations."""

    # Type handling configuration
    object_typing: ObjectTypingMode = Field(default=ObjectTypingMode.NESTED_TYPES)
    """Mode for handling complex object types.
    - 'nested_types': Type nested fields as structs (better query performance, stricter).
    - 'as_json_strings': Stringify all complex objects to JSON (more permissive)."""

    additional_properties: AdditionalPropertiesMode = Field(default=AdditionalPropertiesMode.FAIL)
    """How to handle additional (undeclared) properties in objects.
    Only applicable when object_typing='nested_types'.
    - 'fail': Fail if we encounter an undeclared property at runtime.
    - 'ignore': Silently drop additional properties.
    - 'stringify': Create `_additional_properties_json` column for extra data."""

    anyof_properties: AnyOfPropertiesMode = Field(default=AnyOfPropertiesMode.FAIL)
    """How to handle anyOf/oneOf union types in schemas.
    Only applicable when object_typing='nested_types'.
    - 'fail': Fail if we encounter anyOf types in the schema.
    - 'branch': Store each type option as a separate subcolumn (simple types only).
    - 'stringify': Store anyOf types as JSON strings."""

    _catalog: Catalog | None = None
    """Cached catalog instance."""

    @model_validator(mode="after")
    def _validate_typing_config(self) -> IcebergConfig:
        """Validate that typing configuration options are consistent."""
        if self.object_typing == ObjectTypingMode.AS_JSON_STRINGS:
            # additional_properties and anyof_properties are ignored in this mode
            # but we don't need to warn - they just have no effect
            pass
        return self

    def _get_catalog_uri(self) -> str:
        """Get the catalog URI, using default if not specified."""
        if self.catalog_uri:
            return self.catalog_uri
        # Default to SQLite catalog in the cache directory
        return f"sqlite:///{DEFAULT_ICEBERG_CATALOG_DB}"

    def _get_warehouse_path(self) -> str:
        """Get the warehouse path as a string.

        For local paths (both Path objects and strings without URI schemes),
        ensures the directory exists before returning.
        """
        warehouse = self.warehouse_path
        if isinstance(warehouse, Path):
            # Ensure the directory exists for local warehouses
            warehouse.mkdir(parents=True, exist_ok=True)
            return str(warehouse.absolute())
        # Handle string paths - create directory for local paths (no URI scheme)
        if isinstance(warehouse, str) and "://" not in warehouse:
            local_path = Path(warehouse)
            local_path.mkdir(parents=True, exist_ok=True)
            return str(local_path.absolute())
        return warehouse

    def _get_s3_config(self) -> dict[str, str]:
        """Get S3-specific configuration for PyIceberg.

        Returns a dictionary of S3 configuration options that PyIceberg expects.
        See: https://py.iceberg.apache.org/configuration/#s3
        """
        s3_config: dict[str, str] = {}

        if self.aws_access_key_id:
            s3_config["s3.access-key-id"] = str(self.aws_access_key_id)
        if self.aws_secret_access_key:
            s3_config["s3.secret-access-key"] = str(self.aws_secret_access_key)
        if self.s3_bucket_region:
            s3_config["s3.region"] = self.s3_bucket_region
        if self.s3_endpoint:
            s3_config["s3.endpoint"] = self.s3_endpoint

        return s3_config

    def _get_glue_config(self) -> dict[str, str]:
        """Get Glue-specific configuration for PyIceberg.

        Returns a dictionary of Glue configuration options that PyIceberg expects.
        See: https://py.iceberg.apache.org/configuration/#glue-catalog
        """
        glue_config: dict[str, str] = {}

        if self.glue_id:
            glue_config["glue.id"] = self.glue_id
        if self.s3_bucket_region:
            glue_config["glue.region"] = self.s3_bucket_region

        return glue_config

    def get_catalog(self) -> Catalog:
        """Get or create the Iceberg catalog instance."""
        if self._catalog is not None:
            return self._catalog

        catalog_config: dict[str, Any] = {
            "warehouse": self._get_warehouse_path(),
        }

        # Add S3 configuration if any S3 settings are provided
        catalog_config.update(self._get_s3_config())

        if self.catalog_type == "sql":
            catalog_config["uri"] = self._get_catalog_uri()
            self._catalog = SqlCatalog(self.catalog_name, **catalog_config)
        elif self.catalog_type == "glue":
            # Glue catalog configuration
            catalog_config["type"] = "glue"
            catalog_config.update(self._get_glue_config())
            self._catalog = load_catalog(self.catalog_name, **catalog_config)
        else:
            # For REST, Hive, etc., use the generic load_catalog
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
    """Convert JSON schema types to Iceberg types.

    This converter provides both Iceberg-native type conversion (`json_schema_to_iceberg_type`)
    and a to_sql_type method for compatibility with the SqlProcessorBase interface.

    The converter supports different modes controlled by `IcebergConfig`:
    - object_typing: `nested_types` or `as_json_strings`
    - additional_properties: `fail`, `ignore`, or `stringify`
    - anyof_properties: `fail`, `branch`, or `stringify`

    Special handling:
    - The `_airbyte_meta` column is always preserved as a StructType (not stringified)
    - Arrays are always strongly typed (ListType) regardless of object_typing mode
    - Schemaless objects/arrays are always stringified
    """

    def __init__(self, conversion_map: dict | None = None) -> None:
        """Initialize the type converter.

        Args:
            conversion_map: Optional conversion map (ignored for Iceberg).
        """
        # Iceberg doesn't use SQLAlchemy types, but we keep this for interface compatibility
        self._conversion_map = conversion_map
        # Field ID counter for generating unique Iceberg field IDs (instance-level)
        self._field_id_counter: int = 0

    def next_field_id(self) -> int:
        """Generate a unique field ID for Iceberg nested fields."""
        self._field_id_counter += 1
        return self._field_id_counter

    def reset_field_id_counter(self) -> None:
        """Reset the field ID counter for a new schema generation."""
        self._field_id_counter = 0

    def json_schema_to_iceberg_type(  # noqa: PLR0911, PLR0912
        self,
        json_schema_property_def: dict[str, Any],
        *,
        object_typing: ObjectTypingMode = ObjectTypingMode.NESTED_TYPES,
        anyof_mode: AnyOfPropertiesMode = AnyOfPropertiesMode.FAIL,
        additional_props_mode: AdditionalPropertiesMode = AdditionalPropertiesMode.FAIL,
    ) -> IcebergType:
        """Convert a JSON schema property definition to an Iceberg type.

        This method handles the full complexity of JSON schema to Iceberg type conversion,
        including union types, format hints, objects, arrays, and primitives. The number
        of return statements reflects the distinct type categories that need handling.

        Args:
            json_schema_property_def: The JSON schema property definition.
            object_typing: Mode for handling complex objects.
            anyof_mode: Mode for handling anyOf/oneOf types.
            additional_props_mode: Mode for handling additional properties.

        Returns:
            The corresponding Iceberg type.

        Raises:
            PyAirbyteInputError: If schema contains incompatible types for the selected mode.
        """
        json_type = json_schema_property_def.get("type")

        # Handle union types in JSON schema (e.g., ["string", "null"])
        if isinstance(json_type, list):
            non_null_types = [t for t in json_type if t != "null"]
            if len(non_null_types) == 0:
                return StringType()
            if len(non_null_types) == 1:
                json_type = non_null_types[0]
            else:
                # Multiple non-null types in array - treat as anyOf
                return self._handle_anyof_types(
                    non_null_types, anyof_mode, object_typing, additional_props_mode
                )

        # Handle oneOf/anyOf (union types)
        if "oneOf" in json_schema_property_def or "anyOf" in json_schema_property_def:
            options: list[dict[str, Any]] = (
                json_schema_property_def.get("anyOf", json_schema_property_def.get("oneOf")) or []
            )
            option_types: list[str] = []
            for opt in options:
                opt_type = opt.get("type")
                if opt_type and opt_type != "null":
                    option_types.append(opt_type)
            return self._handle_anyof_types(
                option_types, anyof_mode, object_typing, additional_props_mode
            )

        # Check for format hints first (these override type)
        json_format = json_schema_property_def.get("format")
        if json_format == "date":
            return DateType()
        if json_format == "time":
            return TimeType()
        if json_format in {"date-time", "datetime"}:
            return TimestamptzType()

        # Handle object type
        if json_type == "object":
            properties = json_schema_property_def.get("properties")
            has_additional = json_schema_property_def.get("additionalProperties", False)

            if object_typing == ObjectTypingMode.AS_JSON_STRINGS:
                return StringType()

            if not properties:
                # Schemaless object - always stringify
                return StringType()

            # nested_types mode - convert to StructType
            return self._convert_object_to_struct(
                properties,
                has_additional_properties=has_additional,
                additional_props_mode=additional_props_mode,
                anyof_mode=anyof_mode,
            )

        # Handle array type
        if json_type == "array":
            items = json_schema_property_def.get("items")
            if not items:
                # Schemaless array - stringify
                return StringType()

            # In AS_JSON_STRINGS mode, stringify arrays too (for consistency)
            if object_typing == ObjectTypingMode.AS_JSON_STRINGS:
                return StringType()

            # nested_types mode: Convert to ListType with proper element type
            element_type = self.json_schema_to_iceberg_type(
                items,
                object_typing=object_typing,
                anyof_mode=anyof_mode,
                additional_props_mode=additional_props_mode,
            )
            return ListType(self.next_field_id(), element_type)

        # Map primitive JSON schema types to Iceberg types
        type_mapping: dict[str, IcebergType] = {
            "string": StringType(),
            "integer": LongType(),
            "number": DoubleType(),
            "boolean": BooleanType(),
        }

        return type_mapping.get(str(json_type), StringType())

    def _handle_anyof_types(
        self,
        option_types: list[str],
        anyof_mode: AnyOfPropertiesMode,
        object_typing: ObjectTypingMode,
        additional_props_mode: AdditionalPropertiesMode,
    ) -> IcebergType:
        """Handle anyOf/oneOf union types based on configuration.

        Args:
            option_types: List of type names in the union.
            anyof_mode: Mode for handling anyOf types.
            object_typing: Mode for handling complex objects.
            additional_props_mode: Mode for handling additional properties.

        Returns:
            The Iceberg type for this union.

        Raises:
            PyAirbyteInputError: If mode is 'fail' or 'branch' with complex types.
        """
        if not option_types:
            return StringType()

        # Single type - just convert it directly
        if len(option_types) == 1:
            return self.json_schema_to_iceberg_type(
                {"type": option_types[0]},
                object_typing=object_typing,
                anyof_mode=anyof_mode,
                additional_props_mode=additional_props_mode,
            )

        # Multiple types - handle based on mode
        if anyof_mode == AnyOfPropertiesMode.STRINGIFY:
            return StringType()

        if anyof_mode == AnyOfPropertiesMode.FAIL:
            raise PyAirbyteInputError(
                message="Schema contains anyOf/oneOf union types which are not supported "
                "in 'fail' mode. Set anyof_properties='stringify' or 'branch' to handle these.",
                context={"option_types": option_types},
            )

        # Branch mode - create struct with subcolumns for each type
        # Only works with simple types
        complex_types = [t for t in option_types if t not in SIMPLE_ANYOF_BRANCH_NAMES]
        if complex_types:
            raise PyAirbyteInputError(
                message="Schema contains anyOf/oneOf with complex types which cannot be "
                "handled in 'branch' mode. Set anyof_properties='stringify' to handle these.",
                context={"complex_types": complex_types, "all_types": option_types},
            )

        # Create struct with subcolumns for each simple type
        nested_fields: list[NestedField] = []
        for type_name in option_types:
            branch_name = SIMPLE_ANYOF_BRANCH_NAMES.get(type_name, f"{type_name}_val")
            field_id = self.next_field_id()
            field_type = self.json_schema_to_iceberg_type(
                {"type": type_name},
                object_typing=object_typing,
                anyof_mode=anyof_mode,
                additional_props_mode=additional_props_mode,
            )
            nested_fields.append(
                NestedField(
                    field_id=field_id,
                    name=branch_name,
                    field_type=field_type,
                    required=False,  # All branches are optional
                )
            )

        return StructType(*nested_fields)

    def _convert_object_to_struct(
        self,
        properties: dict[str, Any],
        *,
        has_additional_properties: bool = False,
        additional_props_mode: AdditionalPropertiesMode = AdditionalPropertiesMode.FAIL,
        anyof_mode: AnyOfPropertiesMode = AnyOfPropertiesMode.FAIL,
    ) -> StructType:
        """Convert JSON schema object properties to an Iceberg StructType.

        Args:
            properties: The properties dict from a JSON schema object definition.
            has_additional_properties: Whether the schema allows additional properties.
            additional_props_mode: Mode for handling additional properties.
            anyof_mode: Mode for handling anyOf types.

        Returns:
            An Iceberg StructType with nested fields.
        """
        nested_fields: list[NestedField] = []

        for prop_name, prop_def in properties.items():
            field_id = self.next_field_id()
            # Recursively convert nested types (always use nested_types within structs)
            field_type = self.json_schema_to_iceberg_type(
                prop_def,
                object_typing=ObjectTypingMode.NESTED_TYPES,
                anyof_mode=anyof_mode,
                additional_props_mode=additional_props_mode,
            )

            # Check if field is nullable (default to True for safety)
            prop_type = prop_def.get("type")
            is_nullable = True
            if isinstance(prop_type, list):
                is_nullable = "null" in prop_type

            nested_fields.append(
                NestedField(
                    field_id=field_id,
                    name=prop_name,
                    field_type=field_type,
                    required=not is_nullable,
                )
            )

        # Add placeholder for additional properties if needed
        if (
            has_additional_properties
            and additional_props_mode == AdditionalPropertiesMode.STRINGIFY
        ):
            nested_fields.append(
                NestedField(
                    field_id=self.next_field_id(),
                    name=get_additional_properties_column_name(),
                    field_type=StringType(),
                    required=False,
                )
            )

        return StructType(*nested_fields)

    def to_sql_type(
        self,
        json_schema_property_def: dict[str, Any],
    ) -> IcebergType:
        """Convert a JSON schema property definition to a type.

        Note: This method exists for compatibility with SqlProcessorBase interface.
        For Iceberg, we return the Iceberg type instead of a SQLAlchemy type.
        The actual type conversion for Iceberg tables uses json_schema_to_iceberg_type.
        """
        return self.json_schema_to_iceberg_type(json_schema_property_def)

    @classmethod
    def get_string_type(cls) -> IcebergType:
        """Return the Iceberg type for string columns.

        Required by SqlProcessorBase for internal column definitions.
        """
        return StringType()

    @classmethod
    def get_json_type(cls) -> IcebergType:
        """Return the Iceberg type for JSON columns.

        Required by SqlProcessorBase for internal column definitions.
        Iceberg stores JSON as string type.
        """
        return StringType()


class IcebergProcessor(SqlProcessorBase):
    """An Iceberg implementation of the cache processor.

    This processor writes data to Apache Iceberg tables using PyIceberg.
    Data is stored as Parquet files with Iceberg metadata for efficient querying.

    The processor uses a two-phase write approach:
    1. `_write_files_to_new_table` reads JSONL files and stores the converted Arrow
       table in memory (keyed by table name).
    2. The appropriate write method (`_append_temp_table_to_final_table` or
       `_emulated_merge_temp_table_to_final_table`) then writes the data to Iceberg
       using the correct operation (append or upsert).

    For MERGE mode, this processor uses PyIceberg's native upsert() method which
    implements merge-on-read semantics - delete files are created for existing
    records and new records are appended. Deduplication happens at read time.
    """

    file_writer_class = JsonlWriter  # pyrefly: ignore[bad-override]
    supports_merge_insert = (
        False  # pyrefly: ignore[bad-override]  # Iceberg has its own merge semantics
    )

    normalizer = LowerCaseNormalizer

    sql_config: IcebergConfig

    type_converter_class = IcebergTypeConverter  # pyrefly: ignore[bad-assignment]
    type_converter: IcebergTypeConverter  # pyrefly: ignore[bad-override]

    # Storage for pending Arrow tables (keyed by table name)
    # This allows us to defer the actual write until we know the write method
    _pending_arrow_tables: dict[str, pa.Table]

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
        """Build an Iceberg schema from the stream's JSON schema.

        Uses the object_typing configuration to determine whether to preserve
        nested structure (StructType/ListType) or stringify complex types to JSON.

        Note: _airbyte_meta is always stored as a StructType (strongly typed),
        matching the behavior of the Kotlin S3 Data Lake destination.

        All field IDs are generated using the type_converter.next_field_id() method
        to ensure unique IDs across both top-level and nested fields.
        """
        # Reset field ID counter for each new schema generation
        # This ensures each table's schema has field IDs starting from 1
        self.type_converter.reset_field_id_counter()

        properties = self.catalog_provider.get_stream_properties(stream_name)
        fields: list[NestedField] = []

        # Get typing configuration from config
        object_typing = self.sql_config.object_typing
        anyof_mode = self.sql_config.anyof_properties
        additional_props_mode = self.sql_config.additional_properties

        for property_name, json_schema_property_def in properties.items():
            clean_prop_name = self.normalizer.normalize(property_name)
            iceberg_type = self.type_converter.json_schema_to_iceberg_type(
                json_schema_property_def,
                object_typing=object_typing,
                anyof_mode=anyof_mode,
                additional_props_mode=additional_props_mode,
            )
            fields.append(
                NestedField(
                    field_id=self.type_converter.next_field_id(),
                    name=clean_prop_name,
                    field_type=iceberg_type,
                    required=False,
                )
            )

        # Add Airbyte internal columns
        # _airbyte_meta is always stored as a StructType (strongly typed)
        # This matches the Kotlin S3 Data Lake destination behavior
        airbyte_meta_type = self._get_airbyte_meta_type()
        fields.extend(
            [
                NestedField(
                    field_id=self.type_converter.next_field_id(),
                    name=AB_RAW_ID_COLUMN,
                    field_type=StringType(),
                    required=False,
                ),
                NestedField(
                    field_id=self.type_converter.next_field_id(),
                    name=AB_EXTRACTED_AT_COLUMN,
                    field_type=TimestamptzType(),
                    required=False,
                ),
                NestedField(
                    field_id=self.type_converter.next_field_id(),
                    name=AB_META_COLUMN,
                    field_type=airbyte_meta_type,
                    required=False,
                ),
            ]
        )

        return Schema(*fields)

    def _get_airbyte_meta_type(self) -> IcebergType:
        """Get the Iceberg type for the _airbyte_meta column.

        The _airbyte_meta column is always stored as a StructType with known fields,
        matching the behavior of the Kotlin S3 Data Lake destination.

        Returns:
            A StructType representing the _airbyte_meta schema.
        """
        return StructType(
            NestedField(
                field_id=self.type_converter.next_field_id(),
                name="sync_id",
                field_type=LongType(),
                required=False,
            ),
            NestedField(
                field_id=self.type_converter.next_field_id(),
                name="changes",
                field_type=ListType(
                    self.type_converter.next_field_id(),
                    StructType(
                        NestedField(
                            field_id=self.type_converter.next_field_id(),
                            name="field",
                            field_type=StringType(),
                            required=False,
                        ),
                        NestedField(
                            field_id=self.type_converter.next_field_id(),
                            name="change",
                            field_type=StringType(),
                            required=False,
                        ),
                        NestedField(
                            field_id=self.type_converter.next_field_id(),
                            name="reason",
                            field_type=StringType(),
                            required=False,
                        ),
                    ),
                ),
                required=False,
            ),
        )

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

    def _init_pending_tables(self) -> None:
        """Initialize the pending Arrow tables storage if not already initialized."""
        if not hasattr(self, "_pending_arrow_tables"):
            self._pending_arrow_tables = {}

    @overrides
    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,  # noqa: ARG002  # Required by base class interface
    ) -> str:
        """Read JSONL files and prepare data for writing to Iceberg.

        This method reads JSONL files and converts records to an Arrow table,
        storing it in memory for later writing. The actual write to Iceberg
        happens in the appropriate write method (_append_temp_table_to_final_table
        or _emulated_merge_temp_table_to_final_table).

        This two-phase approach allows us to use the correct Iceberg operation
        (append vs upsert) based on the write method.

        This method handles:
        - Null values for struct/list columns
        - Python dicts -> Arrow structs
        - Python lists -> Arrow lists
        - Schemaless arrays/objects -> JSON strings
        """
        self._init_pending_tables()
        table_name = self.get_sql_table_name(stream_name)

        # Get or create the Iceberg table (schema is declared before parsing records)
        iceberg_table = self._get_or_create_iceberg_table(stream_name, table_name)
        arrow_schema = iceberg_table.schema().as_arrow()

        # Read all JSONL files and convert records to match the Arrow schema
        converted_records: list[dict[str, Any]] = []
        for file_path in files:
            # Read gzip-compressed JSONL file line by line (progressive read for large files)
            with gzip.open(file_path, mode="rt", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    # Convert record to match the Arrow schema
                    converted = convert_record_to_arrow_schema(
                        record, arrow_schema, self.normalizer
                    )
                    converted_records.append(converted)

        if not converted_records:
            return table_name

        # Build PyArrow arrays for each column
        arrays: list[pa.Array] = []
        for field in arrow_schema:
            column_values = [record.get(field.name) for record in converted_records]
            # Create array with the correct type
            array = pa.array(column_values, type=field.type)
            arrays.append(array)

        # Create PyArrow table from arrays and store for later writing
        arrow_table = pa.Table.from_arrays(arrays, schema=arrow_schema)
        self._pending_arrow_tables[table_name] = arrow_table

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
    def _ensure_compatible_table_schema(
        self,
        stream_name: str,
        table_name: str,
    ) -> None:
        """Ensure the Iceberg table schema is compatible with the stream schema.

        For Iceberg, we handle schema evolution differently than SQL databases.
        Iceberg has native schema evolution support, so we don't need to add
        columns via ALTER TABLE. The schema is managed when creating the table.
        """
        # Iceberg handles schema evolution natively - no action needed here
        # The table schema is set when the table is created in _create_iceberg_table
        pass

    @overrides
    def _emulated_merge_temp_table_to_final_table(
        self,
        stream_name: str,
        temp_table_name: str,  # noqa: ARG002  # Required by base class interface
        final_table_name: str,
    ) -> None:
        """Merge data into the final Iceberg table using upsert (merge-on-read).

        This method uses PyIceberg's native upsert() method which implements
        merge-on-read semantics:
        - Delete files are created for existing records with matching primary keys
        - New records are appended
        - Deduplication happens at read time

        This is more efficient than copy-on-write for high-volume streaming workloads.
        """
        self._init_pending_tables()

        # Get the pending Arrow table data
        arrow_table = self._pending_arrow_tables.get(final_table_name)
        if arrow_table is None:
            # No data to write
            return

        # Get primary keys for the stream
        pk_columns = self.catalog_provider.get_primary_keys(stream_name)

        # Load the Iceberg table
        iceberg_table = self.catalog.load_table(self._get_table_identifier(final_table_name))

        if pk_columns:
            # Use upsert for merge-on-read semantics
            # This creates delete files for existing records and appends new records
            iceberg_table.upsert(df=arrow_table, join_cols=pk_columns)
        else:
            # No primary keys - fall back to append (no deduplication possible)
            iceberg_table.append(arrow_table)

        # Clear the pending data
        del self._pending_arrow_tables[final_table_name]

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
        temp_table_name: str,  # noqa: ARG002  # Required by base class interface
        final_table_name: str,
        stream_name: str,  # noqa: ARG002  # Required by base class interface
    ) -> None:
        """Append data to the final Iceberg table.

        This method writes the pending Arrow table data to Iceberg using append().
        No deduplication is performed - all records are added to the table.
        """
        self._init_pending_tables()

        # Get the pending Arrow table data
        arrow_table = self._pending_arrow_tables.get(final_table_name)
        if arrow_table is None:
            # No data to write
            return

        # Load the Iceberg table and append the data
        iceberg_table = self.catalog.load_table(self._get_table_identifier(final_table_name))
        iceberg_table.append(arrow_table)

        # Clear the pending data
        del self._pending_arrow_tables[final_table_name]

    @overrides
    def _swap_temp_table_with_final_table(
        self,
        stream_name: str,  # noqa: ARG002  # Required by base class interface
        temp_table_name: str,  # noqa: ARG002  # Required by base class interface
        final_table_name: str,
    ) -> None:
        """Replace all data in the final Iceberg table.

        This method uses PyIceberg's overwrite() to replace all existing data
        with the new data. This is used for the REPLACE write method.
        """
        self._init_pending_tables()

        # Get the pending Arrow table data
        arrow_table = self._pending_arrow_tables.get(final_table_name)
        if arrow_table is None:
            # No data to write
            return

        # Load the Iceberg table and overwrite all data
        iceberg_table = self.catalog.load_table(self._get_table_identifier(final_table_name))
        iceberg_table.overwrite(arrow_table)

        # Clear the pending data
        del self._pending_arrow_tables[final_table_name]
