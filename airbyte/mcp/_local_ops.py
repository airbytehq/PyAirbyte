# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local MCP operations."""

import sys
import traceback
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

from fastmcp import FastMCP
from pydantic import BaseModel, Field

from airbyte import get_source
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.util import get_default_cache
from airbyte.mcp._util import resolve_config
from airbyte.secrets.config import _get_secret_sources
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from airbyte.sources.registry import get_connector_metadata


if TYPE_CHECKING:
    from airbyte.sources.base import Source


CONFIG_HELP = """
You can provide `config` as JSON or a Path to a YAML/JSON file.
If a `dict` is provided, it must not contain hardcoded secrets.
Instead, secrets should be provided using environment variables,
and the config should reference them using the format
`secret_reference::ENV_VAR_NAME`.

You can also provide a `config_secret_name` to use a specific
secret name for the configuration. This is useful if you want to
validate a configuration that is stored in a secrets manager.

If `config_secret_name` is provided, it should point to a string
that contains valid JSON or YAML.

If both `config` and `config_secret_name` are provided, the
`config` will be loaded first and then the referenced secret config
will be layered on top of the non-secret config.
"""


# @app.tool()  # << deferred
def validate_connector_config(
    connector_name: Annotated[
        str,
        Field(description="The name of the connector to validate."),
    ],
    config: Annotated[
        dict | Path | None,
        Field(description="The configuration for the connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
) -> tuple[bool, str]:
    """Validate a connector configuration.

    Returns a tuple of (is_valid: bool, message: str).
    """
    try:
        source = get_source(connector_name)
    except Exception as ex:
        return False, f"Failed to get connector '{connector_name}': {ex}"

    try:
        config_dict = resolve_config(
            config=config,
            config_secret_name=config_secret_name,
            config_spec_jsonschema=source.config_spec,
        )
        source.set_config(config_dict)
    except Exception as ex:
        return False, f"Failed to resolve configuration for {connector_name}: {ex}"

    try:
        source.check()
    except Exception as ex:
        return False, f"Configuration for {connector_name} is invalid: {ex}"

    return True, f"Configuration for {connector_name} is valid!"


# @app.tool()  # << deferred
def list_connector_config_secrets(
    connector_name: Annotated[
        str,
        Field(description="The name of the connector."),
    ],
) -> list[str]:
    """List all `config_secret_name` options that are known for the given connector.

    This can be used to find out which already-created config secret names are available
    for a given connector. The return value is a list of secret names, but it will not
    return the actual secret values.
    """
    secrets_names: list[str] = []
    for secrets_mgr in _get_secret_sources():
        if isinstance(secrets_mgr, GoogleGSMSecretManager):
            secrets_names.extend(
                [
                    secret_handle.secret_name.split("/")[-1]
                    for secret_handle in secrets_mgr.fetch_connector_secrets(connector_name)
                ]
            )

    return secrets_names


# @app.tool()  # << deferred
def list_source_streams(
    source_connector_name: Annotated[
        str,
        Field(description="The name of the source connector."),
    ],
    config: Annotated[
        dict | Path | None,
        Field(description="The configuration for the source connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
) -> list[str]:
    """List all streams available in a source connector.

    This operation (generally) requires a valid configuration, including any required secrets.
    """
    source: Source = get_source(
        source_connector_name,
    )
    config_dict = resolve_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=source.config_spec,
    )
    source.set_config(config_dict)
    return source.get_available_streams()


# @app.tool()  # << deferred
def get_source_stream_json_schema(
    source_connector_name: Annotated[
        str,
        Field(description="The name of the source connector."),
    ],
    stream_name: Annotated[
        str,
        Field(description="The name of the stream."),
    ],
    config: Annotated[
        dict | Path | None,
        Field(description="The configuration for the source connector."),
    ],
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ],
) -> dict[str, Any]:
    """List all properties for a specific stream in a source connector."""
    source: Source = get_source(source_connector_name)
    config_dict = resolve_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=source.config_spec,
    )
    source.set_config(config_dict)
    return source.get_stream_json_schema(stream_name=stream_name)


# @app.tool()  # << deferred
def read_source_stream_records(
    source_connector_name: Annotated[
        str,
        Field(description="The name of the source connector."),
    ],
    config: Annotated[
        dict | Path | None,
        Field(description="The configuration for the source connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
    *,
    stream_name: Annotated[
        str,
        Field(description="The name of the stream to read records from."),
    ],
    max_records: Annotated[
        int,
        Field(description="The maximum number of records to read."),
    ] = 1000,
) -> list[dict[str, Any]] | str:
    """Get records from a source connector."""
    try:
        source = get_source(source_connector_name)
        config_dict = resolve_config(
            config=config,
            config_secret_name=config_secret_name,
            config_spec_jsonschema=source.config_spec,
        )
        source.set_config(config_dict)
        # First we get a generator for the records in the specified stream.
        record_generator = source.get_records(stream_name)
        # Next we load a limited number of records from the generator into our list.
        records: list[dict[str, Any]] = list(islice(record_generator, max_records))

        print(f"Retrieved {len(records)} records from stream '{stream_name}'", sys.stderr)

    except Exception as ex:
        tb_str = traceback.format_exc()
        # If any error occurs, we print the error message to stderr and return an empty list.
        return (
            f"Error reading records from source '{source_connector_name}': {ex!r}, {ex!s}\n{tb_str}"
        )

    else:
        return records


# @app.tool()  # << deferred
def sync_source_to_cache(
    source_connector_name: Annotated[
        str,
        Field(description="The name of the source connector."),
    ],
    config: Annotated[
        dict | Path | None,
        Field(description="The configuration for the source connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
    streams: Annotated[
        list[str] | str,
        Field(description="The streams to sync."),
    ] = "suggested",
) -> str:
    """Run a sync from a source connector to the default DuckDB cache."""
    source = get_source(source_connector_name)
    config_dict = resolve_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=source.config_spec,
    )
    source.set_config(config_dict)
    cache = get_default_cache()

    if isinstance(streams, str) and streams == "suggested":
        streams = "*"  # Default to all streams if 'suggested' is not otherwise specified.
        try:
            metadata = get_connector_metadata(
                source_connector_name,
            )
        except Exception:
            streams = "*"  # Fallback to all streams if suggested streams fail.
        else:
            if metadata is not None:
                streams = metadata.suggested_streams or "*"

    if isinstance(streams, str) and streams != "*":
        streams = [streams]  # Ensure streams is a list

    source.read(
        cache=cache,
        streams=streams,
    )
    del cache  # Ensure the cache is closed properly

    summary: str = f"Sync completed for '{source_connector_name}'!\n\n"
    summary += "Data written to default DuckDB cache\n"
    return summary


class CachedDatasetInfo(BaseModel):
    """Class to hold information about a cached dataset."""

    stream_name: str
    """The name of the stream in the cache."""
    # TODO: add later:
    # table_name: str
    # schema_name: str | None = None
    # source_name: str | None = None


def list_cached_datasets() -> list[CachedDatasetInfo]:
    """List all streams available in the default DuckDB cache."""
    cache: DuckDBCache = get_default_cache()
    result = [CachedDatasetInfo(stream_name=stream_name) for stream_name in cache.streams]
    del cache  # Ensure the cache is closed properly
    return result


def describe_default_cache() -> dict[str, Any]:
    """Describe the currently configured default cache."""
    cache = get_default_cache()
    return {
        "cache_type": type(cache).__name__,
        "cache_dir": str(cache.cache_dir),
        "cache_db_path": str(Path(cache.db_path).absolute()),
        "cached_streams": list(cache.streams.keys()),
    }


def _is_safe_sql(sql_query: str) -> bool:
    """Check if a SQL query is safe to execute.

    For security reasons, we only allow read-only operations like SELECT, DESCRIBE, and SHOW.
    Multi-statement queries (containing semicolons) are also disallowed for security.

    Note: SQLAlchemy will also validate downstream, but this is a first-pass check.

    Args:
        sql_query: The SQL query to check

    Returns:
        True if the query is safe to execute, False otherwise
    """
    # Remove leading/trailing whitespace and convert to uppercase for checking
    normalized_query = sql_query.strip().upper()

    # Disallow multi-statement queries (containing semicolons)
    # Note: We check the original query to catch semicolons anywhere, including in comments
    if ";" in sql_query:
        return False

    # List of allowed SQL statement prefixes (read-only operations)
    allowed_prefixes = (
        "SELECT",
        "DESCRIBE",
        "DESC",  # Short form of DESCRIBE
        "SHOW",
        "EXPLAIN",  # Also safe - shows query execution plan
    )

    # Check if the query starts with any allowed prefix
    return any(normalized_query.startswith(prefix) for prefix in allowed_prefixes)


def run_sql_query(
    sql_query: Annotated[
        str,
        Field(description="The SQL query to execute."),
    ],
    max_records: Annotated[
        int,
        Field(description="Maximum number of records to return."),
    ] = 1000,
) -> list[dict[str, Any]]:
    """Run a SQL query against the default cache.

    The dialect of SQL should match the dialect of the default cache.
    Use `describe_default_cache` to see the cache type.

    For DuckDB-type caches:
    - Use `SHOW TABLES` to list all tables.
    - Use `DESCRIBE <table_name>` to get the schema of a specific table

    For security reasons, only read-only operations are allowed: SELECT, DESCRIBE, SHOW, EXPLAIN.
    """
    # Check if the query is safe to execute
    if not _is_safe_sql(sql_query):
        return [
            {
                "ERROR": "Unsafe SQL query detected. Only read-only operations are allowed: "
                "SELECT, DESCRIBE, SHOW, EXPLAIN",
                "SQL_QUERY": sql_query,
            }
        ]

    cache: DuckDBCache = get_default_cache()
    try:
        return cache.run_sql_query(
            sql_query,
            max_records=max_records,
        )
    except Exception as ex:
        tb_str = traceback.format_exc()
        return [
            {
                "ERROR": f"Error running SQL query: {ex!r}, {ex!s}",
                "TRACEBACK": tb_str,
                "SQL_QUERY": sql_query,
            }
        ]
    finally:
        del cache  # Ensure the cache is closed properly


def register_local_ops_tools(app: FastMCP) -> None:
    """Register tools with the FastMCP app."""
    app.tool(list_connector_config_secrets)
    for tool in (
        describe_default_cache,
        get_source_stream_json_schema,
        list_cached_datasets,
        list_source_streams,
        read_source_stream_records,
        run_sql_query,
        sync_source_to_cache,
        validate_connector_config,
    ):
        # Register each tool with the FastMCP app.
        app.tool(
            tool,
            description=(tool.__doc__ or "").rstrip() + "\n" + CONFIG_HELP,
        )
