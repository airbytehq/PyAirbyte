# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local MCP operations."""

import sys
import traceback
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from fastmcp import FastMCP

from airbyte import get_source
from airbyte.caches.util import get_default_cache
from airbyte.mcp._util import resolve_config
from airbyte.secrets.config import _get_secret_sources
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


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
    connector_name: str,
    config: dict | Path | None = None,
    config_secret_name: str | None = None,
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
    connector_name: str,
) -> list[str]:
    """List all `config_secret_name` options that are known for the given connector.

    This can be used to find out which already-created config secret names are available
    for a given connector. The return value is a list of secret names, but it will not
    return the actual secret values.
    """
    secrets_names: list[str] = []
    for secrets_mgr in _get_secret_sources():
        if isinstance(secrets_mgr, GoogleGSMSecretManager):
            secrets_names.extend([
                secret_handle.secret_name.split("/")[-1]
                for secret_handle in secrets_mgr.fetch_connector_secrets(connector_name)
            ])

    return secrets_names


# @app.tool()  # << deferred
def list_source_streams(
    source_connector_name: str,
    config: dict | Path | None = None,
    config_secret_name: str | None = None,
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
    source_connector_name: str,
    stream_name: str,
    config: dict | Path | None = None,
    config_secret_name: str | None = None,
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
    source_connector_name: str,
    config: dict | Path | None = None,
    config_secret_name: str | None = None,
    *,
    stream_name: str,
    max_records: int,
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
        return f"Error reading records from source '{source_connector_name}': {ex!r}, {ex!s}\n{tb_str}"

    else:
        return records


# @app.tool()  # << deferred
def sync_source_to_cache(
    source_connector_name: str,
    config: dict | Path | None = None,
    config_secret_name: str | None = None,
    streams: list[str] | str = "suggested",
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

    if isinstance(streams, str):
        if streams == "suggested":
            streams = "*"  # TODO: obtain the real suggested streams list from `metadata.yaml`
        if streams != "*":
            streams = [streams]  # Ensure streams is a list

    source.read(
        cache=cache,
        streams=streams,
    )

    summary: str = f"Sync completed for '{source_connector_name}'!\n\n"
    summary += "Data written to default DuckDB cache\n"
    return summary


def register_local_ops_tools(app: FastMCP) -> None:
    """Register tools with the FastMCP app."""
    app.tool(list_connector_config_secrets)
    for tool in (
        validate_connector_config,
        list_source_streams,
        get_source_stream_json_schema,
        read_source_stream_records,
        sync_source_to_cache,
    ):
        # Register each tool with the FastMCP app.
        app.tool(
            tool,
            description=(tool.__doc__ or "").rstrip() + "\n" + CONFIG_HELP,
        )
