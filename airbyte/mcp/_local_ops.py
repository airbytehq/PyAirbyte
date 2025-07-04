# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local MCP operations."""

import traceback
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

from fastmcp import FastMCP
from pydantic import Field

from airbyte import get_source
from airbyte.caches.util import get_default_cache
from airbyte.mcp._util import log_mcp_message, resolve_config
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

        log_mcp_message(
            f"Retrieved {len(records)} records from stream '{stream_name}'",
            component="local_ops",
            event="records_retrieved",
            stream_name=stream_name,
            record_count=len(records),
        )

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
