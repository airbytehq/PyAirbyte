

"""MCP tools for manifest-only connector development.

This module provides Model Context Protocol tools specifically for developing
manifest-only connectors using PyAirbyte and the Airbyte CDK.
"""

from __future__ import annotations

import copy
import json
import pkgutil
from typing import TYPE_CHECKING, Any, Optional, Union

import yaml

from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)


if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

MAX_TEST_RECORDS = 3
MAX_SLICES_FOR_TESTING = 5
MAX_SAMPLE_RECORD_LENGTH = 500
MAX_RECENT_LOGS = 3

RESERVED_STREAM_CONFIG_KEYS = {"name", "path", "primary_key", "schema"}


def _test_auth_with_endpoint(
    source: ManifestDeclarativeSource, manifest_config: dict[str, Any], stream_name: str
) -> str:
    """Test auth logic with a specific endpoint."""
    try:
        streams = list(source.streams(manifest_config.get("config", {})))
        stream = next((s for s in streams if s.name == stream_name), None)
        if not stream:
            return f"Stream '{stream_name}' not found in manifest"

        records = []
        for record in stream.read_records(sync_mode=None, stream_slice={}):
            records.append(record)
            if len(records) >= MAX_TEST_RECORDS:
                break

        return (
            f"Auth test successful for stream '{stream_name}' - "
            f"retrieved {len(records)} records"
        )
    except Exception as e:
        return f"Auth test failed for stream '{stream_name}': {e!s}"


def _test_auth_without_endpoint(
    source: ManifestDeclarativeSource, manifest_config: dict[str, Any], connector_name: str
) -> str:
    """Test auth logic without hitting a specific endpoint."""
    try:
        check_result = source.check_connection(
            logger=source.logger, config=manifest_config.get("config", {})
        )
        if check_result.status.name == "SUCCEEDED":
            return f"Auth logic validation successful for connector '{connector_name}'"
        return f"Auth logic validation failed: {check_result.message}"
    except Exception as e:
        return f"Auth logic validation failed: {e!s}"


def _find_stream_config(manifest_config: dict[str, Any], stream_name: str) -> Optional[dict[str, Any]]:
    """Find stream configuration in manifest."""
    for stream in manifest_config.get("streams", []):
        if stream.get("name") == stream_name:
            return stream
    return None


def _read_stream_records(
    stream: object, max_records: int
) -> tuple[list[dict[str, Any]], list[str], int]:
    """Read records from stream with limits."""
    records = []
    logs = []
    slices_processed = 0

    try:
        for slice_data in stream.stream_slices(sync_mode=None, cursor_field=None, stream_state={}):
            slices_processed += 1
            for record in stream.read_records(sync_mode=None, stream_slice=slice_data):
                records.append(record.dict() if hasattr(record, "dict") else dict(record))
                if len(records) >= max_records:
                    break
            if len(records) >= max_records:
                break
            if slices_processed >= MAX_SLICES_FOR_TESTING:
                break
    except Exception as read_error:
        logs.append(f"Read error: {read_error!s}")

    return records, logs, slices_processed


def _generate_stream_summary(
    stream_name: str, records: list[dict[str, Any]], logs: list[str], slices_processed: int
) -> str:
    """Generate summary of stream read test."""
    summary = f"Stream read test completed for '{stream_name}':\n"
    summary += f"- Records read: {len(records)}\n"
    summary += f"- Log messages: {len(logs)}\n"
    summary += f"- Slices processed: {slices_processed}\n"

    if records:
        sample_record = records[0]
        if len(str(sample_record)) > MAX_SAMPLE_RECORD_LENGTH:
            sample_str = str(sample_record)[:MAX_SAMPLE_RECORD_LENGTH] + "..."
        else:
            sample_str = json.dumps(sample_record, indent=2)
        summary += f"\nSample record:\n{sample_str}"

    if logs:
        summary += "\nRecent logs:\n"
        for log in logs[-MAX_RECENT_LOGS:]:
            summary += f"- {log}\n"

    return summary


def register_connector_development_tools(app: FastMCP) -> None:
    """Register all connector development tools with the FastMCP app."""

    @app.tool()
    def create_stream_template(
        connector_name: str,
        stream_name: str,
        url_base: str,
        path: str,
        http_method: str = "GET",
        output_format: str = "yaml",
    ) -> str:
        """Create or modify a stream template in a manifest-only connector.

        Args:
            connector_name: Name of the connector to work with
            stream_name: Name of the stream template to create
            url_base: Base URL for the API
            path: API endpoint path
            http_method: HTTP method (GET, POST, etc.)
            output_format: Output format, either "yaml" or "json"
        """
        _ = connector_name  # Context parameter for future use
        stream_template = {
            "type": "DeclarativeStream",
            "name": stream_name,
            "primary_key": [],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": url_base,
                    "path": path,
                    "http_method": http_method,
                    "authenticator": {"type": "NoAuth"},
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
                "paginator": {"type": "NoPagination"},
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {"type": "object", "properties": {}},
            },
        }

        if output_format.lower() == "json":
            return json.dumps(stream_template, indent=2)
        return yaml.dump(stream_template, default_flow_style=False, indent=2)

    @app.tool()
    def create_auth_logic(
        connector_name: str,
        auth_type: str = "api_key",
        auth_config: Optional[dict[str, Any]] = None,
        output_format: str = "yaml",
    ) -> str:
        """Create or modify auth logic for a stream template.

        Args:
            connector_name: Name of the connector
            auth_type: Type of authentication (api_key, bearer, oauth, basic_http, no_auth)
            auth_config: Authentication configuration dict
            output_format: Output format, either "yaml" or "json"
        """
        _ = connector_name  # Context parameter for future use
        auth_config = auth_config or {}

        auth_templates = {
            "api_key": {
                "type": "ApiKeyAuthenticator",
                "header": auth_config.get("header", "X-API-Key"),
                "api_token": auth_config.get("api_token", "{{ config['api_key'] }}"),
            },
            "bearer": {
                "type": "BearerAuthenticator",
                "api_token": auth_config.get("api_token", "{{ config['access_token'] }}"),
            },
            "basic_http": {
                "type": "BasicHttpAuthenticator",
                "username": auth_config.get("username", "{{ config['username'] }}"),
                "password": auth_config.get("password", "{{ config['password'] }}"),
            },
            "no_auth": {"type": "NoAuth"},
            "oauth": {
                "type": "OAuthAuthenticator",
                "client_id": auth_config.get("client_id", "{{ config['client_id'] }}"),
                "client_secret": auth_config.get("client_secret", "{{ config['client_secret'] }}"),
                "refresh_token": auth_config.get("refresh_token", "{{ config['refresh_token'] }}"),
            },
        }

        auth_logic = auth_templates.get(auth_type.lower(), auth_templates["no_auth"])

        if output_format.lower() == "json":
            return json.dumps(auth_logic, indent=2)
        return yaml.dump(auth_logic, default_flow_style=False, indent=2)

    @app.tool()
    def test_auth_logic(
        connector_name: str,
        manifest_config: dict[str, Any],
        stream_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ) -> str:
        """Test auth logic for a stream or stream template.

        Args:
            connector_name: Name of the connector
            manifest_config: The manifest configuration as a dict
            stream_name: Optional stream name to test specific stream
            endpoint_url: Optional endpoint URL to test against
        """
        try:
            source = ManifestDeclarativeSource(
                source_config=manifest_config, emit_connector_builder_messages=True
            )

            if endpoint_url and stream_name:
                return _test_auth_with_endpoint(source, manifest_config, stream_name)

            return _test_auth_without_endpoint(source, manifest_config, connector_name)

        except Exception as e:
            return f"Error testing auth logic: {e!s}"

    @app.tool()
    def create_stream_from_template(
        template_config: dict[str, Any],
        stream_config: dict[str, Any],
        output_format: str = "yaml",
    ) -> str:
        """Create a stream leveraging an existing stream template.

        Args:
            template_config: The stream template configuration
            stream_config: Configuration for the new stream (name, path, etc.)
            output_format: Output format, either "yaml" or "json"
        """
        new_stream = copy.deepcopy(template_config)

        new_stream["name"] = stream_config.get("name", "new_stream")

        if "path" in stream_config:
            new_stream["retriever"]["requester"]["path"] = stream_config["path"]

        if "primary_key" in stream_config:
            new_stream["primary_key"] = stream_config["primary_key"]

        if "schema" in stream_config:
            new_stream["schema_loader"]["schema"] = stream_config["schema"]

        for key, value in stream_config.items():
            if key not in RESERVED_STREAM_CONFIG_KEYS:
                if "." in key:
                    keys = key.split(".")
                    current = new_stream
                    for k in keys[:-1]:
                        current = current.setdefault(k, {})
                    current[keys[-1]] = value
                else:
                    new_stream[key] = value

        if output_format.lower() == "json":
            return json.dumps(new_stream, indent=2)
        return yaml.dump(new_stream, default_flow_style=False, indent=2)

    @app.tool()
    def test_stream_read(
        manifest_config: dict[str, Any], stream_name: str, max_records: int = 10
    ) -> str:
        """Test a stream read using test-read action from the CDK.

        Args:
            manifest_config: The manifest configuration as a dict
            stream_name: Name of the stream to test
            max_records: Maximum number of records to read for testing
        """
        try:
            source = ManifestDeclarativeSource(
                source_config=manifest_config, emit_connector_builder_messages=True
            )

            stream_config = _find_stream_config(manifest_config, stream_name)
            if not stream_config:
                return f"Stream '{stream_name}' not found in manifest"

            streams = list(source.streams(manifest_config.get("config", {})))
            stream = next((s for s in streams if s.name == stream_name), None)
            if not stream:
                return f"Stream '{stream_name}' not found in source streams"

            records, logs, slices_processed = _read_stream_records(stream, max_records)

            return _generate_stream_summary(stream_name, records, logs, slices_processed)

        except Exception as e:
            return f"Error testing stream read: {e!s}"

    @app.tool()
    def get_manifest_schema(format: str = "yaml") -> str:
        """Retrieve the JSON schema used to validate manifest.YAML files.
        
        Args:
            format: Output format, either "yaml" or "json"
        """
        try:
            schema_data = pkgutil.get_data("airbyte_cdk", "sources/declarative/declarative_component_schema.yaml")
            if not schema_data:
                return "Error: Could not load manifest schema from CDK package"
            
            schema_content = schema_data.decode('utf-8')
            
            if format.lower() == "json":
                schema_dict = yaml.safe_load(schema_content)
                def json_serializer(obj):
                    if hasattr(obj, 'isoformat'):
                        return obj.isoformat()
                    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
                return json.dumps(schema_dict, indent=2, default=json_serializer)
            else:
                return schema_content
                
        except Exception as e:
            return f"Error retrieving manifest schema: {e!s}"
