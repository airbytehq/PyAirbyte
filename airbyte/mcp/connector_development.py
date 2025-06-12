"""MCP tools for manifest-only connector development.

This module provides Model Context Protocol tools specifically for developing
manifest-only connectors using PyAirbyte and the Airbyte CDK.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import yaml
from mcp.server.fastmcp import FastMCP


def register_connector_development_tools(app: FastMCP):
    """Register all connector development tools with the FastMCP app."""
    
    @app.tool()
    def create_stream_template(connector_name, stream_name, url_base, path, http_method="GET", output_format="yaml"):
        """Create or modify a stream template in a manifest-only connector.
        
        Args:
            connector_name: Name of the connector to work with
            stream_name: Name of the stream template to create
            url_base: Base URL for the API
            path: API endpoint path
            http_method: HTTP method (GET, POST, etc.)
            output_format: Output format, either "yaml" or "json"
        """
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
                    "authenticator": {"type": "NoAuth"}
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []}
                },
                "paginator": {"type": "NoPagination"}
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {"type": "object", "properties": {}}
            }
        }
        
        if output_format.lower() == "json":
            return json.dumps(stream_template, indent=2)
        return yaml.dump(stream_template, default_flow_style=False, indent=2)

    @app.tool()
    def create_auth_logic(connector_name, auth_type="api_key", auth_config=None, output_format="yaml"):
        """Create or modify auth logic for a stream template.
        
        Args:
            connector_name: Name of the connector
            auth_type: Type of authentication (api_key, bearer, oauth, basic_http, no_auth)
            auth_config: Authentication configuration dict
            output_format: Output format, either "yaml" or "json"
        """
        auth_config = auth_config or {}
        
        auth_templates = {
            "api_key": {
                "type": "ApiKeyAuthenticator",
                "header": auth_config.get("header", "X-API-Key"),
                "api_token": auth_config.get("api_token", "{{ config['api_key'] }}")
            },
            "bearer": {
                "type": "BearerAuthenticator", 
                "api_token": auth_config.get("api_token", "{{ config['access_token'] }}")
            },
            "basic_http": {
                "type": "BasicHttpAuthenticator",
                "username": auth_config.get("username", "{{ config['username'] }}"),
                "password": auth_config.get("password", "{{ config['password'] }}")
            },
            "no_auth": {"type": "NoAuth"},
            "oauth": {
                "type": "OAuthAuthenticator",
                "client_id": auth_config.get("client_id", "{{ config['client_id'] }}"),
                "client_secret": auth_config.get("client_secret", "{{ config['client_secret'] }}"),
                "refresh_token": auth_config.get("refresh_token", "{{ config['refresh_token'] }}")
            }
        }
        
        auth_logic = auth_templates.get(auth_type.lower(), auth_templates["no_auth"])
        
        if output_format.lower() == "json":
            return json.dumps(auth_logic, indent=2)
        return yaml.dump(auth_logic, default_flow_style=False, indent=2)

    @app.tool()
    def test_auth_logic(connector_name, manifest_config, stream_name=None, endpoint_url=None):
        """Test auth logic for a stream or stream template.
        
        Args:
            connector_name: Name of the connector
            manifest_config: The manifest configuration as a dict
            stream_name: Optional stream name to test specific stream
            endpoint_url: Optional endpoint URL to test against
        """
        try:
            from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
            
            source = ManifestDeclarativeSource(
                source_config=manifest_config,
                emit_connector_builder_messages=True
            )
            
            if endpoint_url and stream_name:
                try:
                    streams = list(source.streams(manifest_config.get("config", {})))
                    stream = next((s for s in streams if s.name == stream_name), None)
                    if not stream:
                        return f"Stream '{stream_name}' not found in manifest"
                    
                    records = []
                    for record in stream.read_records(sync_mode=None, stream_slice={}):
                        records.append(record)
                        if len(records) >= 3:  # Limit to 3 records for testing
                            break
                    
                    return f"Auth test successful for stream '{stream_name}' - retrieved {len(records)} records"
                except Exception as e:
                    return f"Auth test failed for stream '{stream_name}': {str(e)}"
            else:
                try:
                    check_result = source.check_connection(logger=source.logger, config=manifest_config.get("config", {}))
                    if check_result.status.name == "SUCCEEDED":
                        return f"Auth logic validation successful for connector '{connector_name}'"
                    else:
                        return f"Auth logic validation failed: {check_result.message}"
                except Exception as e:
                    return f"Auth logic validation failed: {str(e)}"
                    
        except Exception as e:
            return f"Error testing auth logic: {str(e)}"

    @app.tool()
    def create_stream_from_template(connector_name, template_config, stream_config, output_format="yaml"):
        """Create a stream leveraging an existing stream template.
        
        Args:
            connector_name: Name of the connector
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
            if key not in ["name", "path", "primary_key", "schema"]:
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
    def test_stream_read(connector_name, manifest_config, stream_name, max_records=10):
        """Test a stream read using test-read action from the CDK.
        
        Args:
            connector_name: Name of the connector
            manifest_config: The manifest configuration as a dict
            stream_name: Name of the stream to test
            max_records: Maximum number of records to read for testing
        """
        try:
            from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
            
            source = ManifestDeclarativeSource(
                source_config=manifest_config,
                emit_connector_builder_messages=True
            )
            
            stream_config = None
            for stream in manifest_config.get("streams", []):
                if stream.get("name") == stream_name:
                    stream_config = stream
                    break
            
            if not stream_config:
                return f"Stream '{stream_name}' not found in manifest"
            
            streams = list(source.streams(manifest_config.get("config", {})))
            stream = next((s for s in streams if s.name == stream_name), None)
            
            if not stream:
                return f"Stream '{stream_name}' not found in source streams"
            
            records = []
            logs = []
            slices_processed = 0
            
            try:
                for slice_data in stream.stream_slices(sync_mode=None, cursor_field=None, stream_state={}):
                    slices_processed += 1
                    for record in stream.read_records(sync_mode=None, stream_slice=slice_data):
                        records.append(record.dict() if hasattr(record, 'dict') else dict(record))
                        if len(records) >= max_records:
                            break
                    if len(records) >= max_records:
                        break
                    if slices_processed >= 5:  # Limit slices for testing
                        break
            except Exception as read_error:
                logs.append(f"Read error: {str(read_error)}")
            
            summary = f"Stream read test completed for '{stream_name}':\n"
            summary += f"- Records read: {len(records)}\n"
            summary += f"- Log messages: {len(logs)}\n"
            summary += f"- Slices processed: {slices_processed}\n"
            
            if records:
                sample_record = records[0]
                if len(str(sample_record)) > 500:
                    sample_str = str(sample_record)[:500] + "..."
                else:
                    sample_str = json.dumps(sample_record, indent=2)
                summary += f"\nSample record:\n{sample_str}"
            
            if logs:
                summary += f"\nRecent logs:\n"
                for log in logs[-3:]:
                    summary += f"- {log}\n"
            
            return summary
            
        except Exception as e:
            return f"Error testing stream read: {str(e)}"
