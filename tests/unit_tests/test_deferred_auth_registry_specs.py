# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Validate deferred-auth placeholder configurations against registry schemas."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import requests

from airbyte._util.registry_spec import get_connector_spec_from_registry
from airbyte.cloud._deferred_auth import (
    _schema_secret_paths,
    _stub_missing_secrets,
)
from airbyte.registry import (
    InstallType,
    get_available_connectors,
    get_connector_metadata,
)


def _certified_source_names() -> list[str]:
    try:
        connector_names = get_available_connectors(InstallType.ANY)
        return [
            name
            for name in connector_names
            if name.startswith("source-")
            and (
                (metadata := get_connector_metadata(name)) is not None
                and metadata.support_level == "certified"
            )
        ]
    except (requests.exceptions.RequestException, OSError):
        return []


def _cached_spec(connector_name: str) -> dict[str, Any] | None:
    metadata = get_connector_metadata(connector_name)
    if metadata is None or metadata.latest_available_version is None:
        return None

    version = metadata.latest_available_version
    cache_path = (
        Path(tempfile.gettempdir())
        / "pyairbyte-registry-spec-cache"
        / f"{connector_name}-{version}.json"
    )
    if cache_path.exists():
        try:
            cached_spec = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached_spec, dict):
                return cached_spec
        except (OSError, json.JSONDecodeError):
            pass  # Unreadable or corrupt cache entry; fall through to refetch.

    spec = None
    for platform in ("cloud", "oss"):
        spec = get_connector_spec_from_registry(
            connector_name,
            version=version,
            platform=platform,
        )
        if spec is not None:
            break
    if spec is None:
        return None

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(spec), encoding="utf-8")
    return spec


_UNSET = object()


def _matches_type(value: Any, schema: Mapping[str, Any]) -> bool:
    schema_type = schema.get("type")
    if schema_type is None:
        return True
    if isinstance(schema_type, list):
        return any(_matches_type(value, {"type": item}) for item in schema_type)
    if schema_type == "string":
        return isinstance(value, str)
    if schema_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if schema_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if schema_type == "boolean":
        return isinstance(value, bool)
    if schema_type == "array":
        return isinstance(value, list)
    if schema_type == "object":
        return isinstance(value, Mapping)
    return True


def _synthesize_value(schema: Mapping[str, Any]) -> Any:
    for key in ("default", "const"):
        if key in schema and _matches_type(schema[key], schema):
            return schema[key]

    if isinstance(schema.get("oneOf"), list) or isinstance(schema.get("anyOf"), list):
        return _UNSET

    examples = schema.get("examples")
    if isinstance(examples, list) and examples and _matches_type(examples[0], schema):
        return examples[0]

    enum = schema.get("enum")
    if isinstance(enum, list) and enum and _matches_type(enum[0], schema):
        return enum[0]

    schema_type = schema.get("type")
    if schema_type == "string":
        pattern = schema.get("pattern")
        if pattern:
            return _UNSET
        schema_format = schema.get("format")
        if schema_format == "date":
            return "2024-01-01"
        if schema_format == "date-time":
            return "2024-01-01T00:00:00Z"
        return "x"
    if schema_type in ("integer", "number"):
        minimum = schema.get("minimum")
        return minimum if isinstance(minimum, (int, float)) else 1
    if schema_type == "boolean":
        return False
    if schema_type == "array":
        items = schema.get("items")
        if isinstance(items, Mapping):
            item = _synthesize_value(items)
            if item is not _UNSET:
                return [item]
        return _UNSET
    if schema_type == "object" or "properties" in schema or "allOf" in schema:
        return _fill_required_non_secrets(schema)
    return _UNSET


def _fill_required_non_secrets(schema: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    properties = schema.get("properties")
    required = schema.get("required")
    if isinstance(properties, Mapping) and isinstance(required, list):
        for name in required:
            if not isinstance(name, str):
                continue
            child = properties.get(name)
            if not isinstance(child, Mapping):
                continue
            if name in _schema_secret_paths(child, name):
                continue
            value = _synthesize_value(child)
            if value is not _UNSET:
                result[name] = value

    all_of = schema.get("allOf")
    if isinstance(all_of, list):
        for branch in all_of:
            if isinstance(branch, Mapping):
                result.update(_fill_required_non_secrets(branch))
    return result


EXPECTED_FAILURES: dict[str, str] = {
    "source-azure-blob-storage": "requires choosing an unselectable credentials or stream format branch",
    "source-db2-enterprise": "requires choosing an unselectable encryption branch",
    "source-facebook-marketing": (
        "required account IDs have a regex-constrained array item with no example"
    ),
    "source-file": "requires choosing an unselectable provider branch",
    "source-gcs": "requires choosing an unselectable credentials or stream format branch",
    "source-github": "requires choosing an unselectable credentials branch",
    "source-gitlab": "requires choosing an unselectable credentials branch",
    "source-google-drive": "requires choosing an unselectable credentials or stream format branch",
    "source-google-search-console": "requires choosing an unselectable authorization branch",
    "source-google-sheets": "requires choosing an unselectable credentials branch",
    "source-hubspot": "requires choosing an unselectable credentials branch",
    "source-jira": "requires choosing an unselectable credentials branch",
    "source-mongodb-v2": "requires choosing an unselectable database_config branch",
    "source-mssql": "requires choosing an unselectable replication_method branch",
    "source-mysql": "requires choosing an unselectable replication_method branch",
    "source-netsuite-enterprise": "requires choosing an unselectable authentication_method branch",
    "source-oracle-enterprise": "requires choosing an unselectable connection_data branch",
    "source-postgres": "requires choosing an unselectable tunnel_method branch",
    "source-s3": "requires choosing an unselectable stream format branch",
    "source-sap-hana-enterprise": "requires choosing an unselectable encryption branch",
    "source-sendgrid": "required start_date has a regex pattern with no example",
    "source-sharepoint-enterprise": "requires choosing an unselectable credentials or stream format branch",
    "source-typeform": "requires choosing an unselectable credentials branch",
}

_CERTIFIED_SOURCE_NAMES = _certified_source_names()
if not _CERTIFIED_SOURCE_NAMES:
    pytest.skip("registry unreachable", allow_module_level=True)


@pytest.mark.parametrize(
    "connector_name",
    [
        pytest.param(
            name,
            id=name,
            marks=pytest.mark.xfail(
                reason=EXPECTED_FAILURES[name],
                strict=False,
            ),
        )
        if name in EXPECTED_FAILURES
        else pytest.param(name, id=name)
        for name in _CERTIFIED_SOURCE_NAMES
    ],
)
def test_stubbed_config_passes_registry_schema(connector_name: str) -> None:
    spec = _cached_spec(connector_name)
    if spec is None:
        pytest.skip(f"no spec available for {connector_name}")

    config = _fill_required_non_secrets(spec)
    config = _stub_missing_secrets(config, spec)
    jsonschema.validate(instance=config, schema=spec)
