# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Validate deferred-auth placeholder configurations against registry schemas."""

from __future__ import annotations

import json
import re
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
_PATTERN_CANDIDATES = (
    "2024-01-01T00:00:00Z",
    "2024-01-01",
    "2024-01-01T00:00:00.000Z",
    "0",
    "1",
    "x",
    "a1",
    "https://example.com",
)


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


def _discriminator_values(schema: Mapping[str, Any]) -> dict[str, Any]:
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        return {}
    values: dict[str, Any] = {}
    for name, child in properties.items():
        if not isinstance(name, str) or not isinstance(child, Mapping):
            continue
        if "const" in child and _matches_type(child["const"], child):
            values[name] = child["const"]
        else:
            enum = child.get("enum")
            if (
                isinstance(enum, list)
                and len(enum) == 1
                and _matches_type(enum[0], child)
            ):
                values[name] = enum[0]
    return values


def _fill_required_non_secrets_result(
    schema: Mapping[str, Any],
) -> dict[str, Any] | object:
    result: dict[str, Any] = {}
    for branch_key in ("oneOf", "anyOf"):
        branches = schema.get(branch_key)
        if not isinstance(branches, list):
            continue
        selected: dict[str, Any] | object = _UNSET
        for branch in branches:
            if not isinstance(branch, Mapping):
                continue
            candidate = _fill_required_non_secrets_result(branch)
            if candidate is not _UNSET:
                selected = candidate
                break
        if selected is _UNSET:
            return _UNSET
        result.update(selected)

    result.update({
        name: value
        for name, value in _discriminator_values(schema).items()
        if name not in result
    })
    properties = schema.get("properties")
    required = schema.get("required")
    if isinstance(properties, Mapping) and isinstance(required, list):
        for name in required:
            if not isinstance(name, str):
                continue
            child = properties.get(name)
            if not isinstance(child, Mapping):
                return _UNSET
            if name in _schema_secret_paths(child, name):
                continue
            value = _synthesize_value(child)
            if value is _UNSET:
                return _UNSET
            result[name] = value

    all_of = schema.get("allOf")
    if isinstance(all_of, list):
        for branch in all_of:
            if not isinstance(branch, Mapping):
                continue
            candidate = _fill_required_non_secrets_result(branch)
            if candidate is _UNSET:
                return _UNSET
            result.update(candidate)
    return result


def _synthesize_value(schema: Mapping[str, Any]) -> Any:
    for key in ("default", "const"):
        if key in schema and _matches_type(schema[key], schema):
            return schema[key]

    if isinstance(schema.get("oneOf"), list) or isinstance(schema.get("anyOf"), list):
        return _fill_required_non_secrets_result(schema)

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
            try:
                for candidate in _PATTERN_CANDIDATES:
                    if re.fullmatch(pattern, candidate):
                        return candidate
            except re.error:
                pass
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
            examples = schema.get("examples")
            if (
                isinstance(examples, list)
                and examples
                and _matches_type(examples[0], items)
            ):
                return [examples[0]]
            item = _synthesize_value(items)
            if item is not _UNSET:
                return [item]
        return _UNSET
    if schema_type == "object" or "properties" in schema or "allOf" in schema:
        result = _fill_required_non_secrets_result(schema)
        return result if result is not _UNSET else _UNSET
    return _UNSET


def _fill_required_non_secrets(schema: Mapping[str, Any]) -> dict[str, Any]:
    result = _fill_required_non_secrets_result(schema)
    return result if result is not _UNSET else {}


EXPECTED_FAILURES: dict[str, str] = {}

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
