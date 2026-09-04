# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Helpers for deferred Cloud connector authentication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SECRET_PLACEHOLDER = "__airbyte_placeholder__"


def _schema_secret_paths(schema: Mapping[str, Any], prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if prefix and any(
        (
            schema.get("airbyte_secret") is True,
            schema.get("writeOnly") is True,
            schema.get("format") == "password",
        )
    ):
        paths.add(prefix)
    properties = schema.get("properties", {})
    if isinstance(properties, Mapping):
        for name, child in properties.items():
            if not isinstance(name, str) or not isinstance(child, Mapping):
                continue
            path = f"{prefix}.{name}" if prefix else name
            paths.update(_schema_secret_paths(child, path))
    items = schema.get("items")
    if isinstance(items, Mapping):
        paths.update(_schema_secret_paths(items, prefix))
    for branch_key in ("oneOf", "anyOf", "allOf"):
        branches = schema.get(branch_key, [])
        if not isinstance(branches, list):
            continue
        for branch in branches:
            if isinstance(branch, Mapping):
                paths.update(_schema_secret_paths(branch, prefix))
    return paths


def _paths_present(value: object, prefix: str = "") -> set[str]:
    if isinstance(value, (list, tuple)):
        paths: set[str] = set()
        for item in value:
            paths.update(_paths_present(item, prefix))
        return paths
    if not isinstance(value, Mapping):
        return set()
    paths: set[str] = set()
    for name, child in value.items():
        if not isinstance(name, str):
            continue
        path = f"{prefix}.{name}" if prefix else name
        paths.add(path)
        paths.update(_paths_present(child, path))
    return paths


def _select_branch(
    branches: list[Any],
    value: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    for branch in branches:
        if not isinstance(branch, Mapping):
            continue
        properties = branch.get("properties", {})
        if not isinstance(properties, Mapping):
            continue
        discriminators: dict[str, Any] = {}
        for name, child in properties.items():
            if not isinstance(child, Mapping):
                continue
            if child.get("const") is not None:
                discriminators[name] = child["const"]
            else:
                enum = child.get("enum")
                if isinstance(enum, list) and len(enum) == 1:
                    discriminators[name] = enum[0]
        if discriminators and all(
            value.get(name) == expected for name, expected in discriminators.items()
        ):
            return branch
    return None


def _stub_missing_secrets(value: object, schema: Mapping[str, Any]) -> object:
    """Fill missing secret fields with placeholder values.

    Missing secret properties receive placeholder values so Cloud schema
    validation passes at source create time.
    """
    for branch_key in ("oneOf", "anyOf"):
        branches = schema.get(branch_key)
        if isinstance(branches, list) and isinstance(value, Mapping):
            branch = _select_branch(branches, value)
            if branch is not None:
                return _stub_missing_secrets(value, branch)
    properties = schema.get("properties")
    if not isinstance(properties, Mapping) or not isinstance(value, Mapping):
        return value
    result: dict[str, Any] = dict(value)
    for name, child in properties.items():
        if not isinstance(name, str) or not isinstance(child, Mapping):
            continue
        is_secret = (
            child.get("airbyte_secret") is True
            or child.get("writeOnly") is True
            or child.get("format") == "password"
        )
        if is_secret:
            if not result.get(name):
                result[name] = SECRET_PLACEHOLDER
        elif name in result:
            result[name] = _stub_missing_secrets(result[name], child)
    return result


__all__ = [
    "SECRET_PLACEHOLDER",
    "_paths_present",
    "_schema_secret_paths",
    "_select_branch",
    "_stub_missing_secrets",
]
