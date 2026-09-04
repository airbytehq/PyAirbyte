# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Helpers for deferred Cloud connector authentication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SECRET_PLACEHOLDER = "__airbyte_placeholder__"
_UNSET = object()


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


def _supplied_secret_paths(
    value: object,
    schema: Mapping[str, Any],
    prefix: str = "",
) -> set[str]:
    """Return secret paths with non-null values supplied in a configuration."""
    paths: set[str] = set()
    if (
        prefix
        and value is not None
        and any(
            (
                schema.get("airbyte_secret") is True,
                schema.get("writeOnly") is True,
                schema.get("format") == "password",
            )
        )
    ):
        paths.add(prefix)

    items = schema.get("items")
    if isinstance(value, list) and isinstance(items, Mapping):
        for item in value:
            paths.update(_supplied_secret_paths(item, items, prefix))

    if isinstance(value, Mapping):
        for branch_key in ("oneOf", "anyOf"):
            branches = schema.get(branch_key)
            if not isinstance(branches, list):
                continue
            branch = _select_branch(branches, value)
            selected_branches = (
                [branch]
                if branch is not None
                else [candidate for candidate in branches if isinstance(candidate, Mapping)]
            )
            for candidate in selected_branches:
                paths.update(_supplied_secret_paths(value, candidate, prefix))

        all_of = schema.get("allOf")
        if isinstance(all_of, list):
            for branch in all_of:
                if isinstance(branch, Mapping):
                    paths.update(_supplied_secret_paths(value, branch, prefix))

        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            for name, child in properties.items():
                if not isinstance(name, str) or not isinstance(child, Mapping):
                    continue
                if name not in value:
                    continue
                child_prefix = f"{prefix}.{name}" if prefix else name
                paths.update(_supplied_secret_paths(value[name], child, child_prefix))

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
            if "const" in child:
                discriminators[name] = child["const"]
            else:
                enum = child.get("enum")
                if isinstance(enum, list) and len(enum) == 1:
                    discriminators[name] = enum[0]
        if discriminators and all(
            name in value and value[name] == expected for name, expected in discriminators.items()
        ):
            return branch
    return None


def _discriminator_values(schema: Mapping[str, Any]) -> dict[str, Any]:
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        return {}
    values: dict[str, Any] = {}
    for name, child in properties.items():
        if not isinstance(name, str) or not isinstance(child, Mapping):
            continue
        if "const" in child:
            values[name] = child["const"]
            continue
        enum = child.get("enum")
        if isinstance(enum, list) and len(enum) == 1:
            values[name] = enum[0]
    return values


def _is_secret_schema(schema: Mapping[str, Any]) -> bool:
    return any(
        (
            schema.get("airbyte_secret") is True,
            schema.get("writeOnly") is True,
            schema.get("format") == "password",
        )
    )


def _default_branch_value(
    value: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> tuple[Mapping[str, Any], dict[str, Any]] | None:
    for branch_key in ("oneOf", "anyOf"):
        branches = schema.get(branch_key)
        if not isinstance(branches, list):
            continue
        for branch in branches:
            if not isinstance(branch, Mapping):
                continue
            properties = branch.get("properties", {})
            if not isinstance(properties, Mapping):
                continue
            candidate = dict(value)
            candidate.update(
                {
                    name: expected
                    for name, expected in _discriminator_values(branch).items()
                    if name not in candidate
                }
            )
            required = branch.get("required", [])
            if not isinstance(required, list):
                required = []
            satisfiable = True
            for name in required:
                if not isinstance(name, str) or name in candidate:
                    continue
                child = properties.get(name)
                if not isinstance(child, Mapping):
                    satisfiable = False
                    break
                if _is_secret_schema(child):
                    candidate[name] = SECRET_PLACEHOLDER
                elif "default" in child:
                    candidate[name] = child["default"]
                elif isinstance(child.get("oneOf"), list) or isinstance(child.get("anyOf"), list):
                    nested = _default_branch_value({}, child)
                    if nested is None:
                        satisfiable = False
                        break
                    _, candidate[name] = nested
                else:
                    satisfiable = False
                    break
            if satisfiable:
                return branch, candidate
    return None


def _is_required_container(
    schema: Mapping[str, Any],
    name: str,
    child: Mapping[str, Any],
) -> bool:
    required = schema.get("required")
    return (
        isinstance(required, list)
        and name in required
        and any(key in child for key in ("properties", "allOf", "oneOf", "anyOf"))
    )


def _stub_missing_secrets(  # noqa: PLR0912
    value: object,
    schema: Mapping[str, Any],
    *,
    _allow_default_branch: bool = False,
) -> object:
    """Fill missing secret fields with placeholder values.

    Missing secret properties receive placeholder values so required secret
    fields are present in the configuration sent to the Cloud API at source
    create time.
    """
    items = schema.get("items")
    if isinstance(value, list) and isinstance(items, Mapping):
        return [
            _stub_missing_secrets(item, items, _allow_default_branch=_allow_default_branch)
            for item in value
        ]
    for branch_key in ("oneOf", "anyOf"):
        branches = schema.get(branch_key)
        if isinstance(branches, list) and isinstance(value, Mapping):
            branch = _select_branch(branches, value)
            if branch is not None:
                value = _stub_missing_secrets(value, branch)
            elif _allow_default_branch:
                default_branch = _default_branch_value(value, {branch_key: branches})
                if default_branch is not None:
                    branch, value = default_branch
                    value = _stub_missing_secrets(value, branch)
    all_of = schema.get("allOf")
    if isinstance(all_of, list):
        for branch in all_of:
            if isinstance(branch, Mapping):
                value = _stub_missing_secrets(
                    value,
                    branch,
                    _allow_default_branch=_allow_default_branch,
                )
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
            if name not in result or result[name] is None:
                result[name] = SECRET_PLACEHOLDER
        elif name in result:
            result[name] = _stub_missing_secrets(
                result[name],
                child,
                _allow_default_branch=(
                    isinstance(schema.get("required"), list) and name in schema["required"]
                ),
            )
        elif _is_required_container(schema, name, child):
            materialized = _stub_missing_secrets(
                {},
                child,
                _allow_default_branch=True,
            )
            if isinstance(materialized, Mapping) and materialized:
                result[name] = materialized
    return result


__all__ = [
    "SECRET_PLACEHOLDER",
    "_paths_present",
    "_schema_secret_paths",
    "_select_branch",
    "_supplied_secret_paths",
    "_stub_missing_secrets",
]
