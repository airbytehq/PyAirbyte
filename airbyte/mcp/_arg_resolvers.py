# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Argument resolver functions for MCP tools.

This module provides functions to resolve and validate arguments passed to MCP tools,
including connector configurations and list-of-strings arguments.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, overload

import yaml

from airbyte.secrets.hydration import deep_update, detect_hardcoded_secrets
from airbyte.secrets.util import get_secret


# Hint: Null result if input is Null
@overload
def resolve_list_of_strings(value: None) -> None: ...


# Hint: Non-null result if input is non-null
@overload
def resolve_list_of_strings(value: str | list[str] | set[str]) -> list[str]: ...


def resolve_list_of_strings(value: str | list[str] | set[str] | None) -> list[str] | None:
    """Resolve a string or list of strings to a list of strings.

    This method will handle three types of input:

    1. A list of strings (e.g., ["stream1", "stream2"]) will be returned as-is.
    2. None or empty input will return None.
    3. A single CSV string (e.g., "stream1,stream2") will be split into a list.
    4. A JSON string (e.g., '["stream1", "stream2"]') will be parsed into a list.
    5. If the input is empty or None, an empty list will be returned.

    Args:
        value: A string or list of strings.
    """
    if value is None:
        return None

    if isinstance(value, list):
        return value

    if isinstance(value, set):
        return list(value)

    if not isinstance(value, str):
        raise TypeError(
            "Expected a string, list of strings, a set of strings, or None. "
            f"Got '{type(value).__name__}': {value}"
        )

    value = value.strip()
    if not value:
        return []

    if value.startswith("[") and value.endswith("]"):
        # Try to parse as JSON array:
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list) and all(isinstance(item, str) for item in parsed):
                return parsed
        except json.JSONDecodeError as ex:
            raise ValueError(f"Invalid JSON array: {value}") from ex

    # Fallback to CSV split:
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_connector_config(  # noqa: PLR0912
    config: dict | str | None = None,
    config_file: str | Path | None = None,
    config_secret_name: str | None = None,
    config_spec_jsonschema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve a configuration dictionary, JSON string, or file path to a dictionary.

    Returns:
        Resolved configuration dictionary (empty if no inputs provided)

    Raises:
        ValueError: If JSON parsing fails or a provided input is invalid

    We reject hardcoded secrets in a config dict if we detect them.
    """
    config_dict: dict[str, Any] = {}

    if config is None and config_file is None and config_secret_name is None:
        return {}

    if config_file is not None:
        if isinstance(config_file, str):
            config_file = Path(config_file)

        if not isinstance(config_file, Path):
            raise ValueError(
                f"config_file must be a string or Path object, got: {type(config_file).__name__}"
            )

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        def _raise_invalid_type(file_config: object) -> None:
            raise TypeError(
                f"Configuration file must contain a valid JSON/YAML object, "
                f"got: {type(file_config).__name__}"
            )

        try:
            file_config = yaml.safe_load(config_file.read_text())
            if not isinstance(file_config, dict):
                _raise_invalid_type(file_config)
            config_dict.update(file_config)
        except Exception as e:
            raise ValueError(f"Error reading configuration file {config_file}: {e}") from e

    if config is not None:
        if isinstance(config, dict):
            config_dict.update(config)
        elif isinstance(config, str):
            try:
                parsed_config = json.loads(config)
                if not isinstance(parsed_config, dict):
                    raise TypeError(
                        f"Parsed JSON config must be an object/dict, "
                        f"got: {type(parsed_config).__name__}"
                    )
                config_dict.update(parsed_config)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in config parameter: {e}") from e
        else:
            raise ValueError(f"Config must be a dict or JSON string, got: {type(config).__name__}")

    if config_dict and config_spec_jsonschema is not None:
        hardcoded_secrets: list[list[str]] = detect_hardcoded_secrets(
            config=config_dict,
            spec_json_schema=config_spec_jsonschema,
        )
        if hardcoded_secrets:
            error_msg = "Configuration contains hardcoded secrets in fields: "
            error_msg += ", ".join(
                [".".join(hardcoded_secret) for hardcoded_secret in hardcoded_secrets]
            )

            error_msg += (
                "Please use environment variables instead. For example:\n"
                "To set a secret via reference, set its value to "
                "`secret_reference::ENV_VAR_NAME`.\n"
            )
            raise ValueError(error_msg)

    if config_secret_name is not None:
        # Assume this is a secret name that points to a JSON/YAML config.
        secret_config = yaml.safe_load(str(get_secret(config_secret_name)))
        if not isinstance(secret_config, dict):
            raise ValueError(
                f"Secret '{config_secret_name}' must contain a valid JSON or YAML object, "
                f"but got: {type(secret_config).__name__}"
            )

        # Merge the secret config into the main config:
        deep_update(
            config_dict,
            secret_config,
        )

    return config_dict
