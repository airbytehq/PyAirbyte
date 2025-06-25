# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Internal utility functions for MCP."""
import os
from pathlib import Path
from typing import Any

import yaml

from airbyte.secrets.hydration import deep_update
from airbyte.secrets.util import get_secret


def detect_hardcoded_secrets(config: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    """Detect hardcoded secrets in config that should be environment variables."""
    hardcoded_secrets = []
    properties = spec.get("properties", {})

    for field_name, field_value in config.items():
        if field_name not in properties:
            continue

        field_spec = properties[field_name]

        is_secret = (
            field_spec.get("writeOnly") is True
            or field_spec.get("format") == "password"
            or "password" in field_name.lower()
            or "secret" in field_name.lower()
            or "token" in field_name.lower()
            or "key" in field_name.lower()
        )

        if is_secret and isinstance(field_value, str):
            is_env_var = (
                (field_value.startswith("${") and field_value.endswith("}"))
                or os.environ.get(field_value) is not None
                or field_value.startswith("$")
            )

            if not is_env_var:
                hardcoded_secrets.append(field_name)

    return hardcoded_secrets


def resolve_config(
    config: dict | Path | None = None,
    config_secret_name: str | None = None,
    config_spec_jsonschema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve a configuration dictionary or file path to a dictionary."""
    config_dict: dict[str, Any] = {}
    if config is None and config_secret_name is None:
        raise ValueError(
            "No configuration provided. Either `config` or `config_secret_name` must be specified."
        )

    if isinstance(config, Path):
        config_dict.update(yaml.safe_load(config.read_text()))
    elif isinstance(config, dict):
        if isinstance(config, dict) and config_spec_jsonschema is not None:
            hardcoded_secrets = detect_hardcoded_secrets(config_dict, config_spec_jsonschema)
            if hardcoded_secrets:
                error_msg = f"Configuration contains hardcoded secrets in fields: {', '.join(hardcoded_secrets)}\n"
                error_msg += "Please use environment variables instead. For example:\n"
                for field in hardcoded_secrets:
                    clean_field_name = field.replace(".", "_").upper()
                    error_msg += (
                        f"- Set a `{clean_field_name}` environment variable and then specify "
                        f"`secret_reference::{clean_field_name}` in your config.\n"
                    )
                raise ValueError(error_msg)

        config_dict.update(config)
    elif config is not None:
        # We shouldn't reach here.
        raise ValueError(
            "Config must be a dict or a Path object pointing to a YAML or JSON file. "
            f"Found type: {type(config).__name__}"
        )

    if config_secret_name is not None:
        # Assume this is a secret name that points to a JSON/YAML config.
        secret_config = yaml.safe_load(get_secret(config_secret_name))
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
