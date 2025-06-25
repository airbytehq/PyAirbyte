# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Internal utility functions for MCP."""

from pathlib import Path
from typing import Any

import yaml

from airbyte.secrets.hydration import deep_update, detect_hardcoded_secrets
from airbyte.secrets.util import get_secret


def resolve_config(
    config: dict | Path | None = None,
    config_secret_name: str | None = None,
    config_spec_jsonschema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve a configuration dictionary or file path to a dictionary.

    We reject hardcoded secrets in a config dict if we detect them.
    """
    config_dict: dict[str, Any] = {}
    if config is None and config_secret_name is None:
        raise ValueError(
            "No configuration provided. Either `config` or `config_secret_name` must be specified."
        )

    if isinstance(config, Path):
        config_dict.update(yaml.safe_load(config.read_text()))
    elif isinstance(config, dict):
        if config_spec_jsonschema is not None:
            hardcoded_secrets: list[list[str]] = detect_hardcoded_secrets(
                config=config,
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
