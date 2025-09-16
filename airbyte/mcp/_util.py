# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Internal utility functions for MCP."""

import json
import os
from pathlib import Path
from typing import Any

import dotenv
import yaml

from airbyte.secrets import DotenvSecretManager, GoogleGSMSecretManager, register_secret_manager
from airbyte.secrets.hydration import deep_update, detect_hardcoded_secrets
from airbyte.secrets.util import get_secret, is_secret_available


AIRBYTE_MCP_DOTENV_PATH_ENVVAR = "AIRBYTE_MCP_ENV_FILE"


def _load_dotenv_file(dotenv_path: Path | str) -> None:
    """Load environment variables from a .env file."""
    if isinstance(dotenv_path, str):
        dotenv_path = Path(dotenv_path)
    if not dotenv_path.exists():
        raise FileNotFoundError(f".env file not found: {dotenv_path}")

    dotenv.load_dotenv(dotenv_path=dotenv_path)


def initialize_secrets() -> None:
    """Initialize dotenv to load environment variables from .env files."""
    # Load the .env file from the current working directory.
    if AIRBYTE_MCP_DOTENV_PATH_ENVVAR in os.environ:
        dotenv_path = Path(os.environ[AIRBYTE_MCP_DOTENV_PATH_ENVVAR]).absolute()
        custom_dotenv_secret_mgr = DotenvSecretManager(dotenv_path)
        _load_dotenv_file(dotenv_path)
        register_secret_manager(
            custom_dotenv_secret_mgr,
        )

    envrc_path = Path.cwd() / ".envrc"
    if envrc_path.exists():
        envrc_secret_mgr = DotenvSecretManager(envrc_path)
        _load_dotenv_file(envrc_path)
        register_secret_manager(
            envrc_secret_mgr,
        )

    if is_secret_available("GCP_GSM_CREDENTIALS") and is_secret_available("GCP_GSM_PROJECT_ID"):
        # Initialize the GoogleGSMSecretManager if the credentials and project are set.
        register_secret_manager(
            GoogleGSMSecretManager(
                project=get_secret("GCP_GSM_PROJECT_ID"),
                credentials_json=get_secret("GCP_GSM_CREDENTIALS"),
            )
        )


def resolve_config(  # noqa: PLR0912
    config: dict | str | None = None,
    config_file: str | Path | None = None,
    config_secret_name: str | None = None,
    config_spec_jsonschema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve a configuration dictionary, JSON string, or file path to a dictionary.

    Returns:
        Resolved configuration dictionary

    Raises:
        ValueError: If no configuration provided or if JSON parsing fails

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
