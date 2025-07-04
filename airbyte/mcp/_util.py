# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Internal utility functions for MCP."""

import logging
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import dotenv
import structlog
import yaml

from airbyte.constants import NO_LIVE_PROGRESS
from airbyte.logs import AIRBYTE_STRUCTURED_LOGGING
from airbyte.secrets import GoogleGSMSecretManager, register_secret_manager
from airbyte.secrets.hydration import deep_update, detect_hardcoded_secrets
from airbyte.secrets.util import get_secret, is_secret_available


if not NO_LIVE_PROGRESS:
    os.environ["NO_LIVE_PROGRESS"] = "1"


@lru_cache
def get_mcp_logger() -> logging.Logger | structlog.BoundLogger:
    """Get a logger for MCP server operations that respects structured logging settings."""
    if AIRBYTE_STRUCTURED_LOGGING:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        logger = logging.getLogger("airbyte.mcp")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        for handler in logger.handlers:
            logger.removeHandler(handler)

        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

        return structlog.get_logger("airbyte.mcp")
    logger = logging.getLogger("airbyte.mcp")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in logger.handlers:
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

    return logger


def log_mcp_message(
    message: str, level: str = "info", **kwargs: str | float | bool | None
) -> None:
    """Log a message using the MCP logger with appropriate formatting."""
    logger = get_mcp_logger()

    if AIRBYTE_STRUCTURED_LOGGING:
        getattr(logger, level)(message, **kwargs)
    else:
        getattr(logger, level)(message)


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
        dotenv_path = Path(os.environ[AIRBYTE_MCP_DOTENV_PATH_ENVVAR])
        _load_dotenv_file(dotenv_path)

    if is_secret_available("GCP_GSM_CREDENTIALS") and is_secret_available("GCP_GSM_PROJECT_ID"):
        # Initialize the GoogleGSMSecretManager if the credentials and project are set.
        register_secret_manager(
            GoogleGSMSecretManager(
                project=get_secret("GCP_GSM_PROJECT_ID"),
                credentials_json=get_secret("GCP_GSM_CREDENTIALS"),
            )
        )


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
