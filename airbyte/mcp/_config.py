# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Internal utility functions for MCP secret loading."""

from __future__ import annotations

import os
from pathlib import Path

import dotenv

from airbyte._util.meta import is_interactive
from airbyte.mcp._guards import is_trusted_execution_enabled
from airbyte.secrets import (
    DotenvSecretManager,
    GoogleGSMSecretManager,
    SecretSourceEnum,
    register_secret_manager,
)
from airbyte.secrets.config import disable_secret_source
from airbyte.secrets.util import get_secret, is_secret_available


AIRBYTE_MCP_DOTENV_PATH_ENVVAR = "AIRBYTE_MCP_ENV_FILE"


def _load_dotenv_file(dotenv_path: Path | str) -> None:
    """Load environment variables from a .env file."""
    if isinstance(dotenv_path, str):
        dotenv_path = Path(dotenv_path)
    if not dotenv_path.exists():
        raise FileNotFoundError(f".env file not found: {dotenv_path}")

    dotenv.load_dotenv(dotenv_path=dotenv_path)


def load_secrets_to_env_vars() -> None:
    """Load secrets from dotenv files and secret managers into environment variables.

    This function must be called before mcp_server() so that config args can resolve
    from the loaded environment variables.

    Note: Later secret manager registrations have higher priority than earlier ones.

    When trusted execution is disabled (`airbyte.mcp._guards.is_trusted_execution_enabled`),
    the dotenv and Google Secret Manager backends are *not* registered as secret sources.
    This prevents an untrusted (for example hosted HTTP) deployment from resolving
    server-side secrets into connector config via `config_secret_name` /
    `secret_reference::`. Ordinary environment variables are still loaded from any
    referenced dotenv file so that safe, non-secret-manager configuration continues to
    work.
    """
    trusted = is_trusted_execution_enabled()

    # Load the .env file from the current working directory.
    envrc_path = Path.cwd() / ".envrc"
    if envrc_path.exists():
        _load_dotenv_file(envrc_path)
        if trusted:
            register_secret_manager(
                DotenvSecretManager(envrc_path),
            )

    if AIRBYTE_MCP_DOTENV_PATH_ENVVAR in os.environ:
        dotenv_path = Path(os.environ[AIRBYTE_MCP_DOTENV_PATH_ENVVAR]).absolute()
        _load_dotenv_file(dotenv_path)
        if trusted:
            register_secret_manager(
                DotenvSecretManager(dotenv_path),
            )

    if (
        trusted
        and is_secret_available("GCP_GSM_CREDENTIALS")
        and is_secret_available("GCP_GSM_PROJECT_ID")
    ):
        # Initialize the GoogleGSMSecretManager if the credentials and project are set.
        register_secret_manager(
            GoogleGSMSecretManager(
                project=get_secret("GCP_GSM_PROJECT_ID"),
                credentials_json=get_secret("GCP_GSM_CREDENTIALS"),
            )
        )

    # Make sure we disable the prompt source in non-interactive environments.
    if not is_interactive():
        disable_secret_source(SecretSourceEnum.PROMPT)
