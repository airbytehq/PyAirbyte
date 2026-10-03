# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Typed settings for PyAirbyte.

`AirbyteSettings` reads environment variables when instantiated, so a new instance observes
environment changes and dotenv files loaded after import. It does not cache settings or create
directories. `AirbyteCloudSettings` reads Cloud configuration from the process environment and
`./.env`, with process environment values taking precedence. Neither settings class caches values.
Each field documents its corresponding environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from airbyte._util.text_util import _str_to_bool
from airbyte.constants import (
    AIRBYTE_BEARER_TOKEN_ENV_VAR,
    AIRBYTE_CLIENT_ID_ENV_VAR,
    AIRBYTE_CLIENT_SECRET_ENV_VAR,
    AIRBYTE_ORGANIZATION_ID_ENV_VAR,
    AIRBYTE_WORKSPACE_ID_ENV_VAR,
    CLOUD_API_ROOT,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_ORGANIZATION_ID_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.secrets.base import SecretString  # noqa: TC001


class AirbyteSettings(BaseSettings):
    """Environment-backed configuration for PyAirbyte."""

    model_config = SettingsConfigDict(
        env_prefix="AIRBYTE_",
        env_ignore_empty=True,
        extra="ignore",
    )

    project_dir: Path = Field(
        default_factory=Path.cwd,
        description=(
            "Project directory (`AIRBYTE_PROJECT_DIR`). Defaults to the current working "
            "directory and is the parent for the default install and cache directories."
        ),
    )
    install_dir: Path = Field(
        default_factory=lambda data: data["project_dir"],
        description=(
            "Connector install directory (`AIRBYTE_INSTALL_DIR`). Defaults to `project_dir`."
        ),
    )
    cache_root: Path = Field(
        default_factory=lambda data: data["project_dir"] / ".cache",
        description=(
            "Root for cache files (`AIRBYTE_CACHE_ROOT`). Defaults to `.cache` in `project_dir`."
        ),
    )
    temp_dir: Path | None = Field(
        default=None,
        description=(
            "Directory for temporary files (`AIRBYTE_TEMP_DIR`). If unset, the system default "
            "temporary directory is used."
        ),
    )
    temp_file_cleanup: bool = Field(
        default=True,
        description=(
            "Whether to clean up temporary files after use (`AIRBYTE_TEMP_FILE_CLEANUP`)."
        ),
    )
    offline_mode: bool = Field(
        default=False,
        description=(
            "Offline mode (`AIRBYTE_OFFLINE_MODE`). Prevents registry connectivity errors and "
            "disables telemetry."
        ),
    )
    print_full_error_logs: bool = Field(
        default_factory=lambda: _str_to_bool(os.getenv("CI"), default=False),
        description=(
            "Whether to print full error logs (`AIRBYTE_PRINT_FULL_ERROR_LOGS`). Defaults to "
            "true when `CI` is set to a recognized truthy value, otherwise false."
        ),
    )
    no_uv: bool = Field(
        default=False,
        description="Whether to use pip instead of uv for connector installs (`AIRBYTE_NO_UV`).",
    )
    structured_logging: bool = Field(
        default=False,
        description="Whether to enable structured JSON logging (`AIRBYTE_STRUCTURED_LOGGING`).",
    )
    logging_root: Path | None = Field(
        default=None,
        description=(
            "Root directory for logs (`AIRBYTE_LOGGING_ROOT`). Defaults to a system temporary "
            "directory."
        ),
    )
    local_registry: str | None = Field(
        default=None,
        description=(
            "Custom connector registry URL (`AIRBYTE_LOCAL_REGISTRY`). The strings `0`, "
            "`false`, or `f` disable the registry."
        ),
    )

    @field_validator(
        "project_dir",
        "install_dir",
        "cache_root",
        "temp_dir",
        "logging_root",
        mode="after",
    )
    @classmethod
    def _expand_and_make_absolute(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        return value.expanduser().absolute()


class AirbyteCloudSettings(BaseSettings):
    """Cloud credentials and URLs read from the process environment and `./.env`.

    Process environment values take precedence over values in `./.env`. Settings are not cached,
    so each instance reflects the environment at the time it is created.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    client_id: SecretString | None = Field(
        default=None,
        validation_alias=AliasChoices(AIRBYTE_CLIENT_ID_ENV_VAR, CLOUD_CLIENT_ID_ENV_VAR),
        description=(
            f"Cloud client ID (`{AIRBYTE_CLIENT_ID_ENV_VAR}` or " f"`{CLOUD_CLIENT_ID_ENV_VAR}`)."
        ),
    )
    client_secret: SecretString | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AIRBYTE_CLIENT_SECRET_ENV_VAR,
            CLOUD_CLIENT_SECRET_ENV_VAR,
        ),
        description=(
            "Cloud client secret "
            f"(`{AIRBYTE_CLIENT_SECRET_ENV_VAR}` or `{CLOUD_CLIENT_SECRET_ENV_VAR}`)."
        ),
    )
    bearer_token: SecretString | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AIRBYTE_BEARER_TOKEN_ENV_VAR,
            CLOUD_BEARER_TOKEN_ENV_VAR,
        ),
        description=(
            "Cloud bearer token "
            f"(`{AIRBYTE_BEARER_TOKEN_ENV_VAR}` or `{CLOUD_BEARER_TOKEN_ENV_VAR}`)."
        ),
    )
    workspace_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AIRBYTE_WORKSPACE_ID_ENV_VAR,
            CLOUD_WORKSPACE_ID_ENV_VAR,
        ),
        description=(
            "Cloud workspace ID "
            f"(`{AIRBYTE_WORKSPACE_ID_ENV_VAR}` or `{CLOUD_WORKSPACE_ID_ENV_VAR}`)."
        ),
    )
    organization_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AIRBYTE_ORGANIZATION_ID_ENV_VAR,
            CLOUD_ORGANIZATION_ID_ENV_VAR,
        ),
        description=(
            "Cloud organization ID "
            f"(`{AIRBYTE_ORGANIZATION_ID_ENV_VAR}` or `{CLOUD_ORGANIZATION_ID_ENV_VAR}`)."
        ),
    )
    api_url: str = Field(
        default=CLOUD_API_ROOT,
        validation_alias=CLOUD_API_ROOT_ENV_VAR,
        description=f"Cloud API root URL (`{CLOUD_API_ROOT_ENV_VAR}`).",
    )
    config_api_url: str | None = Field(
        default=None,
        validation_alias=CLOUD_CONFIG_API_ROOT_ENV_VAR,
        description=f"Cloud Config API root URL (`{CLOUD_CONFIG_API_ROOT_ENV_VAR}`).",
    )
