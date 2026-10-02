# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Typed settings for PyAirbyte.

`AirbyteSettings` reads environment variables when instantiated, so a new instance observes
environment changes and dotenv files loaded after import. It does not cache settings or create
directories. Each field documents its corresponding environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from airbyte._util.text_util import _str_to_bool


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
