# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Typed runtime settings for PyAirbyte.

PyAirbyte reads these settings once when `airbyte.constants` is imported.
Optional `airbyte.yaml` and `airbyte.toml` files in the current working
directory may provide values for the same settings. Environment variables take
precedence over file values, and file values take precedence over defaults.

This module intentionally contains only non-secret runtime settings. Cloud
credentials, connector credentials, and other secret-shaped values continue to
resolve through `airbyte.secrets`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

from pydantic import AliasChoices, BeforeValidator, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
    YamlConfigSettingsSource,
)


def _str_to_bool(value: object) -> bool:
    """Convert a value using PyAirbyte's legacy truthiness rules."""
    if isinstance(value, bool):
        return value
    return bool(value) and str(value).lower() not in {"", "0", "false", "f", "no", "n", "off"}


def _empty_path_to_none(value: object) -> object:
    """Treat unset and empty path values as absent."""
    if not value:
        return None
    return value


def _parse_no_uv(value: object) -> bool:
    """Preserve the inverted legacy AIRBYTE_NO_UV behavior."""
    if isinstance(value, bool):
        return value
    return str(value).lower() not in {"1", "true", "yes"}


BoolSetting = Annotated[bool, BeforeValidator(_str_to_bool)]
OptionalPathSetting = Annotated[Path | None, BeforeValidator(_empty_path_to_none)]
NoUvSetting = Annotated[bool, BeforeValidator(_parse_no_uv)]


class Settings(BaseSettings):
    """Non-secret runtime settings loaded from environment or local config files."""

    project_dir: OptionalPathSetting = None
    install_dir: OptionalPathSetting = None
    cache_root: OptionalPathSetting = None
    temp_dir: OptionalPathSetting = None
    temp_file_cleanup: BoolSetting = True
    offline_mode: BoolSetting = False
    print_full_error_logs: BoolSetting = Field(
        default_factory=lambda: _str_to_bool(os.getenv("CI", "false")),
        validation_alias=AliasChoices("AIRBYTE_PRINT_FULL_ERROR_LOGS", "print_full_error_logs"),
    )
    no_uv: NoUvSetting = True

    model_config = SettingsConfigDict(
        env_prefix="AIRBYTE_",
        yaml_file=("airbyte.yaml",),
        toml_file=("airbyte.toml",),
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Load local config files below environment and dotenv sources."""
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            TomlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )
