# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Input helpers for CLI commands."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import yaml

from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from pathlib import Path


def parse_config_options(
    *,
    config_json: str | None = None,
    config_file: Path | None = None,
) -> dict[str, object]:
    """Parse connector configuration from JSON text or a YAML/JSON file."""
    if bool(config_json) == bool(config_file):
        raise PyAirbyteInputError(
            message="Exactly one config input is required.",
            context={"options": "--config-json, --config-file"},
        )

    if config_json:
        parsed_json = json.loads(config_json)
        if not isinstance(parsed_json, dict):
            raise PyAirbyteInputError(message="Config JSON must be an object.")
        return parsed_json

    if not config_file:
        raise PyAirbyteInputError(message="Config file is required.")
    if not config_file.exists():
        raise PyAirbyteInputError(message="Config file does not exist.")
    parsed_file = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    if not isinstance(parsed_file, dict):
        raise PyAirbyteInputError(message="Config file must contain an object.")
    return parsed_file


def parse_csv(value: str | None) -> list[str]:
    """Parse a comma-separated CLI option value."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_entity_id(
    args: tuple[str, ...],
    option_value: str | None,
    *,
    option_name: str,
) -> str:
    """Resolve an entity ID from one positional argument or a named option."""
    if len(args) > 1:
        raise PyAirbyteInputError(message="Only one entity ID argument is allowed.")

    arg_value = args[0] if args else None
    if arg_value and option_value and arg_value != option_value:
        raise PyAirbyteInputError(message="Entity ID arguments must match.")

    entity_id = arg_value or option_value
    if not entity_id:
        raise PyAirbyteInputError(
            message="Entity ID is required.",
            context={"option": option_name},
        )

    return entity_id


__all__ = ["parse_config_options", "parse_csv", "resolve_entity_id"]
