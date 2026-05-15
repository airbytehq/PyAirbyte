# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""JSON output and help helpers for `airbyte cloud` commands."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, NoReturn

import click
import yaml


COMMAND_SCHEMAS: dict[str, dict[str, Any]] = {}


def json_output(data: list[dict[str, object]] | dict[str, object]) -> None:
    """Print a JSON-serializable object to stdout."""
    click.echo(json.dumps(data, indent=2, default=str))


def error_json(message: str, **extra: object) -> NoReturn:
    """Print an error object to stderr and exit."""
    payload: dict[str, object] = {"error": message, **extra}
    click.echo(json.dumps(payload, indent=2, default=str), err=True)
    sys.exit(1)


def parse_json_option(raw: str | None, *, option_name: str) -> dict[str, Any]:
    """Parse a JSON option value into a dict."""
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        error_json(f"{option_name} value must be a JSON object (dict).")
    return parsed


def parse_config_options(
    config_json: str | None,
    config_file: str | None,
    *,
    config_type_key: str | None = None,
    config_type_value: str | None = None,
) -> dict[str, Any]:
    """Parse connector config from `--config-json` or `--config-file`."""
    if config_json and config_file:
        error_json("Provide only one of --config-json or --config-file.", type="InvalidParameter")

    config = parse_json_option(config_json, option_name="--config-json")
    if config_file:
        parsed: Any | None = None
        try:
            parsed = yaml.safe_load(Path(config_file).expanduser().read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            error_json(str(exc), type=exc.__class__.__name__)
        if parsed is None:
            config = {}
        elif isinstance(parsed, dict):
            config = parsed
        else:
            error_json("--config-file must contain a JSON or YAML object.", type="InvalidParameter")

    if config_type_key and config_type_value:
        existing = config.get(config_type_key)
        if existing and existing != config_type_value:
            error_json(
                f"{config_type_key} conflicts with the explicit connector type option.",
                type="InvalidParameter",
            )
        config[config_type_key] = config_type_value
    return config


def resolve_entity_id(
    positional_id: str | None,
    option_id: str | None,
    *,
    option_name: str,
) -> str:
    """Resolve an entity ID accepted as either positional arg or named option."""
    if positional_id and option_id and positional_id != option_id:
        error_json(
            f"{option_name} conflicts with the positional ID argument.",
            type="InvalidParameter",
        )
    resolved_id = positional_id or option_id
    if not resolved_id:
        error_json(f"{option_name} is required.", type="MissingParameter")
    assert resolved_id
    return resolved_id


def register_schema(
    func_name: str,
    description: str,
    required_params: dict[str, str] | None = None,
    optional_params: dict[str, str] | None = None,
) -> None:
    """Register JSON-help metadata for a command."""
    schema: dict[str, Any] = {"description": description}
    if required_params:
        schema["required_params"] = required_params
    if optional_params:
        schema["optional_params"] = optional_params
    COMMAND_SCHEMAS[func_name] = schema


def emit_json_help(ctx: click.Context) -> None:
    """If `--format json` is active, print JSON help and exit."""
    if not is_json_format(ctx):
        return

    cmd_name = ctx.info_name or ""
    parent = ctx.parent
    while parent and parent.info_name:
        cmd_name = f"{parent.info_name}_{cmd_name}"
        parent = parent.parent

    func = ctx.command.callback
    func_name = func.__name__ if func else cmd_name
    schema = COMMAND_SCHEMAS.get(func_name)
    if schema:
        json_output(schema)
    else:
        json_output({"description": ctx.command.help or cmd_name})
    ctx.exit(0)


def is_json_format(ctx: click.Context) -> bool:
    """Check if `--format json` was requested."""
    current: click.Context | None = ctx
    while current:
        fmt = current.params.get("output_format")
        if fmt:
            return fmt == "json"
        current = current.parent
    for i, arg in enumerate(sys.argv):
        if arg == "--format" and i + 1 < len(sys.argv) and sys.argv[i + 1].lower() == "json":
            return True
        if arg.lower() == "--format=json":
            return True
    return False


def consume_json_format_arg(ctx: click.Context, args: list[str]) -> list[str]:
    """Consume command-local `--format json` arguments before Click validates options."""
    if "--format=json" in args:
        ctx.params["output_format"] = "json"
        return [arg for arg in args if arg != "--format=json"]
    if "--format" not in args:
        return args
    idx = args.index("--format")
    if idx + 1 >= len(args) or args[idx + 1].lower() != "json":
        return args
    ctx.params["output_format"] = "json"
    return [*args[:idx], *args[idx + 2 :]]


class JsonHelpGroup(click.Group):
    """Click group that emits JSON help when `--format json --help` is used."""

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        cleaned_args = consume_json_format_arg(ctx, args)
        return super().parse_args(ctx, cleaned_args)

    def get_help(self, ctx: click.Context) -> str:
        if is_json_format(ctx):
            commands: dict[str, str] = {}
            for name in self.list_commands(ctx):
                cmd = self.get_command(ctx, name)
                if cmd:
                    commands[name] = cmd.get_short_help_str(limit=300)
            json_output({"description": self.help or "", "commands": commands})
            ctx.exit(0)
        return super().get_help(ctx)


class JsonHelpCommand(click.Command):
    """Click command that emits JSON help when `--format json --help` is used."""

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        cleaned_args = consume_json_format_arg(ctx, args)
        return super().parse_args(ctx, cleaned_args)

    def get_help(self, ctx: click.Context) -> str:
        emit_json_help(ctx)
        return super().get_help(ctx)
