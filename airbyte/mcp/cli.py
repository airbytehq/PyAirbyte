# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
r"""CLI for calling Airbyte MCP tools directly.

This module provides a command-line interface for invoking MCP tools without
running the full MCP server. It's designed to work with uvx for one-off invocations.

Example usage:
    uvx --from=airbyte airbyte-mcp-cli validate_connector_config \
        '{"connector_name": "source-faker"}'

    uvx --from=airbyte airbyte-mcp-cli validate_connector_config \
        --connector_name=source-faker

    uvx --from=airbyte airbyte-mcp-cli list_connectors \
        '{"keyword_filter": "faker"}' --connector_type_filter=source

    uvx --from=airbyte airbyte-mcp-cli --list-tools

    uvx --from=airbyte airbyte-mcp-cli validate_connector_config --help
"""

from __future__ import annotations

import inspect
import json
import sys
import traceback
from typing import Any, get_type_hints

import click

from airbyte._util.meta import set_mcp_mode
from airbyte.mcp._tool_utils import get_registered_tools
from airbyte.mcp._util import initialize_secrets


set_mcp_mode()
initialize_secrets()

from airbyte.mcp import cloud_ops, connector_registry, local_ops  # noqa: E402, F401


def _parse_value(value: str) -> Any:  # noqa: ANN401
    """Parse a string value into appropriate Python type.

    Attempts to parse as JSON first. If that fails, returns the string as-is.

    Args:
        value: The string value to parse

    Returns:
        Parsed value (could be dict, list, int, bool, str, etc.)
    """
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return value


def _get_tool_function(tool_name: str) -> tuple[Any, dict[str, Any]] | None:
    """Get the tool function and its annotations by name.

    Args:
        tool_name: Name of the tool to find

    Returns:
        Tuple of (function, annotations) or None if not found
    """
    all_tools = get_registered_tools()
    for func, tool_annotations in all_tools:
        if func.__name__ == tool_name:
            return func, tool_annotations
    return None


def _list_tools() -> None:
    """List all available MCP tools grouped by domain."""
    all_tools = get_registered_tools()

    tools_by_domain: dict[str, list[tuple[Any, dict[str, Any]]]] = {}
    for func, tool_annotations in all_tools:
        domain = tool_annotations.get("domain", "unknown")
        if domain not in tools_by_domain:
            tools_by_domain[domain] = []
        tools_by_domain[domain].append((func, tool_annotations))

    click.echo("Available MCP Tools:\n")

    for domain in sorted(tools_by_domain.keys()):
        click.echo(f"[{domain.upper()}]")
        for func, tool_annotations in sorted(tools_by_domain[domain], key=lambda x: x[0].__name__):
            tool_name = func.__name__
            doc = (func.__doc__ or "").strip().split("\n")[0]
            read_only = " [read-only]" if tool_annotations.get("readOnlyHint") else ""
            click.echo(f"  {tool_name}{read_only}")
            if doc:
                click.echo(f"    {doc}")
        click.echo()


def _show_tool_help(tool_name: str) -> None:
    """Show detailed help for a specific tool.

    Args:
        tool_name: Name of the tool to show help for
    """
    tool_info = _get_tool_function(tool_name)
    if not tool_info:
        click.echo(f"Error: Tool '{tool_name}' not found.", err=True)
        click.echo("\nUse --list-tools to see available tools.", err=True)
        sys.exit(1)

    func, annotations = tool_info

    click.echo(f"Tool: {tool_name}")
    click.echo(f"Domain: {annotations.get('domain', 'unknown')}")

    if func.__doc__:
        click.echo(f"\n{func.__doc__.strip()}")

    sig = inspect.signature(func)
    if sig.parameters:
        click.echo("\nParameters:")
        for param_name, param in sig.parameters.items():
            param_type = param.annotation if param.annotation != inspect.Parameter.empty else "Any"
            default = (
                f" (default: {param.default})" if param.default != inspect.Parameter.empty else ""
            )
            click.echo(f"  --{param_name}: {param_type}{default}")

    hints = []
    if annotations.get("readOnlyHint"):
        hints.append("read-only")
    if annotations.get("idempotentHint"):
        hints.append("idempotent")
    if annotations.get("destructiveHint"):
        hints.append("destructive")
    if hints:
        click.echo(f"\nProperties: {', '.join(hints)}")


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help=__doc__,
)
@click.argument("tool_name", required=False)
@click.argument("json_input", required=False)
@click.option(
    "--list-tools",
    is_flag=True,
    help="List all available MCP tools",
)
@click.option(
    "--help-tool",
    is_flag=True,
    help="Show detailed help for the specified tool",
)
@click.pass_context
def main(  # noqa: PLR0912, PLR0915
    ctx: click.Context,
    tool_name: str | None,
    json_input: str | None,
    list_tools: bool,  # noqa: FBT001
    help_tool: bool,  # noqa: FBT001
) -> None:
    """CLI for calling Airbyte MCP tools directly.

    TOOL_NAME: Name of the MCP tool to call

    JSON_INPUT: Optional JSON string with tool arguments
    """
    if list_tools:
        _list_tools()
        sys.exit(0)

    if not tool_name:
        click.echo("Error: TOOL_NAME is required (or use --list-tools)", err=True)
        click.echo(ctx.get_help())
        sys.exit(1)

    if help_tool:
        _show_tool_help(tool_name)
        sys.exit(0)

    tool_info = _get_tool_function(tool_name)
    if not tool_info:
        click.echo(f"Error: Tool '{tool_name}' not found.", err=True)
        click.echo("\nUse --list-tools to see available tools.", err=True)
        sys.exit(1)

    func, _tool_annotations = tool_info

    args: dict[str, Any] = {}

    if json_input and json_input.startswith("--"):
        ctx.args = [json_input, *ctx.args]
        json_input = None

    if json_input:
        try:
            args = json.loads(json_input)
            if not isinstance(args, dict):
                click.echo(
                    f"Error: JSON input must be an object/dict, got {type(args).__name__}",
                    err=True,
                )
                sys.exit(1)
        except json.JSONDecodeError as e:
            click.echo(f"Error: Invalid JSON input: {e}", err=True)
            sys.exit(1)

    for arg in ctx.args:
        if arg.startswith("--"):
            if "=" in arg:
                key, value = arg[2:].split("=", 1)
                args[key] = _parse_value(value)
            else:
                args[arg[2:]] = True

    sig = inspect.signature(func)
    hints = get_type_hints(func, include_extras=True)

    final_args: dict[str, Any] = {}
    for param_name in sig.parameters:
        if param_name in args:
            final_args[param_name] = args[param_name]
        else:
            param_hint = hints.get(param_name)
            if param_hint and hasattr(param_hint, "__metadata__"):
                for metadata in param_hint.__metadata__:
                    if hasattr(metadata, "default"):
                        final_args[param_name] = metadata.default
                        break

    try:
        result = func(**final_args)

        if result is not None:
            if isinstance(result, (dict, list, tuple)):
                click.echo(json.dumps(result, indent=2, default=str))
            else:
                click.echo(result)

    except TypeError as e:
        click.echo(f"Error calling tool '{tool_name}': {e}", err=True)
        click.echo(f"\nUse: airbyte-mcp-cli {tool_name} --help-tool", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error executing tool '{tool_name}': {e}", err=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
