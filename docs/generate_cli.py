# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Generate Markdown CLI references for the `airbyte` CLI.

This mirrors the `airbyte-ops-mcp` CLI docs generation pattern: command
metadata is rendered to Markdown first, then pdoc grafts that Markdown into
the `airbyte.cli` API pages via `.. include::` directives.

Usage:
    uv run python -m docs.generate_cli [OUTPUT_PATH]

Or as part of the combined docs pipeline:
    poe docs-generate
"""

from __future__ import annotations

import pathlib
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import click

from airbyte.cli.cloud._cli import cloud as cloud_reference_root  # noqa: PLC2701
from airbyte.cli.local._cli import local as local_reference_root  # noqa: PLC2701


if TYPE_CHECKING:
    from collections.abc import Iterable


DEFAULT_CLOUD_OUTPUT_PATH = pathlib.Path("docs/generated/cli/cloud-reference.md")
DEFAULT_CLOUD_SUBMODULE_OUTPUT_DIR = pathlib.Path("docs/generated/cli/cloud")
DEFAULT_LOCAL_OUTPUT_PATH = pathlib.Path("docs/generated/cli/local-reference.md")
DEFAULT_LOCAL_SUBMODULE_OUTPUT_DIR = pathlib.Path("docs/generated/cli/local")


@dataclass(frozen=True)
class CommandDoc:
    """Documentation metadata for a Click command."""

    name: str
    command: click.Command
    children: tuple[CommandDoc, ...] = field(default_factory=tuple)


def _command_docs(command: click.Command, name: str, ctx: click.Context) -> CommandDoc:
    children: list[CommandDoc] = []
    if isinstance(command, click.Group):
        for child_name in command.list_commands(ctx):
            child = command.get_command(ctx, child_name)
            if child is None:
                continue
            child_ctx = click.Context(child, info_name=child_name, parent=ctx)
            children.append(_command_docs(child, child_name, child_ctx))
    return CommandDoc(name=name, command=command, children=tuple(children))


def _usage(command: click.Command, command_chain: Iterable[str]) -> str:
    chain = list(command_chain)
    ctx = click.Context(command, info_name=chain[-1])
    pieces = " ".join(command.collect_usage_pieces(ctx))
    return f"{' '.join(chain)} {pieces}".strip()


def _format_option(param: click.Option) -> str:
    opts = ", ".join(param.opts)
    secondary_opts = ", ".join(param.secondary_opts)
    names = ", ".join(part for part in (opts, secondary_opts) if part)
    help_text = param.help or ""
    default = ""
    if param.default not in (None, "", (), []) and not callable(param.default):
        default = f" Default: `{param.default}`."
    required = " Required." if param.required else ""
    return f"- `{names}` — {help_text}{required}{default}".rstrip()


def _render_command(
    doc: CommandDoc, chain: list[str], lines: list[str], heading_level: int
) -> None:
    heading = "#" * heading_level
    command_name = " ".join(chain)
    help_text = doc.command.help or doc.command.short_help or ""

    lines.extend((f"{heading} {command_name}", ""))
    if help_text:
        lines.extend((help_text, ""))

    lines.extend(
        (
            "**Usage**",
            "",
            "```console",
            _usage(doc.command, chain),
            "```",
            "",
        )
    )

    options = [
        _format_option(param) for param in doc.command.params if isinstance(param, click.Option)
    ]
    if options:
        lines.extend(("**Options**", ""))
        lines.extend(options)
        lines.append("")

    if doc.children:
        lines.extend(("**Commands**", ""))
        for child in doc.children:
            short_help = child.command.short_help or child.command.help or ""
            lines.append(f"- `{child.name}` — {short_help}".rstrip())
        lines.append("")
        for child in doc.children:
            _render_command(child, [*chain, child.name], lines, heading_level + 1)


def _render_markdown(
    root_doc: CommandDoc,
    *,
    command_chain: list[str],
    heading_level: int,
) -> str:
    lines: list[str] = []
    _render_command(root_doc, command_chain, lines, heading_level)
    return "\n".join(lines).rstrip() + "\n"


def generate_cli_reference(
    output_path: pathlib.Path = DEFAULT_CLOUD_OUTPUT_PATH,
    *,
    heading_level: int = 1,
    cli_reference_root: click.Command = cloud_reference_root,
    command_chain: list[str] | None = None,
) -> pathlib.Path:
    """Render a combined CLI reference as Markdown."""
    command_chain = command_chain or ["airbyte", "cloud"]
    ctx = click.Context(cli_reference_root, info_name=command_chain[-1])
    markdown = _render_markdown(
        _command_docs(cli_reference_root, command_chain[-1], ctx),
        command_chain=command_chain,
        heading_level=heading_level,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown)
    return output_path


def generate_cli_submodule_references(
    output_dir: pathlib.Path | None = None,
    *,
    heading_level: int = 2,
    root_command: str = "airbyte cloud",
    cli_reference_root: click.Command = cloud_reference_root,
) -> list[pathlib.Path]:
    """Render one Markdown file per CLI command group."""
    output_dir = output_dir or DEFAULT_CLOUD_SUBMODULE_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    root_command_chain = root_command.split()
    root_ctx = click.Context(cli_reference_root, info_name=root_command_chain[-1])
    written: list[pathlib.Path] = []
    for name in cli_reference_root.list_commands(root_ctx):
        command = cli_reference_root.get_command(root_ctx, name)
        if command is None:
            continue
        command_ctx = click.Context(command, info_name=name, parent=root_ctx)
        command_chain = [*root_command_chain, name]
        markdown = _render_markdown(
            _command_docs(command, name, command_ctx),
            command_chain=command_chain,
            heading_level=heading_level,
        )
        output_path = output_dir / f"{name}.md"
        output_path.write_text(markdown)
        written.append(output_path)
    return written


def generate_local_cli_reference(
    output_path: pathlib.Path = DEFAULT_LOCAL_OUTPUT_PATH,
    *,
    heading_level: int = 1,
) -> pathlib.Path:
    """Render the combined `airbyte local` CLI reference as Markdown."""
    return generate_cli_reference(
        output_path,
        heading_level=heading_level,
        cli_reference_root=local_reference_root,
        command_chain=["airbyte", "local"],
    )


def generate_local_cli_submodule_references(
    output_dir: pathlib.Path | None = None,
    *,
    heading_level: int = 2,
) -> list[pathlib.Path]:
    """Render one Markdown file per `airbyte local` command group."""
    return generate_cli_submodule_references(
        output_dir or DEFAULT_LOCAL_SUBMODULE_OUTPUT_DIR,
        heading_level=heading_level,
        root_command="airbyte local",
        cli_reference_root=local_reference_root,
    )


def _main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    output = pathlib.Path(args[0]) if args else DEFAULT_CLOUD_OUTPUT_PATH
    combined_path = generate_cli_reference(output)
    print(f"Wrote combined CLI reference to {combined_path}")
    submodule_dir = output.parent / "cloud"
    for path in generate_cli_submodule_references(submodule_dir):
        print(f"Wrote CLI group reference to {path}")

    local_output = output.parent / "local-reference.md"
    local_combined_path = generate_local_cli_reference(local_output)
    print(f"Wrote combined CLI reference to {local_combined_path}")
    local_submodule_dir = output.parent / "local"
    for path in generate_local_cli_submodule_references(local_submodule_dir):
        print(f"Wrote CLI group reference to {path}")


if __name__ == "__main__":
    _main()
