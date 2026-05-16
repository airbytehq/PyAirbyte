# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Generate Markdown CLI references for the `airbyte` CLI.

This mirrors the `airbyte-ops-mcp` CLI docs generation pattern: Cyclopts
command metadata is rendered to Markdown first, then pdoc grafts that Markdown
into the `airbyte.cli` API pages via `.. include::` directives.

Usage:
    uv run python -m docs.generate_cli [OUTPUT_PATH]

Or as part of the combined docs pipeline:
    poe docs-generate
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from cyclopts.docs import generate_markdown_docs


if TYPE_CHECKING:
    from cyclopts import App

from airbyte.cli._cli import app as airbyte_app  # noqa: PLC2701
from airbyte.cli.cloud import connections, destinations, jobs, sources, workspaces
from airbyte.cli.cloud._cli import cloud_app  # noqa: PLC2701
from airbyte.cli.local import connectors, debug
from airbyte.cli.local._cli import local_app  # noqa: PLC2701


DEFAULT_CLOUD_OUTPUT_PATH = Path("docs/generated/cli/cloud-reference.md")
DEFAULT_CLOUD_SUBMODULE_OUTPUT_DIR = Path("docs/generated/cli/cloud")
DEFAULT_LOCAL_OUTPUT_PATH = Path("docs/generated/cli/local-reference.md")
DEFAULT_LOCAL_SUBMODULE_OUTPUT_DIR = Path("docs/generated/cli/local")

_CLOUD_SUBMODULE_APPS: tuple[tuple[str, App], ...] = (
    ("workspaces", workspaces.workspaces_app),
    ("sources", sources.sources_app),
    ("destinations", destinations.destinations_app),
    ("connections", connections.connections_app),
    ("jobs", jobs.jobs_app),
)

_LOCAL_SUBMODULE_APPS: tuple[tuple[str, App], ...] = (
    ("connectors", connectors.connectors_app),
    ("sync", local_app),
    ("debug", debug.debug_app),
)


def _write_markdown(
    app: App,
    output_path: Path,
    *,
    heading_level: int,
    command_chain: list[str],
    exclude_commands: list[str] | None = None,
) -> Path:
    """Write Cyclopts Markdown docs for one CLI app."""
    markdown = generate_markdown_docs(
        app,
        include_hidden=False,
        heading_level=heading_level,
        command_chain=command_chain,
        exclude_commands=exclude_commands,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown)
    return output_path


def generate_cli_reference(
    output_path: Path = DEFAULT_CLOUD_OUTPUT_PATH,
    *,
    heading_level: int = 1,
) -> Path:
    """Render the combined `airbyte cloud` CLI reference as Markdown."""
    return _write_markdown(
        cloud_app,
        output_path,
        heading_level=heading_level,
        command_chain=["airbyte", "cloud"],
    )


def generate_cli_submodule_references(
    output_dir: Path | None = None,
    *,
    heading_level: int = 2,
) -> list[Path]:
    """Render one Markdown file per `airbyte cloud` command group."""
    output_dir = output_dir or DEFAULT_CLOUD_SUBMODULE_OUTPUT_DIR
    return [
        _write_markdown(
            submodule_app,
            output_dir / f"{name}.md",
            heading_level=heading_level,
            command_chain=["airbyte", "cloud", name],
        )
        for name, submodule_app in _CLOUD_SUBMODULE_APPS
    ]


def generate_local_cli_reference(
    output_path: Path = DEFAULT_LOCAL_OUTPUT_PATH,
    *,
    heading_level: int = 1,
) -> Path:
    """Render the combined `airbyte local` CLI reference as Markdown."""
    return _write_markdown(
        local_app,
        output_path,
        heading_level=heading_level,
        command_chain=["airbyte", "local"],
    )


def generate_local_cli_submodule_references(
    output_dir: Path | None = None,
    *,
    heading_level: int = 2,
) -> list[Path]:
    """Render one Markdown file per `airbyte local` command group."""
    output_dir = output_dir or DEFAULT_LOCAL_SUBMODULE_OUTPUT_DIR
    return [
        _write_markdown(
            submodule_app,
            output_dir / f"{name}.md",
            heading_level=heading_level,
            command_chain=["airbyte", "local", name],
            exclude_commands=["connectors", "debug"] if name == "sync" else None,
        )
        for name, submodule_app in _LOCAL_SUBMODULE_APPS
    ]


def _main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    output = Path(args[0]) if args else DEFAULT_CLOUD_OUTPUT_PATH
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


__all__ = [
    "airbyte_app",
    "generate_cli_reference",
    "generate_cli_submodule_references",
    "generate_local_cli_reference",
    "generate_local_cli_submodule_references",
]


if __name__ == "__main__":
    _main()
