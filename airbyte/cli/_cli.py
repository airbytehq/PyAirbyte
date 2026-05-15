# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal Click implementation for the `airbyte` command."""

from __future__ import annotations

import json
import sys

import click

from airbyte.cli.cloud._cli import cloud
from airbyte.cli.local._cli import local
from airbyte.exceptions import PyAirbyteInputError


def _error_json(message: str, **extra: object) -> None:
    """Print an error object to stderr and exit."""
    payload: dict[str, object] = {"error": message, **extra}
    click.echo(json.dumps(payload, indent=2, default=str), err=True)
    sys.exit(1)


@click.group(name="airbyte")
def cli() -> None:
    """Airbyte CLI."""
    pass


cli.add_command(cloud)
cli.add_command(local)


def main() -> None:
    """Entry point for the `airbyte` command."""
    try:
        cli(standalone_mode=False)
    except SystemExit:
        raise
    except (KeyboardInterrupt, click.Abort):
        _error_json("Operation cancelled.")
    except click.ClickException as exc:
        _error_json(exc.format_message(), type=exc.__class__.__name__)
    except json.JSONDecodeError as exc:
        _error_json(str(exc), type="JSONDecodeError")
    except PyAirbyteInputError as exc:
        _error_json(str(exc), type="PyAirbyteInputError")


if __name__ == "__main__":
    main()
