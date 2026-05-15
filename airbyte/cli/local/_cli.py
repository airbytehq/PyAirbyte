# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal Click implementation for `airbyte local` commands."""

from __future__ import annotations

import click

from airbyte.cli.pyab import (
    benchmark,
    destination_smoke_test,
    sync,
    validate,
)


@click.group()
def local() -> None:
    """Run local PyAirbyte connector workflows."""
    pass


@local.group()
def connectors() -> None:
    """Develop and inspect local connector behavior."""
    pass


connectors.add_command(validate)
connectors.add_command(benchmark)
local.add_command(sync)


@local.group()
def debug() -> None:
    """Debug local connector workflows."""
    pass


@debug.group()
def destinations() -> None:
    """Debug destination behavior with synthetic records."""
    pass


destinations.add_command(destination_smoke_test, name="smoke-test")
