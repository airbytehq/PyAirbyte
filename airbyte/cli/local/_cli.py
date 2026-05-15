# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal Click app logic for `airbyte local`."""

from __future__ import annotations

import click

from airbyte.cli.local import connectors, debug, sync


@click.group()
def local() -> None:
    """Run local PyAirbyte connector workflows."""
    pass


local.add_command(connectors.connectors)
local.add_command(debug.debug)
local.add_command(sync.sync)
