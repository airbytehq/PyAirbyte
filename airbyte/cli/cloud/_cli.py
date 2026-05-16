# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cyclopts app for `airbyte cloud`."""

from __future__ import annotations

from airbyte.cli._base import _create_app, app


cloud_app = _create_app(
    name="cloud",
    help_text="Manage Airbyte Cloud workspaces, sources, destinations, connections, and jobs.",
)
app.command(cloud_app)


__all__ = ["cloud_app"]
