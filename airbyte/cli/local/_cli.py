# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cyclopts app for `airbyte local`."""

from __future__ import annotations

from airbyte.cli._base import _create_app, app


local_app = _create_app(name="local", help_text="Run local PyAirbyte connector workflows.")
app.command(local_app)


__all__ = ["local_app"]
