# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base Cyclopts app for the `airbyte` CLI."""

from __future__ import annotations

from cyclopts import App, Parameter

from airbyte.version import get_version


def _create_app(*, name: str, help_text: str) -> App:
    """Create a consistently configured CLI app."""
    return App(
        name=name,  # pyrefly: ignore[unexpected-keyword]
        help=help_text,  # pyrefly: ignore[unexpected-keyword]
        version=get_version(),  # pyrefly: ignore[unexpected-keyword]
        default_parameter=Parameter(  # pyrefly: ignore[unexpected-keyword]
            negative=(),  # pyrefly: ignore[unexpected-keyword]
        ),
    )


app = _create_app(
    name="airbyte",
    help_text="Airbyte CLI for Cloud operations and local connector workflows.",
)


__all__ = ["_create_app", "app"]
