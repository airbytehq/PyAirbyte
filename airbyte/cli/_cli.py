# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Entrypoint for the `airbyte` CLI."""

from __future__ import annotations

import sys

from cyclopts.exceptions import CycloptsError

from airbyte.cli._base import app
from airbyte.exceptions import PyAirbyteError


def main() -> None:
    """Main entry point for the `airbyte` CLI."""
    try:
        app()
    except (CycloptsError, PyAirbyteError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc


__all__ = ["app", "main"]


if __name__ == "__main__":
    main()
