# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Allow running the CLI via python -m airbyte.cli."""

from airbyte.cli.pyab import cli


if __name__ == "__main__":
    cli()
