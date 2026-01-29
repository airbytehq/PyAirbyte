# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Entry point for the PyAirbyte Universal source."""

import sys

from airbyte_cdk.entrypoint import launch

from airbyte.cli.universal_connector import SourcePyAirbyteUniversal


def run() -> None:
    """Run the source."""
    launch(SourcePyAirbyteUniversal(), sys.argv[1:])


if __name__ == "__main__":
    run()
