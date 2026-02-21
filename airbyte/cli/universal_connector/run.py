# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Entry point for the PyAirbyte Universal destination."""

import sys

from airbyte.cli.universal_connector import DestinationPyAirbyteUniversal


def run() -> None:
    """Run the destination."""
    DestinationPyAirbyteUniversal().run(sys.argv[1:])


if __name__ == "__main__":
    run()
