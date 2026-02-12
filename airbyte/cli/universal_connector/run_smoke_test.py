# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Entry point for the Smoke Test source."""

import sys

from airbyte_cdk.entrypoint import launch

from airbyte.cli.universal_connector.smoke_test_source import SourceSmokeTest


def run() -> None:
    """Run the smoke test source."""
    launch(SourceSmokeTest(), sys.argv[1:])


if __name__ == "__main__":
    run()
