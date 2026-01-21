# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import subprocess

import pytest


@pytest.mark.linting
def test_mypy_typing():
    # Run the pyrefly check command
    check_result = subprocess.run(
        ["uv", "run", "pyrefly", "check"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Assert that the Pyrefly command exited without errors (exit code 0)
    assert check_result.returncode == 0, (
        "Pyrefly checks failed:\n"
        + f"{check_result.stdout.decode()}\n{check_result.stderr.decode()}\n\n"
        + "Run `uv run pyrefly check` to see all failures."
    )
