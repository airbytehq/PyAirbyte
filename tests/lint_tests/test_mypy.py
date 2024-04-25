# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import subprocess

import pytest


@pytest.mark.linting
def test_pyright_typing():
    # Run the check command
    check_result = subprocess.run(
        ["poetry", "run", "pyright", "."],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Assert that the Ruff command exited without errors (exit code 0)
    assert check_result.returncode == 0, (
        "Pyright typing checks failed:\n"
        + f"{check_result.stdout.decode()}\n{check_result.stderr.decode()}\n\n"
        + "Run `poetry run pyright .` to see all failures."
    )
