# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import subprocess

import pytest


@pytest.mark.linting
def test_type_checking():
    """Run ty type checker and assert it passes."""
    check_result = subprocess.run(
        ["uv", "run", "ty", "check"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert check_result.returncode == 0, (
        "ty type checks failed:\n"
        + f"{check_result.stdout.decode()}\n{check_result.stderr.decode()}\n\n"
        + "Run `uv run ty check` to see all failures."
    )
