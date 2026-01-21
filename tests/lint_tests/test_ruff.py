# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import subprocess

import pytest


@pytest.mark.linting
def test_ruff_linting():
    # Run the check command
    check_result = subprocess.run(
        ["uv", "run", "ruff", "check", "."],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Assert that the Ruff command exited without errors (exit code 0)
    assert check_result.returncode == 0, (
        "Ruff checks failed:\n\n"
        + f"{check_result.stdout.decode()}\n{check_result.stderr.decode()}\n\n"
        + "Run `uv run ruff check .` to view all issues."
    )


@pytest.mark.linting
def test_ruff_linting_fixable():
    # Run the check command
    fix_diff_result = subprocess.run(
        ["uv", "run", "ruff", "check", "--fix", "--diff", "."],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Assert that the Ruff command exited without errors (exit code 0)
    assert fix_diff_result.returncode == 0, (
        "Ruff checks revealed fixable issues:\n\n"
        + f"{fix_diff_result.stdout.decode()}\n{fix_diff_result.stderr.decode()}\n\n"
        + "Run `uv run ruff check --fix .` to attempt automatic fixes."
    )


@pytest.mark.linting
def test_ruff_format():
    # Define the command to run Ruff
    command = ["uv", "run", "ruff", "format", "--check", "--diff"]

    # Run the command
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Assert that the Ruff command exited without errors (exit code 0)
    assert result.returncode == 0, (
        f"Ruff checks failed:\n\n{result.stdout.decode()}\n{result.stderr.decode()}\n\n"
        + "Run `uv run ruff format .` to attempt automatic fixes."
    )
