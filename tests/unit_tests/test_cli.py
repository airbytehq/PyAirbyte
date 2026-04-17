# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for the PyAirbyte CLI.

These tests exercise the Cyclopts-based `pyab` CLI to confirm that `--help`
remains invocable for the root app and every subcommand after the migration
from Click to Cyclopts.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from airbyte.cli.pyab import cli


@pytest.mark.parametrize(
    "subcommand",
    [
        None,
        "benchmark",
        "validate",
        "sync",
        "destination-smoke-test",
    ],
)
def test_cli_help_subprocess(subcommand: str | None) -> None:
    """Invoking `pyab [subcommand] --help` prints non-empty help and exits 0."""
    args = [sys.executable, "-m", "airbyte.cli.pyab"]
    if subcommand is not None:
        args.append(subcommand)
    args.append("--help")

    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"pyab {subcommand or ''} --help exited with {result.returncode}: "
        f"{result.stderr}"
    )
    assert result.stdout.strip(), "Help output should not be empty"


def test_cli_is_cyclopts_app() -> None:
    """The `cli` export is a `cyclopts.App` instance (not a Click group)."""
    from cyclopts import App

    assert isinstance(cli, App)


def test_cli_registers_expected_commands() -> None:
    """All four existing subcommands are registered on the Cyclopts App."""
    expected = {"benchmark", "validate", "sync", "destination-smoke-test"}
    registered = set(cli)
    missing = expected - registered
    assert not missing, f"Missing commands: {missing}"


def test_benchmark_help_includes_key_flags() -> None:
    """`pyab benchmark --help` surfaces all the previous Click options."""
    result = subprocess.run(
        [sys.executable, "-m", "airbyte.cli.pyab", "benchmark", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    for flag in ("--source", "--streams", "--num-records", "--destination", "--config"):
        assert flag in result.stdout, f"Expected {flag} in benchmark help output"


def test_validate_help_includes_cli_guidance() -> None:
    """`pyab validate --help` continues to include the PyAirbyte CLI guidance."""
    result = subprocess.run(
        [sys.executable, "-m", "airbyte.cli.pyab", "validate", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "PyAirbyte CLI Guidance" in result.stdout


def test_sync_help_includes_mixed_case_config_flags() -> None:
    """The unusual `--Sconfig` / `--Dconfig` / `--Spip-url` / `--Dpip-url` flags
    are preserved on the `sync` subcommand for backward compatibility.
    """
    result = subprocess.run(
        [sys.executable, "-m", "airbyte.cli.pyab", "sync", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    for flag in ("--Sconfig", "--Dconfig", "--Spip-url", "--Dpip-url"):
        assert flag in result.stdout, f"Expected {flag} in sync help output"
