# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for the PyAirbyte CLI.

These tests exercise the Cyclopts-based `pyab` CLI to confirm that `--help`
remains invocable for the root app and every subcommand after the migration
from Click to Cyclopts.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest
from cyclopts import App

from airbyte.cli.pyab import cli


def _capture_help(tokens: list[str] | None = None) -> str:
    """Render help for `cli` (or a subcommand) into a string."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        cli.help_print(tokens=tokens or [])
    return buf.getvalue()


def test_cli_is_cyclopts_app() -> None:
    """The `cli` export is a `cyclopts.App` instance (not a Click group)."""
    assert isinstance(cli, App)


def test_cli_registers_expected_commands() -> None:
    """Exactly the four existing subcommands are registered on the Cyclopts App.

    Cyclopts exposes meta options (e.g. `--help`, `-h`) alongside user commands
    when iterating the app, so we filter those out before comparing. Asserting
    equality (not just "no missing") ensures an accidental new command would
    also fail this test.
    """
    expected = {"benchmark", "validate", "sync", "destination-smoke-test"}
    meta = {"--help", "-h", "--version"}
    registered = {name for name in cli if name not in meta}
    assert registered == expected, (
        f"Missing: {expected - registered}; unexpected: {registered - expected}"
    )


@pytest.mark.parametrize(
    "tokens",
    [
        [],
        ["benchmark"],
        ["validate"],
        ["sync"],
        ["destination-smoke-test"],
    ],
)
def test_cli_help_renders(tokens: list[str]) -> None:
    """Rendering help for the root app and every subcommand produces non-empty output."""
    output = _capture_help(tokens)
    assert output.strip(), f"Help output should not be empty for tokens={tokens}"


def test_benchmark_help_includes_key_flags() -> None:
    """`pyab benchmark --help` surfaces all the previous Click options."""
    output = _capture_help(["benchmark"])
    for flag in ("--source", "--streams", "--num-records", "--destination", "--config"):
        assert flag in output, f"Expected {flag} in benchmark help output"


def test_validate_help_includes_cli_guidance() -> None:
    """`pyab validate --help` continues to include the PyAirbyte CLI guidance."""
    output = _capture_help(["validate"])
    assert "PyAirbyte CLI Guidance" in output


def test_destination_smoke_test_has_no_auto_negated_flag() -> None:
    """Cyclopts normally auto-generates `--no-<flag>` for bool parameters.

    The `destination-smoke-test` command's `--skip-preflight` uses
    `Parameter(negative=[])` to match Click's `is_flag=True` behavior (only
    `--skip-preflight` is exposed). This test pins that down so a future
    cyclopts default change doesn't silently introduce `--no-skip-preflight`
    as a new user-facing flag.
    """
    output = _capture_help(["destination-smoke-test"])
    assert "--skip-preflight" in output
    assert "--no-skip-preflight" not in output


def test_sync_help_includes_mixed_case_config_flags() -> None:
    """The unusual `--Sconfig` / `--Dconfig` / `--Spip-url` / `--Dpip-url` flags
    are preserved on the `sync` subcommand for backward compatibility.
    """
    output = _capture_help(["sync"])
    for flag in ("--Sconfig", "--Dconfig", "--Spip-url", "--Dpip-url"):
        assert flag in output, f"Expected {flag} in sync help output"
