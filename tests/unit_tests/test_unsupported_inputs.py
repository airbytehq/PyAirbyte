# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for the removed-env-var startup guard and the `pyab` CLI."""

from __future__ import annotations

import pytest

from airbyte._util.unsupported_inputs import check_unsupported_env_vars
from airbyte.cli import pyab
from airbyte.constants import (
    CLOUD_MCP_SAFE_MODE_ENV_VAR,
    MCP_DISABLE_DELETE_GUARD_ENV_VAR,
)
from airbyte.exceptions import AirbyteLibInputError


def test_check_unsupported_env_vars_passes_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the startup check passes when no listed env var is set."""
    monkeypatch.delenv(CLOUD_MCP_SAFE_MODE_ENV_VAR, raising=False)
    check_unsupported_env_vars()


@pytest.mark.parametrize("value", ["1", "0", ""])
def test_check_unsupported_env_vars_raises_when_set(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    """Test that any value of a listed env var fails startup with mapped guidance."""
    monkeypatch.setenv(CLOUD_MCP_SAFE_MODE_ENV_VAR, value)
    with pytest.raises(AirbyteLibInputError) as exc_info:
        check_unsupported_env_vars()
    assert CLOUD_MCP_SAFE_MODE_ENV_VAR in str(exc_info.value)
    guidance = exc_info.value.guidance
    assert isinstance(guidance, str)
    assert guidance.startswith(
        "Remove `AIRBYTE_CLOUD_MCP_SAFE_MODE` and consider using one of these alternatives:\n"
    )
    assert MCP_DISABLE_DELETE_GUARD_ENV_VAR in guidance


def test_check_unsupported_env_vars_is_generic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that the env-var guard works for any name added to the map."""
    monkeypatch.setattr(
        "airbyte._util.unsupported_inputs.UNSUPPORTED_ENV_VARS",
        {"FAKE_REMOVED_VAR": "use X"},
    )
    monkeypatch.setenv("FAKE_REMOVED_VAR", "1")
    with pytest.raises(AirbyteLibInputError) as exc_info:
        check_unsupported_env_vars()
    assert "FAKE_REMOVED_VAR" in str(exc_info.value)
    assert exc_info.value.guidance == "use X"


def test_pyab_cli_fails_on_unsupported_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that every `pyab` CLI command runs the env-var guard first."""
    monkeypatch.setenv(CLOUD_MCP_SAFE_MODE_ENV_VAR, "1")
    with pytest.raises(AirbyteLibInputError):
        pyab.cli(["benchmark"], exit_on_error=False)
