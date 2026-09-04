# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for asynchronous Cloud connector checks."""

from __future__ import annotations

import json
from typing import Any

import pytest
import responses

from airbyte.cloud.connectors import CheckResult
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteConnectorCheckTimeoutError


CONFIG_API_ROOT = "https://cloud.airbyte.com/api/v1"
CONNECTOR_ID = "connector-id"
COMMAND_ID = "command-id"


def _workspace() -> CloudWorkspace:
    return CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root="https://api.airbyte.com/v1",
        config_api_root=CONFIG_API_ROOT,
    )


def _register_check_endpoints(
    statuses: list[str],
    output: dict[str, Any] | None = None,
) -> None:
    responses.add(
        responses.POST,
        f"{CONFIG_API_ROOT}/commands/run/check",
        json={"id": COMMAND_ID},
    )
    status_index = 0

    def status_callback(request: Any) -> tuple[int, dict[str, str], str]:
        nonlocal status_index
        status = statuses[min(status_index, len(statuses) - 1)]
        status_index += 1
        return 200, {}, json.dumps({"id": COMMAND_ID, "status": status})

    responses.add_callback(
        responses.POST,
        f"{CONFIG_API_ROOT}/commands/status",
        callback=status_callback,
        content_type="application/json",
    )
    if output is not None:
        responses.add(
            responses.POST,
            f"{CONFIG_API_ROOT}/commands/output/check",
            json=output,
        )


@pytest.mark.parametrize(
    (
        "statuses",
        "output",
        "expected_success",
        "expected_message",
        "expected_failure_type",
        "wait",
    ),
    [
        pytest.param(
            ["pending", "completed"],
            {"id": COMMAND_ID, "status": "succeeded"},
            True,
            None,
            None,
            True,
            id="succeeded",
        ),
        pytest.param(
            ["pending", "completed"],
            {
                "id": COMMAND_ID,
                "status": "failed",
                "failureReason": {
                    "externalMessage": "Invalid credentials.",
                    "failureType": "config_error",
                },
            },
            False,
            "Invalid credentials.",
            "config_error",
            True,
            id="failed-with-failure-reason",
        ),
        pytest.param(
            ["completed"],
            {"id": COMMAND_ID, "status": "failed", "message": "Check failed."},
            False,
            "Check failed.",
            None,
            True,
            id="failed-with-message",
        ),
        pytest.param(
            ["cancelled"],
            None,
            False,
            "Check command was cancelled.",
            None,
            True,
            id="cancelled",
        ),
        pytest.param(
            ["pending"],
            None,
            False,
            None,
            None,
            False,
            id="without-wait",
        ),
    ],
)
@responses.activate
def test_check(
    monkeypatch: pytest.MonkeyPatch,
    statuses: list[str],
    output: dict[str, Any] | None,
    expected_success: bool,
    expected_message: str | None,
    expected_failure_type: str | None,
    wait: bool,
) -> None:
    """Verify Cloud connector checks map asynchronous command results."""
    monkeypatch.setattr("airbyte.cloud.connectors.time.sleep", lambda _: None)
    _register_check_endpoints(statuses, output)

    result = (
        _workspace()
        .get_source(CONNECTOR_ID)
        .check(
            raise_on_error=False,
            wait=wait,
        )
    )

    assert result.success is expected_success
    assert result.error_message == expected_message
    assert result.failure_type == expected_failure_type
    assert result.command_id == COMMAND_ID
    assert result.is_complete() is (wait and statuses[-1] in {"completed", "cancelled"})


@responses.activate
def test_check_raises_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify failed checks raise when requested."""
    monkeypatch.setattr("airbyte.cloud.connectors.time.sleep", lambda _: None)
    _register_check_endpoints(
        ["completed"],
        {"id": COMMAND_ID, "status": "failed", "message": "Check failed."},
    )

    with pytest.raises(ValueError, match="Check failed: Failed: Check failed."):
        _workspace().get_source(CONNECTOR_ID).check()


def test_check_result_str_uses_internal_error() -> None:
    """Verify check result strings include an internal failure message."""
    result = CheckResult(success=False, internal_error="Check service unavailable")

    assert str(result) == "Failed: Check service unavailable"


@responses.activate
def test_check_attaches_to_existing_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify checks can attach to a command without starting another."""
    monkeypatch.setattr("airbyte.cloud.connectors.time.sleep", lambda _: None)
    responses.add(
        responses.POST,
        f"{CONFIG_API_ROOT}/commands/status",
        json={"id": COMMAND_ID, "status": "completed"},
    )
    responses.add(
        responses.POST,
        f"{CONFIG_API_ROOT}/commands/output/check",
        json={"id": COMMAND_ID, "status": "succeeded"},
    )

    result = (
        _workspace()
        .get_source(CONNECTOR_ID)
        .check(
            command_id=COMMAND_ID,
            wait=True,
        )
    )

    assert result.success
    assert (
        len([
            call
            for call in responses.calls
            if "/commands/run/check" in call.request.url
        ])
        == 0
    )


@responses.activate
def test_check_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify checks raise a typed timeout error."""
    monkeypatch.setattr("airbyte.cloud.connectors.time.sleep", lambda _: None)
    _register_check_endpoints(["pending"])

    with pytest.raises(AirbyteConnectorCheckTimeoutError) as error:
        _workspace().get_source(CONNECTOR_ID).check(wait_timeout=0)

    assert error.value.connector_id == CONNECTOR_ID
    assert error.value.command_id == COMMAND_ID


@responses.activate
def test_cancel_skips_log_retrieval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancelled checks do not request logs."""
    _register_check_endpoints(["pending", "cancelled"])
    responses.add(
        responses.POST,
        f"{CONFIG_API_ROOT}/commands/cancel",
        json={"id": COMMAND_ID},
    )
    monkeypatch.setattr("airbyte.cloud.connectors.time.sleep", lambda _: None)
    result = (
        _workspace()
        .get_source(CONNECTOR_ID)
        .check(
            raise_on_error=False,
            wait=False,
        )
    )

    result.cancel()
    assert result.get_logs() == []
    assert any("/commands/cancel" in call.request.url for call in responses.calls)


@responses.activate
def test_get_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify check logs are retrieved from command output."""
    _register_check_endpoints(["completed"])
    responses.add(
        responses.POST,
        f"{CONFIG_API_ROOT}/commands/output/check",
        json={
            "id": COMMAND_ID,
            "status": "succeeded",
            "logs": {"logLines": ["line 1", "line 2"]},
        },
    )
    monkeypatch.setattr("airbyte.cloud.connectors.time.sleep", lambda _: None)
    result = _workspace().get_source(CONNECTOR_ID).check(wait=False)

    assert result.get_logs() == ["line 1", "line 2"]
