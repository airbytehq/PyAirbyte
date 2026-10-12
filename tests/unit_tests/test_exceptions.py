# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path

import pytest

import airbyte.exceptions as exceptions_module
from airbyte._util.destination_smoke_tests import (
    _extract_trace_error_from_log,
    _sanitize_error,
)
from airbyte.exceptions import (
    AirbyteCloudError,
    AirbyteLibError,
    AirbyteMissingWorkspaceContextError,
    AirbyteNoCloudCredentialsError,
)


def test_exceptions():
    exception_classes = [
        (name, obj)
        for name, obj in inspect.getmembers(exceptions_module)
        if inspect.isclass(obj) and name.endswith("Error")
    ]
    assert "AirbyteCloudError" in [name for name, _ in exception_classes]
    assert "NotAnError" not in [name for name, _ in exception_classes]
    for name, obj in exception_classes:
        instance = obj()
        message = instance.get_message()
        assert isinstance(message, str), "No message for class: " + name
        assert message.count("\n") == 0
        assert message != ""
        assert message.strip() == message
        assert any([name.startswith(prefix) for prefix in ["Airbyte", "PyAirbyte"]]), (
            f"{name} does not start with Airbyte or PyAirbyte"
        )
        assert name.endswith("Error")


@pytest.mark.parametrize(
    ("hosted", "allow_bearer", "env_vars", "expected_guidance"),
    [
        pytest.param(
            True,
            True,
            True,
            "Provide a bearer token via the `Authorization` header, or client credentials "
            "via the transport `Client-Id` and `Client-Secret` headers.",
            id="hosted_with_bearer",
        ),
        pytest.param(
            True,
            False,
            False,
            "Provide client credentials via the transport `Client-Id` and "
            "`Client-Secret` headers.",
            id="hosted_client_credentials_only",
        ),
        pytest.param(
            False,
            True,
            True,
            "Provide `bearer_token`, or both `client_id` and `client_secret`, as arguments "
            "or via the `AIRBYTE_CLOUD_BEARER_TOKEN`, `AIRBYTE_CLOUD_CLIENT_ID`, and "
            "`AIRBYTE_CLOUD_CLIENT_SECRET` environment variables.",
            id="local_with_bearer",
        ),
        pytest.param(
            False,
            False,
            True,
            "Provide both `client_id` and `client_secret`, as arguments or via the "
            "`AIRBYTE_CLOUD_CLIENT_ID` and `AIRBYTE_CLOUD_CLIENT_SECRET` environment variables.",
            id="local_client_credentials_only",
        ),
        pytest.param(
            False,
            True,
            False,
            "Provide `bearer_token`, or both `client_id` and `client_secret`.",
            id="local_without_env_vars",
        ),
        pytest.param(
            False,
            False,
            False,
            "Provide both `client_id` and `client_secret`.",
            id="local_client_credentials_only_without_env_vars",
        ),
    ],
)
def test_cloud_credentials_error_guidance(
    hosted: bool,
    allow_bearer: bool,
    env_vars: bool,
    expected_guidance: str,
) -> None:
    """Render cloud credential guidance for each supported mode."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(exceptions_module, "is_hosted_mcp_mode", lambda: hosted)
        error = AirbyteNoCloudCredentialsError(
            _allow_bearer=allow_bearer,
            _env_vars=env_vars,
        )

    assert error.get_message() == "No Airbyte credentials found."
    assert error.guidance == expected_guidance
    assert "Allow Bearer" not in str(error)
    assert "Env Vars" not in str(error)


@pytest.mark.parametrize(
    ("mcp_mode", "expected_guidance"),
    [
        pytest.param(
            True,
            "The authenticated user's default workspace was checked and none was "
            "available. `list_cloud_workspaces` returns direct workspace memberships "
            "by default; pass `organization_id`/`organization_name` or a broader "
            "`privilege_scope` for organization-wide discovery, or call "
            "`list_cloud_organizations` to search organizations by name. If exactly "
            "one workspace is found, use it; otherwise ask the user to choose. Call "
            "`get_default_cloud_context` to inspect your memberships.",
            id="mcp",
        ),
        pytest.param(
            False,
            "The authenticated user's default workspace was checked and none was "
            "available. `list_workspaces` returns direct workspace memberships "
            "by default; pass `organization_id`/`organization_name` or a broader "
            "`privilege_scope` for organization-wide discovery, or call "
            "`list_organizations` to search organizations by name. If exactly "
            "one workspace is found, use it; otherwise ask the user to choose. Call "
            "`get_default_context_for_user` to inspect your memberships.",
            id="python",
        ),
    ],
)
def test_missing_workspace_context_error_guidance(
    mcp_mode: bool,
    expected_guidance: str,
) -> None:
    """Render workspace guidance for each supported mode."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(exceptions_module, "is_mcp_mode", lambda: mcp_mode)
        error = AirbyteMissingWorkspaceContextError()

    assert error.get_message() == "Workspace ID is required but not provided."
    assert error.guidance == expected_guidance
    assert "Allow Bearer" not in str(error)
    assert "Env Vars" not in str(error)


def test_original_exception_reflects_raise_from_cause() -> None:
    inner = ValueError("inner")

    with pytest.raises(AirbyteCloudError) as exc_info:
        raise AirbyteCloudError(message="m") from inner

    error = exc_info.value
    assert error.original_exception is inner
    assert "Caused by:" not in str(error)


def test_original_exception_constructor_argument_sets_cause() -> None:
    inner = ValueError("inner")
    error = AirbyteCloudError(message="m", original_exception=inner)

    assert error.__cause__ is inner
    assert error.original_exception is inner
    assert "Caused by:" in str(error)
    assert "inner" in str(error)

    replacement = ValueError("replacement")
    with pytest.raises(AirbyteCloudError) as exc_info:
        raise error from replacement

    assert exc_info.value.original_exception is replacement
    assert "Caused by:" in str(exc_info.value)
    assert "replacement" in str(exc_info.value)


def test_original_exception_is_none_without_a_cause() -> None:
    error = AirbyteCloudError(message="m")

    assert error.original_exception is None
    assert "Caused by" not in str(error)


def test_external_dataclass_subclass_with_post_init_accepts_original_exception() -> (
    None
):
    @dataclass
    class ExternalAirbyteError(AirbyteLibError):
        post_init_ran: bool = False

        def __post_init__(self) -> None:
            self.post_init_ran = True

    without_cause = ExternalAirbyteError(message="m")
    assert without_cause.post_init_ran
    assert without_cause.original_exception is None

    inner = ValueError("inner")
    with_cause = ExternalAirbyteError(message="m", original_exception=inner)
    assert with_cause.post_init_ran
    assert with_cause.__cause__ is inner
    assert with_cause.original_exception is inner


def test_original_exception_constructor_argument_survives_subclass_post_init() -> None:
    inner = ValueError("inner")
    error = AirbyteNoCloudCredentialsError(
        guidance="guidance",
        original_exception=inner,
    )

    assert error.guidance == "guidance"
    assert error.__cause__ is inner
    assert error.original_exception is inner


def test_sanitize_error_does_not_walk_unopted_raise_from_cause() -> None:
    inner = ValueError("password=hunter2")

    with pytest.raises(AirbyteCloudError) as exc_info:
        raise AirbyteCloudError(message="outer") from inner

    error = exc_info.value
    assert "hunter2" not in _sanitize_error(error)
    assert "hunter2" not in str(error)


def test_sanitize_error_does_not_find_log_file_through_unopted_cause(
    tmp_path: Path,
) -> None:
    class ErrorWithLogFile(Exception):
        def __init__(self, log_file: Path) -> None:
            self.log_file = log_file

    log_file = tmp_path / "connector.log"
    log_file.write_text(
        '{"type":"TRACE","trace":{"error":{"internal_message":"cause detail"}}}',
        encoding="utf-8",
    )
    inner = ErrorWithLogFile(log_file)

    with pytest.raises(AirbyteCloudError) as exc_info:
        raise AirbyteCloudError(message="outer") from inner

    assert _extract_trace_error_from_log(exc_info.value) is None


def test_sanitize_error_walks_raise_from_chain() -> None:
    inner = AirbyteCloudError(message="inner")
    error = AirbyteCloudError(message="outer", original_exception=inner)
    assert _sanitize_error(error) == "AirbyteCloudError: inner"


if __name__ == "__main__":
    pytest.main()
