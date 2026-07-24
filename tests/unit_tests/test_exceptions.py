# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import inspect
import airbyte.exceptions as exceptions_module
import pytest

from airbyte.exceptions import (
    AirbyteMissingWorkspaceContextError,
    AirbyteNoCloudCredentialsError,
)


def test_exceptions():
    exception_classes = [
        (name, obj)
        for name, obj in inspect.getmembers(exceptions_module)
        if inspect.isclass(obj) and name.endswith("Error")
    ]
    assert "AirbyteError" in [name for name, _ in exception_classes]
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
            "via the `X-Airbyte-Cloud-Client-Id` and `X-Airbyte-Cloud-Client-Secret` headers.",
            id="hosted_with_bearer",
        ),
        pytest.param(
            True,
            False,
            False,
            "Provide client credentials via the `X-Airbyte-Cloud-Client-Id` and "
            "`X-Airbyte-Cloud-Client-Secret` headers.",
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
    ("hosted", "expected_guidance"),
    [
        pytest.param(
            True,
            "Provide the workspace ID via the `X-Airbyte-Workspace-Id` header, or pass "
            "the `workspace_id` parameter.",
            id="hosted",
        ),
        pytest.param(
            False,
            "Set the `AIRBYTE_CLOUD_WORKSPACE_ID` environment variable, or pass the "
            "`workspace_id` parameter.",
            id="local",
        ),
    ],
)
def test_missing_workspace_context_error_guidance(
    hosted: bool,
    expected_guidance: str,
) -> None:
    """Render workspace guidance for each supported mode."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(exceptions_module, "is_hosted_mcp_mode", lambda: hosted)
        error = AirbyteMissingWorkspaceContextError()

    assert error.get_message() == "Workspace ID is required but not provided."
    assert error.guidance == expected_guidance
    assert "Allow Bearer" not in str(error)
    assert "Env Vars" not in str(error)


if __name__ == "__main__":
    pytest.main()
