# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for deletion guard errors."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from airbyte._util import api_util
from airbyte.constants import CLOUD_API_ROOT
from airbyte.exceptions import AirbyteDeletionGuardError, AirbyteLibInputError
from airbyte.secrets.base import SecretString


@pytest.fixture
def no_api_calls(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Fail if a deletion guard test attempts an API call."""
    api_call = Mock(side_effect=AssertionError("Unexpected API call"))
    monkeypatch.setattr(api_util, "get_airbyte_server_instance", api_call)
    return api_call


def test_deletion_guard_error_renders_resource_details() -> None:
    resource_url = "https://cloud.airbyte.com/workspaces/w1/source/s1"
    error = AirbyteDeletionGuardError(
        resource_type="source",
        resource_name="prod source",
        resource_url=resource_url,
    )

    assert isinstance(error, AirbyteLibInputError)
    assert isinstance(error, ValueError)
    rendered = str(error)
    assert "delete-me" in rendered
    assert "source" in rendered
    assert "prod source" in rendered
    assert resource_url in rendered
    assert "explicitly consented" in rendered


def test_delete_source_raises_deletion_guard_error_before_api_call(
    no_api_calls: Mock,
) -> None:
    with pytest.raises(AirbyteDeletionGuardError) as error:
        api_util.delete_source(
            source_id="s1",
            source_name="prod source",
            workspace_id="w1",
            api_root=CLOUD_API_ROOT,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("x"),
            delete_guard=True,
        )

    assert (
        error.value.resource_url == "https://cloud.airbyte.com/workspaces/w1/source/s1"
    )
    assert not no_api_calls.called


def test_delete_connection_raises_deletion_guard_error_before_api_call(
    no_api_calls: Mock,
) -> None:
    with pytest.raises(AirbyteDeletionGuardError) as error:
        api_util.delete_connection(
            connection_id="c1",
            connection_name="prod connection",
            api_root=CLOUD_API_ROOT,
            workspace_id="w1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("x"),
            delete_guard=True,
        )

    assert error.value.resource_url == (
        "https://cloud.airbyte.com/workspaces/w1/connections/c1"
    )
    assert not no_api_calls.called


def test_permanently_delete_workspace_raises_before_api_call(
    no_api_calls: Mock,
) -> None:
    with pytest.raises(AirbyteDeletionGuardError) as error:
        api_util.permanently_delete_workspace(
            workspace_id="w1",
            workspace_name="prod",
            api_root=CLOUD_API_ROOT,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("x"),
            delete_guard=True,
        )

    assert error.value.resource_url == "https://cloud.airbyte.com/workspaces/w1"
    assert not no_api_calls.called
