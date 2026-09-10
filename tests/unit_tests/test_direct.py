# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the `airbyte.direct` module and `as_direct_connector` bridges."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from airbyte.agents.models import AgentConnectorDetails
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.connectors import CloudSource
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.direct import (
    AirbyteDirectConnectorNotSupportedError,
    DirectConnector,
    HostedDirectConnector,
)
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.secrets.base import SecretString
from airbyte.sources.base import Source


INSPECT_RESPONSE: dict[str, Any] = {
    "connector_id": "src-1",
    "name": "GitHub",
    "workspace_id": "ws",
    "source_definition_name": "GitHub",
    "docs_skill_id": "connector:github",
    "context_store_readiness": {
        "supported_context_store_entities": [
            {"entity": "issues", "suggested": True},
            {"entity": "repositories", "suggested": False},
        ]
    },
}


class _FakeResponse:
    """Minimal stand-in for a `requests.Response`."""

    def __init__(
        self,
        payload: Any,
        status_code: int = 200,
        content_type: str = "application/json",
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = ""
        self.headers = {"Content-Type": content_type}

    def json(self) -> Any:
        """Return the canned payload."""
        return self._payload


def _credentials(**overrides: Any) -> _AirbyteCredentials:
    """Build bearer-token credentials for tests."""
    kwargs: dict[str, Any] = {
        "client_id": None,
        "client_secret": None,
        "bearer_token": SecretString("test-token"),
        "public_api_root": "https://api.airbyte.com/v1",
        "config_api_root": None,
        "workspace_id": "ws",
        "organization_id": None,
    }
    kwargs.update(overrides)
    return _AirbyteCredentials(**kwargs)


@pytest.fixture
def captured_requests(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture Agents API requests, returning the canned inspect response."""
    calls: list[dict[str, Any]] = []

    def _fake_request(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return _FakeResponse(INSPECT_RESPONSE)

    monkeypatch.setattr(requests, "request", _fake_request)
    # Client credentials are exchanged for a bearer token via `requests.post`.
    monkeypatch.setattr(
        requests, "post", lambda **_: _FakeResponse({"access_token": "test-token"})
    )
    return calls


def _cloud_source(**workspace_kwargs: Any) -> CloudSource:
    """Build a `CloudSource` wired to test credentials."""
    kwargs: dict[str, Any] = {
        "workspace_id": "ws",
        "client_id": "id",
        "client_secret": "secret",
    }
    kwargs.update(workspace_kwargs)
    return CloudSource(CloudWorkspace(**kwargs), "src-1")


def test_hosted_direct_connector_satisfies_protocol() -> None:
    """`HostedDirectConnector` is a `DirectConnector`."""
    connector = HostedDirectConnector("cid", credentials=_credentials())
    assert isinstance(connector, DirectConnector)


def test_cloud_source_as_direct_connector(
    captured_requests: list[dict[str, Any]],
) -> None:
    """`as_direct_connector()` returns a hosted connector verified via `inspect()`."""
    connector = _cloud_source().as_direct_connector()

    assert isinstance(connector, HostedDirectConnector)
    assert connector.connector_id == "src-1"
    assert len(captured_requests) == 1
    assert captured_requests[0]["url"].endswith("/connectors/src-1/inspect")
    assert "X-Organization-Id" not in captured_requests[0]["headers"]


def test_cloud_source_as_direct_connector_no_verify(
    captured_requests: list[dict[str, Any]],
) -> None:
    """`verify=False` skips the `inspect()` request."""
    connector = _cloud_source().as_direct_connector(verify=False)

    assert isinstance(connector, HostedDirectConnector)
    assert captured_requests == []


@pytest.mark.parametrize(
    ("status_code", "expected_error"),
    [
        pytest.param(404, AirbyteDirectConnectorNotSupportedError, id="not_found"),
        pytest.param(403, AirbyteDirectConnectorNotSupportedError, id="forbidden"),
        pytest.param(500, AirbyteError, id="server_error_propagates"),
    ],
)
def test_cloud_source_as_direct_connector_not_supported(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    expected_error: type[AirbyteError],
) -> None:
    """A 404/403 on `inspect()` maps to `AirbyteDirectConnectorNotSupportedError`."""
    monkeypatch.setattr(
        requests,
        "request",
        lambda **_: _FakeResponse({"message": "nope"}, status_code=status_code),
    )
    monkeypatch.setattr(
        requests, "post", lambda **_: _FakeResponse({"access_token": "test-token"})
    )

    with pytest.raises(expected_error) as error_info:
        _cloud_source().as_direct_connector()

    error = error_info.value
    if isinstance(error, AirbyteDirectConnectorNotSupportedError):
        assert error.connector_id == "src-1"
        assert not isinstance(error, PyAirbyteInputError)
    else:
        assert not isinstance(error, AirbyteDirectConnectorNotSupportedError)


def test_cloud_source_as_direct_connector_rejects_non_public_roots(
    captured_requests: list[dict[str, Any]],
) -> None:
    """A workspace with custom API roots cannot produce a direct connector."""
    source = _cloud_source(api_root="https://example.com/api")

    with pytest.raises(PyAirbyteInputError, match="only available on Airbyte Cloud"):
        source.as_direct_connector()

    assert captured_requests == []


def test_local_source_as_direct_connector_raises() -> None:
    """A local `Source` always raises `AirbyteDirectConnectorNotSupportedError`."""
    with patch.object(Source, "_discover", return_value=Mock()):
        source = Source(executor=Mock(), name="source-test")

    with pytest.raises(
        AirbyteDirectConnectorNotSupportedError, match="direct entity actions"
    ) as error_info:
        source.as_direct_connector()

    assert error_info.value.connector_name == "source-test"


def test_hosted_direct_connector_inspect(
    captured_requests: list[dict[str, Any]],
) -> None:
    """The hosted connector's `inspect()` returns the Agents API payload."""
    details: AgentConnectorDetails = _cloud_source().as_direct_connector().inspect()
    assert details.name == "GitHub"
