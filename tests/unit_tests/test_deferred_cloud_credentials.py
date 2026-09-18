# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for deferred-credential Cloud deployment and the setup completion check."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, cast

import pytest
import requests
from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte._util.deferred_setup import (
    DEFERRED_SETUP_PROBLEM_TYPE,
    parse_deferred_setup_problem,
)
from airbyte.cloud.connectors import CheckResult
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp.cloud import ConnectorSetupCheckResult, DeferredDeployResult
from airbyte.secrets.base import SecretString
from airbyte_api._hooks.types import BeforeRequestContext, HookContext  # noqa: PLC2701
from fastmcp import Context
from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701


WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
OTHER_WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"
DEFINITION_ID = "33333333-3333-4333-8333-333333333333"
ACTOR_ID = "44444444-4444-4444-8444-444444444444"
TOKEN = SecretString("test-bearer-token")

CONNECTOR_TYPES: list[Literal["source", "destination"]] = ["source", "destination"]


class _RecordingAdapter(requests.adapters.BaseAdapter):
    """Transport adapter that records every request and replays canned responses."""

    def __init__(
        self, responses: list[tuple[int, dict[str, Any], dict[str, str]]]
    ) -> None:
        super().__init__()
        self._responses = list(responses)
        self.requests: list[requests.PreparedRequest] = []
        self.send_kwargs: list[dict[str, Any]] = []

    def send(
        self, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:  # noqa: ANN401
        self.requests.append(request)
        self.send_kwargs.append(kwargs)
        status, body, headers = self._responses.pop(0)
        response = requests.Response()
        response.status_code = status
        response.headers.update({"Content-Type": "application/json", **headers})
        response._content = json.dumps(body).encode()  # noqa: SLF001
        response.url = request.url or ""
        response.request = request
        return response

    def close(self) -> None:
        pass


def _actor_body(connector_type: str, *, deferred: bool | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "configuration": {"count": 10},
        "createdAt": 1,
        "definitionId": DEFINITION_ID,
        "name": "My connector",
        f"{connector_type}Id": ACTOR_ID,
        f"{connector_type}Type": "faker",
        "workspaceId": WORKSPACE_ID,
    }
    if deferred is not None:
        body["credentialsDeferred"] = deferred
    return body


def _problem_body(reason: str = "secret_input_not_allowed") -> dict[str, Any]:
    return {
        "type": DEFERRED_SETUP_PROBLEM_TYPE,
        "title": "deferred-credential-setup",
        "status": 422,
        "detail": "Connector setup could not be prepared safely.",
        "data": {
            "reason": reason,
            "issues": [{"path": "/credentials/api_key", "code": "omit_secret"}],
            "authOptions": [
                {
                    "selectors": [
                        {"path": "/credentials/auth_type", "value": "oauth2.0"}
                    ]
                },
            ],
        },
    }


def _create(
    connector_type: str,
    session: requests.Session,
    *,
    defer_credentials: bool,
) -> Any:  # noqa: ANN401
    create = (
        api_util.create_source
        if connector_type == "source"
        else api_util.create_destination
    )
    return create(
        name="My connector",
        api_root="https://api.airbyte.test/v1",
        workspace_id=WORKSPACE_ID,
        config={"count": 10},
        definition_id=DEFINITION_ID,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
        defer_credentials=defer_credentials,
        http_session=session,
    )


# --- Transport -----------------------------------------------------------------------------


@pytest.mark.parametrize("via", ["request", "send"])
def test_deferred_session_applies_timeouts_and_refuses_redirects(via: str) -> None:
    """The deferred session times out and never follows a redirect (which would replay the POST).

    Both entry points are exercised because the generated SDK prepares requests itself and calls
    `send` directly, bypassing `request`.
    """
    adapter = _RecordingAdapter([
        (307, {}, {"Location": "https://elsewhere.test/v1/sources"})
    ])
    session = api_util.DeferredSetupSession()
    session.mount("https://", adapter)

    with pytest.raises(requests.TooManyRedirects):
        if via == "request":
            session.post("https://api.airbyte.test/v1/sources", json={})
        else:
            session.send(
                session.prepare_request(
                    requests.Request(
                        "POST", "https://api.airbyte.test/v1/sources", json={}
                    )
                )
            )

    assert len(adapter.requests) == 1
    assert adapter.send_kwargs[0]["timeout"] == (
        api_util.DEFERRED_CONNECT_TIMEOUT_SECS,
        api_util.DEFERRED_READ_TIMEOUT_SECS,
    )


@pytest.mark.parametrize(
    ("operation_id", "expected_body"),
    [
        ("createSource", {"name": "x", "deferCredentials": True}),
        ("createDestination", {"name": "x", "deferCredentials": True}),
        ("listSources", {"name": "x"}),
    ],
)
def test_defer_credentials_hook_targets_create_operations(
    operation_id: str,
    expected_body: dict[str, Any],
) -> None:
    """The wire flag is added only to the create operations."""
    request = requests.Request(
        "POST", "https://api.airbyte.test/v1/sources", json={"name": "x"}
    ).prepare()
    hook_ctx = BeforeRequestContext(
        HookContext(operation_id=operation_id, oauth2_scopes=None, security_source=None)
    )

    result = api_util._DeferCredentialsHook().before_request(hook_ctx, request)  # noqa: SLF001

    assert isinstance(result, requests.PreparedRequest)
    assert json.loads(cast(bytes, result.body)) == expected_body


# --- SDK integration -------------------------------------------------------------------------


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_deferred_create_sends_flag_and_returns_acknowledged_actor(
    connector_type: str,
) -> None:
    """A deferred create sends `deferCredentials` and returns the actor when Cloud acknowledges."""
    adapter = _RecordingAdapter([(200, _actor_body(connector_type, deferred=True), {})])
    session = api_util.DeferredSetupSession()
    session.mount("https://", adapter)

    actor = _create(connector_type, session, defer_credentials=True)

    (request,) = adapter.requests
    assert request.method == "POST"
    body = json.loads(cast(bytes, request.body))
    assert body["deferCredentials"] is True
    assert body["definitionId"] == DEFINITION_ID
    assert body["workspaceId"] == WORKSPACE_ID
    assert getattr(actor, f"{connector_type}_id") == ACTOR_ID


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_ordinary_create_does_not_send_flag(connector_type: str) -> None:
    """Without `defer_credentials`, the request body is unchanged."""
    adapter = _RecordingAdapter([(200, _actor_body(connector_type, deferred=None), {})])
    session = requests.Session()
    session.mount("https://", adapter)

    _create(connector_type, session, defer_credentials=False)

    (request,) = adapter.requests
    assert "deferCredentials" not in json.loads(cast(bytes, request.body))


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_deferred_create_without_acknowledgment_raises_with_actor_id(
    connector_type: str,
) -> None:
    """An unacknowledged create (older platform) is reported, with the created actor's ID."""
    adapter = _RecordingAdapter([(200, _actor_body(connector_type, deferred=None), {})])
    session = api_util.DeferredSetupSession()
    session.mount("https://", adapter)

    with pytest.raises(exc.AirbyteDeferredSetupError) as raised:
        _create(connector_type, session, defer_credentials=True)

    assert raised.value.actor_id == ACTOR_ID
    assert raised.value.problem is None
    assert len(adapter.requests) == 1


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_deferred_create_refusal_exposes_only_sanitized_problem(
    connector_type: str,
) -> None:
    """A platform refusal becomes a typed error carrying fixed codes and paths only."""
    adapter = _RecordingAdapter([(422, _problem_body(), {})])
    session = api_util.DeferredSetupSession()
    session.mount("https://", adapter)

    with pytest.raises(exc.AirbyteDeferredSetupError) as raised:
        _create(connector_type, session, defer_credentials=True)

    problem = raised.value.problem
    assert problem is not None
    assert problem.reason == "secret_input_not_allowed"
    assert [(i.path, i.code) for i in problem.issues] == [
        ("/credentials/api_key", "omit_secret")
    ]
    assert problem.auth_options[0].selectors[0].value == "oauth2.0"
    message = str(raised.value)
    assert "/credentials/api_key: Remove this credential" in message
    assert "oauth2.0" in message
    assert len(adapter.requests) == 1


@pytest.mark.parametrize(
    ("status_code", "body", "expected_reason"),
    [
        (422, _problem_body(), "secret_input_not_allowed"),
        (422, _problem_body("configuration_invalid"), "configuration_invalid"),
        (400, _problem_body(), None),
        (422, {**_problem_body(), "type": "https://example.test/other"}, None),
        (422, {**_problem_body(), "data": {"reason": "unknown", "issues": []}}, None),
        (
            422,
            {
                "type": DEFERRED_SETUP_PROBLEM_TYPE,
                "data": {
                    "reason": "configuration_invalid",
                    "issues": [{"path": "/a", "code": "raw text"}],
                },
            },
            None,
        ),
        (
            422,
            {
                "type": DEFERRED_SETUP_PROBLEM_TYPE,
                "data": {
                    "reason": "configuration_invalid",
                    "issues": [{"path": "not a pointer", "code": "required"}],
                },
            },
            None,
        ),
        (422, "not json", None),
        (422, None, None),
    ],
)
def test_parse_deferred_setup_problem(
    status_code: int,
    body: Any,  # noqa: ANN401
    expected_reason: str | None,
) -> None:
    """Only a well-formed deferred-setup problem is parsed; anything else is ignored."""
    raw = body if isinstance(body, (str, type(None))) else json.dumps(body)
    problem = parse_deferred_setup_problem(status_code=status_code, body=raw)
    assert (problem.reason if problem else None) == expected_reason


# --- Workspace -------------------------------------------------------------------------------


@dataclass
class _CreateCall:
    kwargs: dict[str, Any] = field(default_factory=dict)


def _workspace() -> CloudWorkspace:
    return CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        bearer_token=TOKEN,
        api_root="https://api.airbyte.test/v1",
    )


def _stub_create(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
) -> _CreateCall:
    call = _CreateCall()

    def _create_stub(**kwargs: Any) -> Any:  # noqa: ANN401
        call.kwargs = kwargs
        return type("Actor", (), {f"{connector_type}_id": ACTOR_ID})()

    monkeypatch.setattr(api_util, f"create_{connector_type}", _create_stub)
    monkeypatch.setattr(api_util, f"list_{connector_type}s", lambda **kwargs: [])
    return call


def _deploy(
    workspace: CloudWorkspace, connector_type: str, config: object, **kwargs: Any
) -> Any:  # noqa: ANN401
    if connector_type == "source":
        return workspace.deploy_source(
            name="My connector", source=cast(Any, config), **kwargs
        )
    return workspace.deploy_destination(
        name="My connector", destination=cast(Any, config), **kwargs
    )


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_deploy_deferred_uses_bounded_session_and_flag(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
) -> None:
    """Deferred deploys pass the flag and the definition through to the API layer."""
    call = _stub_create(monkeypatch, connector_type)

    deployed = _deploy(
        _workspace(),
        connector_type,
        {"count": 10},
        definition_id=DEFINITION_ID,
        defer_credentials=True,
    )

    assert deployed.connector_id == ACTOR_ID
    assert call.kwargs["defer_credentials"] is True
    assert call.kwargs["definition_id"] == DEFINITION_ID
    assert call.kwargs["config"] == {"count": 10}


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
@pytest.mark.parametrize(
    ("config", "kwargs", "match"),
    [
        ({"count": 10}, {}, "definition_id"),
        ({"count": 10}, {"definition_id": ""}, "definition_id"),
        ("not a dict", {"definition_id": DEFINITION_ID}, "configuration dictionary"),
        (
            {"api_key": SecretString("k")},
            {"definition_id": DEFINITION_ID},
            "secret values",
        ),
        (
            {"credentials": [{"token": "secret_reference::MY_TOKEN"}]},
            {"definition_id": DEFINITION_ID},
            "secret values",
        ),
        (
            {"credentials": ({"token": SecretString("k")},)},
            {"definition_id": DEFINITION_ID},
            "secret values",
        ),
    ],
)
def test_deploy_deferred_rejects_invalid_input_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    config: object,
    kwargs: dict[str, Any],
    match: str,
) -> None:
    """Missing definition, non-dict config, secret values and references never reach Cloud."""
    call = _stub_create(monkeypatch, connector_type)

    with pytest.raises(exc.PyAirbyteInputError, match=match):
        _deploy(_workspace(), connector_type, config, defer_credentials=True, **kwargs)

    assert call.kwargs == {}


def test_deploy_source_rejects_dict_config_without_defer_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raw source dictionaries are only accepted in deferred mode."""
    call = _stub_create(monkeypatch, "source")

    with pytest.raises(exc.PyAirbyteInputError):
        _deploy(_workspace(), "source", {"count": 10}, definition_id=DEFINITION_ID)

    assert call.kwargs == {}


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
@pytest.mark.parametrize("owner_workspace_id", [WORKSPACE_ID, OTHER_WORKSPACE_ID])
def test_check_connector_setup_verifies_workspace_before_checking(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    owner_workspace_id: str,
) -> None:
    """The check runs once, and only for connectors owned by the workspace."""
    checks: list[dict[str, Any]] = []
    monkeypatch.setattr(
        api_util,
        f"get_{connector_type}",
        lambda **kwargs: type("Actor", (), {"workspace_id": owner_workspace_id})(),
    )

    def _check(**kwargs: Any) -> tuple[bool, str | None]:  # noqa: ANN401
        checks.append(kwargs)
        return (False, "Provider said: token abc123 is invalid")

    monkeypatch.setattr(api_util, "check_connector", _check)
    workspace = _workspace()

    if owner_workspace_id != WORKSPACE_ID:
        with pytest.raises(exc.AirbyteMissingResourceError):
            workspace.check_connector_setup(cast(Any, connector_type), ACTOR_ID)
        assert checks == []
        return

    result = workspace.check_connector_setup(cast(Any, connector_type), ACTOR_ID)

    assert result.success is False
    assert len(checks) == 1
    assert checks[0]["actor_id"] == ACTOR_ID
    assert checks[0]["connector_type"] == connector_type


# --- MCP tools -------------------------------------------------------------------------------


@dataclass
class _DeployedLike:
    connector_id: str
    connector_url: str = (
        f"https://cloud.airbyte.test/workspaces/{WORKSPACE_ID}/settings"
    )


@dataclass
class _WorkspaceLike:
    workspace_id: str = WORKSPACE_ID
    deploy_calls: list[dict[str, Any]] = field(default_factory=list)
    check_result: CheckResult = field(default_factory=lambda: CheckResult(success=True))

    def deploy_source(self, **kwargs: Any) -> _DeployedLike:  # noqa: ANN401
        self.deploy_calls.append(kwargs)
        return _DeployedLike(connector_id=ACTOR_ID)

    def deploy_destination(self, **kwargs: Any) -> _DeployedLike:  # noqa: ANN401
        self.deploy_calls.append(kwargs)
        return _DeployedLike(connector_id=ACTOR_ID)

    def get_source(self, source_id: str) -> _DeployedLike:
        return _DeployedLike(connector_id=source_id)

    def get_destination(self, destination_id: str) -> _DeployedLike:
        return _DeployedLike(connector_id=destination_id)

    def check_connector_setup(
        self, connector_type: str, connector_id: str
    ) -> CheckResult:
        return self.check_result


@pytest.fixture
def workspace_like(monkeypatch: pytest.MonkeyPatch) -> _WorkspaceLike:
    workspace = _WorkspaceLike()
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )
    monkeypatch.setattr(
        cloud_mcp,
        "get_connector_metadata",
        lambda name: type(
            "Metadata",
            (),
            {
                "definition_id": DEFINITION_ID,
                "connector_type": name.split("-", 1)[0],
            },
        )(),
    )
    return workspace


@pytest.mark.parametrize(
    ("tool", "name_parameter", "connector_parameter", "connector_type"),
    [
        (
            cloud_mcp.deploy_source_to_cloud,
            "source_name",
            "source_connector_name",
            "source",
        ),
        (
            cloud_mcp.deploy_destination_to_cloud,
            "destination_name",
            "destination_connector_name",
            "destination",
        ),
    ],
)
def test_mcp_deploy_with_deferred_credentials_returns_handoff(
    workspace_like: _WorkspaceLike,
    tool: Callable[..., str],
    name_parameter: str,
    connector_parameter: str,
    connector_type: str,
) -> None:
    """Deferred MCP deploys return the settings link and next-step guidance."""
    raw = tool(
        ctx=cast(Context, object()),
        workspace_id=WORKSPACE_ID,
        config='{"count": 10}',
        config_secret_name=None,
        unique=True,
        defer_credentials=True,
        **{
            name_parameter: "My connector",
            connector_parameter: f"{connector_type}-faker",
        },
    )

    result = DeferredDeployResult.model_validate_json(raw)
    assert result.connector_id == ACTOR_ID
    assert result.connector_type == connector_type
    assert result.settings_url.endswith("/settings")
    assert "check_cloud_connector_setup" in result.guidance
    (call,) = workspace_like.deploy_calls
    assert call["defer_credentials"] is True
    assert call["definition_id"] == DEFINITION_ID
    assert call[connector_type] == {"count": 10}


def test_mcp_deploy_deferred_rejects_connector_type_mismatch(
    workspace_like: _WorkspaceLike,
) -> None:
    """A destination connector name cannot be deployed through the source tool."""
    with pytest.raises(exc.PyAirbyteInputError, match="not a source connector"):
        cloud_mcp.deploy_source_to_cloud(
            ctx=cast(Context, object()),
            source_name="My connector",
            source_connector_name="destination-faker",
            workspace_id=WORKSPACE_ID,
            config={"count": 10},
            config_secret_name=None,
            unique=True,
            defer_credentials=True,
        )

    assert workspace_like.deploy_calls == []


def test_mcp_deploy_deferred_rejects_config_secret_name(
    workspace_like: _WorkspaceLike,
) -> None:
    """Server-side secrets cannot be combined with deferred credentials."""
    with pytest.raises(exc.PyAirbyteInputError, match="config_secret_name"):
        cloud_mcp.deploy_source_to_cloud(
            ctx=cast(Context, object()),
            source_name="My connector",
            source_connector_name="source-faker",
            workspace_id=WORKSPACE_ID,
            config={"count": 10},
            config_secret_name="MY_SECRET",
            unique=True,
            defer_credentials=True,
        )

    assert workspace_like.deploy_calls == []


@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
@pytest.mark.parametrize("success", [True, False])
def test_mcp_check_connector_setup_reports_fixed_outcome(
    workspace_like: _WorkspaceLike,
    connector_type: str,
    success: bool,
) -> None:
    """The completion check exposes pass/fail and guidance, never provider error text."""
    workspace_like.check_result = CheckResult(
        success=success,
        error_message=None if success else "Provider said: token abc123 is invalid",
    )

    result = cast(
        ConnectorSetupCheckResult,
        cloud_mcp.check_cloud_connector_setup(
            ctx=cast(Context, object()),
            connector_type=cast(Any, connector_type),
            connector_id=ACTOR_ID,
            workspace_id=WORKSPACE_ID,
        ),
    )

    assert result.setup_complete is success
    assert result.connector_type == connector_type
    assert result.settings_url.endswith("/settings")
    assert "abc123" not in result.model_dump_json()


def test_mcp_check_connector_setup_is_not_read_only_or_idempotent() -> None:
    """The check triggers a connection test, so it must not be advertised as read-only."""
    (annotations,) = [
        a for f, a in _REGISTERED_TOOLS if f is cloud_mcp.check_cloud_connector_setup
    ]
    assert annotations["readOnlyHint"] is False
    assert annotations["idempotentHint"] is False
