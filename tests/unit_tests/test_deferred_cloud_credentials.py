# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for deferred-credential Cloud deployment and the setup completion check."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, cast

import pytest
import requests
import responses
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
from fastmcp import Context
from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701


WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
OTHER_WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"
DEFINITION_ID = "33333333-3333-4333-8333-333333333333"
ACTOR_ID = "44444444-4444-4444-8444-444444444444"
TOKEN = SecretString("test-bearer-token")

CONNECTOR_TYPES: list[Literal["source", "destination"]] = ["source", "destination"]


CONFIG_API_ROOT = "https://api.airbyte.test/api/v1"
PUBLIC_API_ROOT = "https://api.airbyte.test/api/public/v1"


def _actor_body(connector_type: str, *, deferred: bool | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "connectionConfiguration": {"count": 10},
        f"{connector_type}DefinitionId": DEFINITION_ID,
        "name": "My connector",
        f"{connector_type}Id": ACTOR_ID,
        f"{connector_type}Name": "Faker",
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


def _create_deferred(connector_type: Literal["source", "destination"]) -> str:
    return api_util.create_connector_deferred(
        connector_type=connector_type,
        name="My connector",
        workspace_id=WORKSPACE_ID,
        definition_id=DEFINITION_ID,
        config={"count": 10},
        api_root=PUBLIC_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
    )


# --- Config API call -------------------------------------------------------------------------


@responses.activate
@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_deferred_create_posts_flag_to_config_api_and_returns_actor_id(
    connector_type: Literal["source", "destination"],
) -> None:
    """A deferred create posts `deferCredentials` to the Config API create operation."""
    responses.post(
        f"{CONFIG_API_ROOT}/{connector_type}s/create",
        json=_actor_body(connector_type, deferred=True),
    )

    actor_id = _create_deferred(connector_type)

    assert actor_id == ACTOR_ID
    (call,) = responses.calls
    assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert json.loads(cast(bytes, call.request.body)) == {
        "name": "My connector",
        "workspaceId": WORKSPACE_ID,
        f"{connector_type}DefinitionId": DEFINITION_ID,
        "connectionConfiguration": {"count": 10},
        "deferCredentials": True,
    }
    assert call.request.url is not None
    assert "/api/public/" not in call.request.url


@responses.activate
def test_deferred_create_uses_bounded_timeouts_and_never_follows_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A redirect is an error rather than a replayed POST, and the request always times out."""
    responses.post(
        f"{CONFIG_API_ROOT}/sources/create",
        status=307,
        headers={"Location": "https://elsewhere.test/api/v1/sources/create"},
    )
    post_kwargs: list[dict[str, Any]] = []
    real_post = requests.post

    def _recording_post(url: str, **kwargs: Any) -> requests.Response:  # noqa: ANN401
        post_kwargs.append(kwargs)
        return real_post(url, **kwargs)

    monkeypatch.setattr(api_util.requests, "post", _recording_post)

    with pytest.raises(exc.AirbyteError) as raised:
        _create_deferred("source")

    assert raised.value.context is not None
    assert raised.value.context["status_code"] == 307
    assert len(responses.calls) == 1
    (kwargs,) = post_kwargs
    assert kwargs["allow_redirects"] is False
    assert kwargs["timeout"] == api_util.DEFERRED_CREATE_TIMEOUT_SECS


@responses.activate
def test_deferred_create_bounds_token_request_for_client_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Minting a bearer token from client credentials is bounded like the create itself."""
    responses.post(
        f"{PUBLIC_API_ROOT}/applications/token", json={"access_token": TOKEN}
    )
    responses.post(
        f"{CONFIG_API_ROOT}/sources/create",
        json=_actor_body("source", deferred=True),
    )
    post_kwargs: list[dict[str, Any]] = []
    real_post = requests.post

    def _recording_post(*args: Any, **kwargs: Any) -> requests.Response:  # noqa: ANN401
        post_kwargs.append(kwargs)
        return real_post(*args, **kwargs)

    monkeypatch.setattr(api_util.requests, "post", _recording_post)

    actor_id = api_util.create_connector_deferred(
        connector_type="source",
        name="My connector",
        workspace_id=WORKSPACE_ID,
        definition_id=DEFINITION_ID,
        config={"count": 10},
        api_root=PUBLIC_API_ROOT,
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
    )

    assert actor_id == ACTOR_ID
    token_kwargs, create_kwargs = post_kwargs
    assert token_kwargs["url"] == f"{PUBLIC_API_ROOT}/applications/token"
    assert token_kwargs["timeout"] == api_util.DEFERRED_CREATE_TIMEOUT_SECS
    assert create_kwargs["timeout"] == api_util.DEFERRED_CREATE_TIMEOUT_SECS
    assert responses.calls[1].request.headers["Authorization"] == f"Bearer {TOKEN}"


@responses.activate
@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_deferred_create_without_acknowledgment_raises_with_actor_id(
    connector_type: Literal["source", "destination"],
) -> None:
    """An unacknowledged create (older platform) is reported, with the created actor's ID."""
    responses.post(
        f"{CONFIG_API_ROOT}/{connector_type}s/create",
        json=_actor_body(connector_type, deferred=None),
    )

    with pytest.raises(exc.AirbyteDeferredSetupError) as raised:
        _create_deferred(connector_type)

    assert raised.value.actor_id == ACTOR_ID
    assert raised.value.problem is None


@responses.activate
@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
def test_deferred_create_refusal_exposes_only_sanitized_problem(
    connector_type: Literal["source", "destination"],
) -> None:
    """A platform refusal becomes a typed error carrying fixed codes and paths only."""
    responses.post(
        f"{CONFIG_API_ROOT}/{connector_type}s/create",
        status=422,
        json=_problem_body(),
    )

    with pytest.raises(exc.AirbyteDeferredSetupError) as raised:
        _create_deferred(connector_type)

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


@responses.activate
def test_deferred_create_other_errors_are_plain_airbyte_errors() -> None:
    """A non-deferred failure (here 403) is not misreported as a deferred-setup refusal."""
    responses.post(f"{CONFIG_API_ROOT}/sources/create", status=403, json={})

    with pytest.raises(exc.AirbyteError) as raised:
        _create_deferred("source")

    assert not isinstance(raised.value, exc.AirbyteDeferredSetupError)
    assert raised.value.context is not None
    assert raised.value.context["status_code"] == 403


@pytest.mark.parametrize(
    ("status_code", "body", "expected_reason"),
    [
        (422, _problem_body(), "secret_input_not_allowed"),
        (422, _problem_body("configuration_invalid"), "configuration_invalid"),
        (
            422,
            {
                "type": DEFERRED_SETUP_PROBLEM_TYPE,
                "data": {
                    "reason": "configuration_invalid",
                    "issues": [{"path": "/credentials", "code": "required"}],
                    "authOptions": None,
                },
            },
            "configuration_invalid",
        ),
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
        api_root=PUBLIC_API_ROOT,
    )


def _stub_create(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
) -> _CreateCall:
    """Record the deferred Config API create; fail loudly if the ordinary create is used."""
    call = _CreateCall()

    def _create_deferred_stub(**kwargs: Any) -> str:  # noqa: ANN401
        call.kwargs = kwargs
        return ACTOR_ID

    def _unexpected_create(**kwargs: Any) -> Any:  # noqa: ANN401
        raise AssertionError("ordinary create must not be used for deferred deploys")

    monkeypatch.setattr(api_util, "create_connector_deferred", _create_deferred_stub)
    monkeypatch.setattr(api_util, f"create_{connector_type}", _unexpected_create)
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
def test_deploy_deferred_uses_config_api_create(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
) -> None:
    """Deferred deploys go through the Config API create with the definition and config."""
    call = _stub_create(monkeypatch, connector_type)

    deployed = _deploy(
        _workspace(),
        connector_type,
        {"count": 10},
        definition_id=DEFINITION_ID,
        defer_credentials=True,
    )

    assert deployed.connector_id == ACTOR_ID
    assert call.kwargs["connector_type"] == connector_type
    assert call.kwargs["name"] == "My connector"
    assert call.kwargs["workspace_id"] == WORKSPACE_ID
    assert call.kwargs["definition_id"] == DEFINITION_ID
    assert call.kwargs["config"] == {"count": 10}
    assert call.kwargs["api_root"] == PUBLIC_API_ROOT


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
    assert result.workspace_id == WORKSPACE_ID
    assert result.settings_url.endswith("/settings")
    assert "check_cloud_connector_setup" in result.guidance
    assert "`workspace_id`" in result.guidance
    (call,) = workspace_like.deploy_calls
    assert call["defer_credentials"] is True
    assert call["definition_id"] == DEFINITION_ID
    assert call[connector_type] == {"count": 10}


def test_mcp_deploy_deferred_registers_unacknowledged_actor_for_cleanup(
    workspace_like: _WorkspaceLike,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An actor created without deferral acknowledgement is still tracked for safe-mode cleanup."""
    registered: list[str] = []
    monkeypatch.setattr(
        cloud_mcp, "register_guid_created_in_session", registered.append
    )

    def _unacknowledged(**kwargs: Any) -> _DeployedLike:  # noqa: ANN401
        raise exc.AirbyteDeferredSetupError(
            message="Cloud did not acknowledge the deferred-credential create.",
            actor_id=ACTOR_ID,
        )

    monkeypatch.setattr(workspace_like, "deploy_source", _unacknowledged)

    with pytest.raises(exc.AirbyteDeferredSetupError):
        cloud_mcp.deploy_source_to_cloud(
            ctx=cast(Context, object()),
            source_name="My connector",
            source_connector_name="source-faker",
            workspace_id=WORKSPACE_ID,
            config={"count": 10},
            config_secret_name=None,
            unique=True,
            defer_credentials=True,
        )

    assert registered == [ACTOR_ID]


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
