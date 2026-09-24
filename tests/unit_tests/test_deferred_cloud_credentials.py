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


def _actor_body(connector_type: str, *, draft: bool | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "connectionConfiguration": {"count": 10},
        f"{connector_type}DefinitionId": DEFINITION_ID,
        "name": "My connector",
        f"{connector_type}Id": ACTOR_ID,
        f"{connector_type}Name": "Faker",
        "workspaceId": WORKSPACE_ID,
    }
    if draft is not None:
        body["isDraft"] = draft
    return body


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
    """A deferred create saves a draft without running a credential check."""
    responses.post(
        f"{CONFIG_API_ROOT}/{connector_type}s/create",
        json=_actor_body(connector_type, draft=True),
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
        "createAsDraft": True,
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
        json=_actor_body("source", draft=True),
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
@pytest.mark.parametrize("draft", [None, False, "true", 1])
def test_deferred_create_without_acknowledgment_raises_with_actor_id(
    connector_type: Literal["source", "destination"],
    draft: object,
) -> None:
    """Only a boolean draft acknowledgement is accepted; legacy acknowledgement is insufficient."""
    body = _actor_body(connector_type, draft=None)
    body["credentialsDeferred"] = True
    if draft is not None:
        body["isDraft"] = draft
    responses.post(
        f"{CONFIG_API_ROOT}/{connector_type}s/create",
        json=body,
    )

    with pytest.raises(exc.AirbyteDeferredSetupError) as raised:
        _create_deferred(connector_type)

    assert raised.value.actor_id == ACTOR_ID


@responses.activate
@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
@pytest.mark.parametrize("status_code", [400, 403, 422, 500])
def test_deferred_create_errors_do_not_expose_response_body(
    connector_type: Literal["source", "destination"],
    status_code: int,
) -> None:
    """Validation and HTTP failures expose their status without configuration or provider text."""
    responses.post(
        f"{CONFIG_API_ROOT}/{connector_type}s/create",
        status=status_code,
        json={"message": "Invalid credential abc123", "config": {"token": "abc123"}},
    )

    with pytest.raises(exc.AirbyteError) as raised:
        _create_deferred(connector_type)

    assert not isinstance(raised.value, exc.AirbyteDeferredSetupError)
    assert raised.value.context is not None
    assert raised.value.context["status_code"] == status_code
    assert "abc123" not in str(raised.value)


@responses.activate
@pytest.mark.parametrize(
    "body",
    [
        "not json abc123",
        "null",
        '["abc123"]',
        '{"isDraft": true, "config": "abc123"}',
        '{"isDraft": true, "sourceId": ""}',
        '{"isDraft": true, "sourceId": 123}',
    ],
)
def test_deferred_create_rejects_invalid_response_without_replaying(body: str) -> None:
    """A malformed successful response must not cause duplicate creates or leak its body."""
    responses.post(f"{CONFIG_API_ROOT}/sources/create", body=body)
    with pytest.raises(exc.AirbyteError) as raised:
        _create_deferred("source")
    assert "abc123" not in str(raised.value)
    assert len(responses.calls) == 1


# --- Workspace -------------------------------------------------------------------------------


@dataclass
class _CreateCall:
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class _ActorOwner:
    workspace_id: str = WORKSPACE_ID


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


@responses.activate
def test_get_destination_tolerates_partial_draft_configuration() -> None:
    """A draft destination's incomplete config must not crash typed deserialization."""
    partial_config = {"host": "localhost", "database": "example", "port": 5432}
    responses.add(
        responses.GET,
        f"{PUBLIC_API_ROOT}/destinations/{ACTOR_ID}",
        json={
            "destinationId": ACTOR_ID,
            "name": "My destination",
            "destinationType": "postgres",
            "definitionId": DEFINITION_ID,
            "workspaceId": WORKSPACE_ID,
            "createdAt": 1700000000,
            "configuration": partial_config,
        },
    )

    result = api_util.get_destination(
        destination_id=ACTOR_ID,
        api_root=PUBLIC_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
    )

    assert result.destination_id == ACTOR_ID
    assert result.workspace_id == WORKSPACE_ID


# --- MCP tools -------------------------------------------------------------------------------


@responses.activate
@pytest.mark.parametrize("connector_type", CONNECTOR_TYPES)
@pytest.mark.parametrize(
    ("status_code", "body", "complete"),
    [
        (200, {"status": "succeeded"}, True),
        (200, {"status": "failed", "message": "Invalid credential abc123"}, False),
        (422, {"message": "Missing required settings abc123"}, False),
    ],
)
def test_mcp_setup_check_uses_saved_actor_and_sanitizes_validation(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: Literal["source", "destination"],
    status_code: int,
    body: dict[str, str],
    complete: bool,
) -> None:
    """A saved draft is checked by ID, with incomplete configuration reported as not ready."""
    monkeypatch.setattr(
        api_util, f"get_{connector_type}", lambda **kwargs: _ActorOwner()
    )
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: _workspace()
    )
    responses.post(
        f"{CONFIG_API_ROOT}/{connector_type}s/check_connection",
        status=status_code,
        json=body,
    )

    result = cloud_mcp.check_cloud_connector_setup(
        ctx=cast(Context, object()),
        connector_type=connector_type,
        connector_id=ACTOR_ID,
        workspace_id=WORKSPACE_ID,
    )

    assert result.setup_complete is complete
    assert "abc123" not in result.model_dump_json()
    (call,) = responses.calls
    assert json.loads(cast(bytes, call.request.body)) == {
        f"{connector_type}Id": ACTOR_ID,
    }


@responses.activate
@pytest.mark.parametrize("status_code", [401, 403, 404, 500])
def test_setup_check_propagates_sanitized_operational_errors(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
) -> None:
    """Authentication and server failures must not be misreported as incomplete setup."""
    monkeypatch.setattr(api_util, "get_source", lambda **kwargs: _ActorOwner())
    responses.post(
        f"{CONFIG_API_ROOT}/sources/check_connection",
        status=status_code,
        json={"message": "Unexpected failure abc123"},
    )
    with pytest.raises(exc.AirbyteError) as raised:
        _workspace().check_connector_setup("source", ACTOR_ID)
    assert raised.value.context == {"status_code": status_code}
    assert "abc123" not in str(raised.value)


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
            message="Cloud created the connector without acknowledging draft mode.",
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
