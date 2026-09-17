# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for deferred-credential Cloud connector setup and the safe completion check.

The HTTP layer is stubbed with `responses` at the adapter, so the real `DeferredSetupSession`
policy, the generated SDK's request serialization/decoding and the raw-wire evidence
projection all run for real.
"""

from __future__ import annotations

import inspect
import json
from typing import Any, cast, get_args

import pytest
import requests
import responses
from airbyte._util import api_util
from airbyte._util.api_util import CLOUD_API_ROOT, DeferredSetupSession
from airbyte._util.deferred_setup import (
    DEFERRED_SETUP_PROBLEM_TYPE,
    DeferredSetupOutcome,
    parse_deferred_setup_problem,
)
from airbyte.cloud import _deferred_setup
from airbyte.cloud.client import CloudClient
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteConnectorNotRegisteredError,
    PyAirbyteInputError,
    PyAirbyteInternalError,
)
from airbyte.mcp import cloud as cloud_mcp
from airbyte.registry import ConnectorMetadata
from airbyte.secrets.base import SecretString
from airbyte.sources.base import Source
from fastmcp import Context
from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701
from requests.adapters import HTTPAdapter


WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
DEFINITION_ID = "22222222-2222-4222-8222-222222222222"
ACTOR_ID = "33333333-3333-4333-8333-333333333333"
OTHER_ID = "44444444-4444-4444-8444-444444444444"
CANARY = "CANARY_SECRET_VALUE_9f8e7d"
TOKEN = SecretString("bearer-token")
CONFIG: dict[str, Any] = {
    "repositories": ["airbytehq/PyAirbyte"],
    "start_date": "2024-01-01",
}
CONFIG_API_ROOT = "https://cloud.airbyte.com/api/v1"


def _create_body(actor_type: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        f"{actor_type}Id": ACTOR_ID,
        "name": "My Connector",
        f"{actor_type}Type": "github",
        "workspaceId": WORKSPACE_ID,
        "definitionId": DEFINITION_ID,
        "configuration": {
            **CONFIG,
            "credentials": {"personal_access_token": "**********"},
        },
        "createdAt": 1700000000,
        "credentialsDeferred": True,
    }
    body.update(overrides)
    return {k: v for k, v in body.items() if v is not ...}


def _problem_body(reason: str, **data: Any) -> dict[str, Any]:
    return {
        "type": DEFERRED_SETUP_PROBLEM_TYPE,
        "title": "deferred-credential-setup",
        "status": 422,
        "detail": "Connector setup could not be prepared safely.",
        "data": {"reason": reason, "issues": [], "issuesTruncated": False, **data},
    }


def _list_body(count: int) -> dict[str, Any]:
    return {"data": [_create_body("source") for _ in range(count)], "next": None}


def _deploy(actor_type: str = "source", **overrides: Any) -> DeferredSetupOutcome:
    kwargs: dict[str, Any] = {
        "actor_type": actor_type,
        "name": "My Connector",
        "config": dict(CONFIG),
        "definition_id": DEFINITION_ID,
        "workspace_id": WORKSPACE_ID,
        "api_root": CLOUD_API_ROOT,
        "client_id": None,
        "client_secret": None,
        "bearer_token": TOKEN,
    }
    kwargs.update(overrides)
    return _deferred_setup.deploy_deferred(**kwargs)


def _collection_url(actor_type: str) -> str:
    return f"{CLOUD_API_ROOT}/{actor_type}s"


def _requests_to(url: str, method: str) -> list[responses.Call]:
    return [
        call
        for call in responses.calls
        if call.request.method == method and str(call.request.url).split("?")[0] == url
    ]


# Transport policy


def test_deferred_session_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fixed timeout, zero adapter retries and no redirect following, whatever callers pass."""
    seen: list[dict[str, Any]] = []

    def fake_send(
        self: HTTPAdapter, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:
        seen.append(kwargs)
        response = requests.Response()
        response.status_code = 307
        response.headers["Location"] = f"{CLOUD_API_ROOT}/elsewhere"
        response.request = request
        response.url = str(request.url)
        response._content = b""  # noqa: SLF001
        return response

    monkeypatch.setattr(HTTPAdapter, "send", fake_send)
    with DeferredSetupSession() as session:
        assert session.adapters["https://"].max_retries.total == 0
        assert session.adapters["http://"].max_retries.total == 0
        with pytest.raises(requests.TooManyRedirects):
            session.post(
                _collection_url("source"), json={}, timeout=None, allow_redirects=True
            )

    assert len(seen) == 1, "the redirect target must never be requested"
    assert seen[0]["timeout"] == (
        api_util.DEFERRED_CONNECT_TIMEOUT_SECS,
        api_util.DEFERRED_READ_TIMEOUT_SECS,
    )
    assert "allow_redirects" not in seen[0]


@responses.activate
def test_deferred_session_refuses_second_create() -> None:
    url = _collection_url("source")
    responses.add(responses.POST, url, json=_create_body("source"), status=200)
    with DeferredSetupSession() as session:
        session.expect_create(
            url=url,
            actor_type="source",
            workspace_id=WORKSPACE_ID,
            definition_id=DEFINITION_ID,
        )
        session.post(url, json={})
        assert session.create_sent
        assert session.evidence is not None
        assert session.evidence.actor_id == ACTOR_ID
        with pytest.raises(PyAirbyteInternalError):
            session.post(url, json={})
        with pytest.raises(PyAirbyteInternalError):
            session.expect_create(
                url=url,
                actor_type="source",
                workspace_id=WORKSPACE_ID,
                definition_id=DEFINITION_ID,
            )
    assert len(responses.calls) == 1


@pytest.mark.parametrize(
    ("status", "body", "expected_actor", "expected_ack"),
    [
        pytest.param(200, _create_body("source"), ACTOR_ID, True, id="literal_true"),
        pytest.param(
            200,
            _create_body("source", credentialsDeferred=...),
            ACTOR_ID,
            False,
            id="missing",
        ),
        pytest.param(
            200,
            _create_body("source", credentialsDeferred=None),
            ACTOR_ID,
            False,
            id="null",
        ),
        pytest.param(
            200,
            _create_body("source", credentialsDeferred=False),
            ACTOR_ID,
            False,
            id="false",
        ),
        pytest.param(
            200,
            _create_body("source", credentialsDeferred="true"),
            ACTOR_ID,
            False,
            id="string",
        ),
        pytest.param(
            200,
            _create_body("source", credentialsDeferred=1),
            ACTOR_ID,
            False,
            id="number",
        ),
        pytest.param(
            200,
            _create_body("source", credentialsDeferred={}),
            ACTOR_ID,
            False,
            id="object",
        ),
        pytest.param(
            200,
            _create_body("source", credentialsDeferred=[True]),
            ACTOR_ID,
            False,
            id="array",
        ),
        pytest.param(
            200,
            _create_body(
                "source", sourceId=ACTOR_ID.upper(), workspaceId=WORKSPACE_ID.upper()
            ),
            ACTOR_ID,
            True,
            id="canonicalized_uuids",
        ),
        pytest.param(
            200,
            _create_body("source", workspaceId=OTHER_ID),
            None,
            False,
            id="foreign_workspace",
        ),
        pytest.param(
            200,
            _create_body("source", definitionId=OTHER_ID),
            None,
            False,
            id="other_definition",
        ),
        pytest.param(
            200,
            _create_body("source", sourceId=..., destinationId=ACTOR_ID),
            None,
            False,
            id="wrong_actor_key",
        ),
        pytest.param(
            200, _create_body("source", sourceId=12), None, False, id="non_string_id"
        ),
        pytest.param(
            200, _create_body("source", sourceId="not-a-uuid"), None, False, id="bad_id"
        ),
        pytest.param(200, [_create_body("source")], None, False, id="array_body"),
        pytest.param(201, _create_body("source"), None, False, id="non_200_status"),
    ],
)
@responses.activate
def test_create_evidence_projection(
    status: int, body: Any, expected_actor: str | None, expected_ack: bool
) -> None:
    url = _collection_url("source")
    responses.add(responses.POST, url, json=body, status=status)
    with DeferredSetupSession() as session:
        session.expect_create(
            url=url,
            actor_type="source",
            workspace_id=WORKSPACE_ID,
            definition_id=DEFINITION_ID,
        )
        session.post(url, json={})
    if expected_actor is None and status != 200:
        assert session.evidence is None
        return
    assert session.evidence is not None
    assert session.evidence.actor_id == expected_actor
    assert session.evidence.acknowledged is expected_ack


@responses.activate
def test_create_evidence_ignores_oversized_body() -> None:
    url = _collection_url("source")
    body = _create_body(
        "source", padding="x" * (api_util.DEFERRED_EVIDENCE_MAX_BYTES + 1)
    )
    responses.add(responses.POST, url, json=body, status=200)
    with DeferredSetupSession() as session:
        session.expect_create(
            url=url,
            actor_type="source",
            workspace_id=WORKSPACE_ID,
            definition_id=DEFINITION_ID,
        )
        session.post(url, json={})
    assert session.evidence is not None
    assert session.evidence.actor_id is None


# SDK compatibility bridge


def test_sdk_feature_detection_matches_installed_models() -> None:
    """The typed fields are absent from the pinned 0.x SDK; the wire hook bridges the gap."""
    assert api_util.sdk_supports_deferred_credentials() is False
    instance = api_util.get_airbyte_server_instance(
        api_root=CLOUD_API_ROOT, client_id=None, client_secret=None, bearer_token=TOKEN
    )
    assert api_util._prepare_deferred_create(instance, defer_credentials=False) == {}  # noqa: SLF001
    assert api_util._prepare_deferred_create(instance, defer_credentials=True) == {}  # noqa: SLF001


@pytest.mark.parametrize(
    ("actor_type", "creator"),
    [
        pytest.param("source", api_util.create_source, id="source"),
        pytest.param("destination", api_util.create_destination, id="destination"),
    ],
)
@responses.activate
def test_create_sends_defer_flag_only_when_deferred(
    actor_type: str, creator: Any
) -> None:
    url = _collection_url(actor_type)
    responses.add(responses.POST, url, json=_create_body(actor_type), status=200)
    responses.add(responses.POST, url, json=_create_body(actor_type), status=200)
    common: dict[str, Any] = {
        "workspace_id": WORKSPACE_ID,
        "config": dict(CONFIG),
        "definition_id": DEFINITION_ID,
        "api_root": CLOUD_API_ROOT,
        "client_id": None,
        "client_secret": None,
        "bearer_token": TOKEN,
    }
    with DeferredSetupSession() as session:
        creator("n", defer_credentials=True, http_session=session, **common)
    creator("n", **common)

    deferred_body = json.loads(responses.calls[0].request.body)
    ordinary_body = json.loads(responses.calls[1].request.body)
    assert deferred_body["deferCredentials"] is True
    assert deferred_body["configuration"] == CONFIG
    assert deferred_body["definitionId"] == DEFINITION_ID
    assert f"{actor_type}Type" not in deferred_body["configuration"]
    assert "deferCredentials" not in ordinary_body
    assert int(responses.calls[0].request.headers["Content-Length"]) == len(
        responses.calls[0].request.body
    )


@responses.activate
def test_create_destination_with_definition_id_skips_type_inference() -> None:
    """An explicit definition ID must not require `destinationType` in the configuration."""
    url = _collection_url("destination")
    responses.add(responses.POST, url, json=_create_body("destination"), status=200)
    api_util.create_destination(
        "n",
        workspace_id=WORKSPACE_ID,
        config={"host": "db.example.com"},
        definition_id=DEFINITION_ID,
        api_root=CLOUD_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
    )
    body = json.loads(responses.calls[0].request.body)
    assert body["definitionId"] == DEFINITION_ID
    assert body["configuration"] == {"host": "db.example.com"}


# Problem parsing


@pytest.mark.parametrize(
    ("status", "body", "expected_reason"),
    [
        pytest.param(
            422,
            _problem_body(
                "secret_input_not_allowed",
                issues=[
                    {
                        "path": "/credentials/token",
                        "code": "omit_secret",
                        "message": CANARY,
                    }
                ],
            ),
            "secret_input_not_allowed",
            id="secret_input",
        ),
        pytest.param(
            422,
            _problem_body(
                "auth_selection_required",
                authOptions=[
                    {
                        "selectors": [
                            {"path": "/credentials/auth_type", "value": "oauth"}
                        ]
                    }
                ],
            ),
            "auth_selection_required",
            id="auth_selection",
        ),
        pytest.param(
            422, _problem_body("unsupported_schema"), "unsupported_schema", id="unsup"
        ),
        pytest.param(
            422,
            _problem_body("auth_selection_required"),
            None,
            id="auth_selection_without_options",
        ),
        pytest.param(
            422,
            _problem_body("secret_input_not_allowed", authOptions=[{"selectors": []}]),
            None,
            id="options_on_other_reason",
        ),
        pytest.param(422, _problem_body("something_else"), None, id="unknown_reason"),
        pytest.param(
            422,
            {**_problem_body("unsupported_schema"), "type": "https://x/other"},
            None,
            id="type",
        ),
        pytest.param(400, _problem_body("unsupported_schema"), None, id="wrong_status"),
        pytest.param(422, "not json", None, id="not_json"),
        pytest.param(422, [], None, id="array"),
        pytest.param(422, None, None, id="no_body"),
    ],
)
def test_parse_deferred_setup_problem(
    status: int, body: Any, expected_reason: str | None
) -> None:
    raw = body if isinstance(body, (str, bytes)) or body is None else json.dumps(body)
    problem = parse_deferred_setup_problem(status_code=status, body=raw)
    if expected_reason is None:
        assert problem is None
        return
    assert problem is not None
    assert problem.reason == expected_reason
    assert CANARY not in problem.model_dump_json()
    for issue in problem.issues:
        assert (
            issue.message
            == "Remove this credential; the user supplies it in Airbyte Cloud."
        )


def test_parse_deferred_setup_problem_rejects_oversized_body() -> None:
    body = json.dumps(_problem_body("unsupported_schema", pad="x" * 70_000))
    assert parse_deferred_setup_problem(status_code=422, body=body) is None


# Deferred deployment outcomes


@pytest.mark.parametrize(
    ("list_status", "list_body", "create_status", "create_body", "expected"),
    [
        pytest.param(
            200,
            _list_body(0),
            200,
            _create_body("source"),
            ("awaiting_user", "complete_in_cloud", None, ACTOR_ID),
            id="acknowledged",
        ),
        pytest.param(
            200,
            _list_body(0),
            200,
            _create_body("source", credentialsDeferred=...),
            (
                "created_unconfirmed",
                "complete_in_cloud",
                "acknowledgment_missing",
                ACTOR_ID,
            ),
            id="ack_missing",
        ),
        pytest.param(
            200,
            _list_body(0),
            200,
            _create_body("source", credentialsDeferred="true"),
            (
                "created_unconfirmed",
                "complete_in_cloud",
                "acknowledgment_missing",
                ACTOR_ID,
            ),
            id="ack_string",
        ),
        pytest.param(
            200,
            _list_body(0),
            200,
            _create_body("source", name=...),
            (
                "created_unconfirmed",
                "complete_in_cloud",
                "acknowledgment_missing",
                ACTOR_ID,
            ),
            id="sdk_decode_failure_after_trusted_identity",
        ),
        pytest.param(
            200,
            _list_body(0),
            200,
            _create_body("source", workspaceId=OTHER_ID),
            (
                "outcome_unknown",
                "inspect_cloud_before_retry",
                "response_unrecognized",
                None,
            ),
            id="foreign_identity",
        ),
        pytest.param(
            200,
            _list_body(0),
            500,
            {"message": CANARY},
            (
                "outcome_unknown",
                "inspect_cloud_before_retry",
                "response_unrecognized",
                None,
            ),
            id="server_error",
        ),
        pytest.param(
            200,
            _list_body(0),
            307,
            {},
            (
                "outcome_unknown",
                "inspect_cloud_before_retry",
                "response_unrecognized",
                None,
            ),
            id="redirect_not_replayed",
        ),
        pytest.param(
            200,
            _list_body(0),
            422,
            {"message": CANARY},
            (
                "outcome_unknown",
                "inspect_cloud_before_retry",
                "response_unrecognized",
                None,
            ),
            id="untrusted_422",
        ),
        pytest.param(
            200,
            _list_body(0),
            422,
            _problem_body(
                "secret_input_not_allowed",
                issues=[
                    {
                        "path": "/credentials/token",
                        "code": "omit_secret",
                        "message": CANARY,
                    }
                ],
            ),
            (
                "invalid_config",
                "correct_nonsecret_config",
                "secret_input_not_allowed",
                None,
            ),
            id="secret_refused",
        ),
        pytest.param(
            200,
            _list_body(0),
            422,
            _problem_body(
                "configuration_invalid",
                issues=[{"path": "/start_date", "code": "pattern", "message": "bad"}],
            ),
            (
                "invalid_config",
                "correct_nonsecret_config",
                "configuration_invalid",
                None,
            ),
            id="config_invalid",
        ),
        pytest.param(
            200,
            _list_body(0),
            422,
            _problem_body(
                "auth_selection_required",
                authOptions=[
                    {
                        "selectors": [
                            {"path": "/credentials/auth_type", "value": "oauth"}
                        ]
                    }
                ],
            ),
            ("invalid_config", "choose_auth_method", "auth_selection_required", None),
            id="auth_selection",
        ),
        pytest.param(
            200,
            _list_body(0),
            422,
            _problem_body("unsupported_schema"),
            ("unsupported", "contact_support", "unsupported_schema", None),
            id="unsupported_schema",
        ),
        pytest.param(
            200,
            _list_body(0),
            403,
            {"message": CANARY},
            ("not_created", "verify_access", "access_denied", None),
            id="create_forbidden",
        ),
        pytest.param(
            200,
            _list_body(1),
            None,
            None,
            ("name_conflict", "choose_another_name", None, None),
            id="name_conflict",
        ),
        pytest.param(
            401,
            {"message": CANARY},
            None,
            None,
            ("not_created", "verify_access", "access_denied", None),
            id="list_unauthorized",
        ),
        pytest.param(
            500,
            {"message": CANARY},
            None,
            None,
            ("not_created", "retry_later", "preflight_unavailable", None),
            id="list_server_error",
        ),
        pytest.param(
            None,
            None,
            None,
            None,
            ("not_created", "retry_later", "preflight_unavailable", None),
            id="list_connection_error",
        ),
    ],
)
@responses.activate
def test_deploy_deferred_outcomes(
    list_status: int | None,
    list_body: Any,
    create_status: int | None,
    create_body: Any,
    expected: tuple[str, str, str | None, str | None],
) -> None:
    url = _collection_url("source")
    if list_status is None:
        responses.add(
            responses.GET, url, body=requests.ConnectionError("boom " + CANARY)
        )
    else:
        responses.add(responses.GET, url, json=list_body, status=list_status)
    if create_status is not None:
        responses.add(responses.POST, url, json=create_body, status=create_status)

    outcome = _deploy()

    status, next_action, reason, actor_id = expected
    assert (outcome.status, outcome.next_action, outcome.reason, outcome.actor_id) == (
        status,
        next_action,
        reason,
        actor_id,
    )
    assert CANARY not in outcome.model_dump_json()
    create_calls = _requests_to(url, "POST")
    assert len(create_calls) == (1 if create_status is not None else 0), (
        "exactly one create"
    )
    if create_calls:
        sent = json.loads(create_calls[0].request.body)
        assert sent["deferCredentials"] is True
        assert sent["configuration"] == CONFIG
        assert sent["workspaceId"] == WORKSPACE_ID
        assert sent["definitionId"] == DEFINITION_ID
    if outcome.problem is not None:
        assert outcome.problem.reason == reason
    else:
        assert reason not in {"secret_input_not_allowed", "configuration_invalid"}


@responses.activate
def test_deploy_deferred_destination_parity() -> None:
    url = _collection_url("destination")
    responses.add(responses.GET, url, json={"data": [], "next": None}, status=200)
    responses.add(responses.POST, url, json=_create_body("destination"), status=200)

    outcome = _deploy("destination", config={"host": "db.example.com"})

    assert outcome.status == "awaiting_user"
    assert outcome.actor_id == ACTOR_ID
    sent = json.loads(responses.calls[-1].request.body)
    assert sent["deferCredentials"] is True
    assert sent["configuration"] == {"host": "db.example.com"}
    assert sent["definitionId"] == DEFINITION_ID


@responses.activate
def test_deploy_deferred_source_body_rejected_for_destination() -> None:
    """A source-shaped body on the destination collection carries no trusted identity."""
    url = _collection_url("destination")
    responses.add(responses.GET, url, json={"data": [], "next": None}, status=200)
    responses.add(responses.POST, url, json=_create_body("source"), status=200)

    outcome = _deploy("destination")

    assert outcome.status == "outcome_unknown"
    assert outcome.actor_id is None


@responses.activate
def test_deploy_deferred_with_client_credentials_maps_token_denial() -> None:
    responses.add(
        responses.POST,
        f"{CLOUD_API_ROOT}/applications/token",
        json={"m": CANARY},
        status=401,
    )
    outcome = _deploy(
        client_id=SecretString("id"),
        client_secret=SecretString("secret"),
        bearer_token=None,
    )
    assert (outcome.status, outcome.reason) == ("not_created", "access_denied")
    assert len(responses.calls) == 1, "the token request must not be retried"
    assert CANARY not in outcome.model_dump_json()


@responses.activate
def test_deploy_deferred_does_not_paginate_past_a_bad_page() -> None:
    url = _collection_url("source")
    responses.add(
        responses.GET,
        url,
        json={
            "data": [_create_body("source", name="Other")],
            "next": f"{url}?offset=1",
        },
        status=200,
    )
    responses.add(responses.GET, url, json={"m": CANARY}, status=502)

    outcome = _deploy()

    assert (outcome.status, outcome.reason) == ("not_created", "preflight_unavailable")
    assert not _requests_to(url, "POST")


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param(
            {"workspace_id": "nope"}, ("invalid_config", "invalid_input"), id="ws"
        ),
        pytest.param(
            {"definition_id": ""}, ("invalid_config", "invalid_input"), id="definition"
        ),
        pytest.param(
            {"api_root": "https://self-managed.example.com/api/public/v1"},
            ("not_created", "unsupported_api_root"),
            id="api_root",
        ),
    ],
)
@responses.activate
def test_deploy_deferred_refuses_before_network(
    overrides: dict[str, Any], expected: tuple[str, str]
) -> None:
    outcome = _deploy(**overrides)
    assert (outcome.status, outcome.reason) == expected
    assert outcome.actor_id is None
    assert not responses.calls


# Input normalization


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        pytest.param(None, {}, id="none"),
        pytest.param({}, {}, id="empty"),
        pytest.param({"a": 1}, {"a": 1}, id="dict"),
        pytest.param(
            '{"a": false, "b": 0, "c": ""}', {"a": False, "b": 0, "c": ""}, id="json"
        ),
        pytest.param("null", None, id="null_string"),
        pytest.param("[]", None, id="array_string"),
        pytest.param("42", None, id="scalar_string"),
        pytest.param("/tmp/config.json", None, id="path"),
        pytest.param("secret_reference::name", None, id="secret_reference"),
        pytest.param({"sourceType": "github"}, None, id="source_type_key"),
    ],
)
def test_normalize_deferred_config(
    config: Any, expected: dict[str, Any] | None
) -> None:
    if expected is None:
        with pytest.raises(PyAirbyteInputError):
            _deferred_setup.normalize_deferred_config(config, actor_type="source")
        return
    assert (
        _deferred_setup.normalize_deferred_config(config, actor_type="source")
        == expected
    )


def test_normalize_deferred_config_rejects_destination_type_key() -> None:
    with pytest.raises(PyAirbyteInputError):
        _deferred_setup.normalize_deferred_config(
            {"destinationType": "x"}, actor_type="destination"
        )


# Workspace API guards


def _workspace() -> CloudWorkspace:
    return CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
        api_root=CLOUD_API_ROOT,
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"defer_credentials": True}, id="missing_definition"),
        pytest.param(
            {
                "defer_credentials": True,
                "definition_id": DEFINITION_ID,
                "unique": False,
            },
            id="not_unique",
        ),
        pytest.param(
            {
                "defer_credentials": True,
                "definition_id": DEFINITION_ID,
                "random_name_suffix": True,
            },
            id="random_suffix",
        ),
        pytest.param({"definition_id": DEFINITION_ID}, id="definition_without_defer"),
        pytest.param({}, id="dict_without_defer"),
    ],
)
@responses.activate
def test_workspace_deploy_guards(kwargs: dict[str, Any]) -> None:
    workspace = _workspace()
    with pytest.raises(PyAirbyteInputError):
        workspace.deploy_source("n", dict(CONFIG), **kwargs)
    with pytest.raises(PyAirbyteInputError):
        workspace.deploy_destination("n", dict(CONFIG), **kwargs)
    assert not responses.calls


@responses.activate
def test_workspace_deferred_deploy_never_reads_connector_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A constructed `Source` is refused without touching its config; a dict is deployed."""
    source = cast(Source, object.__new__(Source))

    def explode(*_: Any, **__: Any) -> None:
        raise AssertionError("connector object must not be read")

    monkeypatch.setattr(Source, "_hydrated_config", property(explode))
    workspace = _workspace()
    with pytest.raises(PyAirbyteInputError):
        workspace.deploy_source(
            "n", source, definition_id=DEFINITION_ID, defer_credentials=True
        )  # type: ignore[call-overload]
    assert not responses.calls

    url = _collection_url("source")
    responses.add(responses.GET, url, json={"data": [], "next": None}, status=200)
    responses.add(responses.POST, url, json=_create_body("source"), status=200)
    outcome = workspace.deploy_source(
        "n", dict(CONFIG), definition_id=DEFINITION_ID, defer_credentials=True
    )
    assert outcome.status == "awaiting_user"
    assert outcome.actor_id == ACTOR_ID


# Safe completion check


def _check(**overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "actor_type": "source",
        "actor_id": ACTOR_ID,
        "workspace_id": WORKSPACE_ID,
        "api_root": CLOUD_API_ROOT,
        "config_api_root": CONFIG_API_ROOT,
        "client_id": None,
        "client_secret": None,
        "bearer_token": TOKEN,
    }
    kwargs.update(overrides)
    return _deferred_setup.check_connector_setup(**kwargs)


CHECK_URL = f"{CONFIG_API_ROOT}/sources/check_connection"
SETTINGS_URL = f"https://cloud.airbyte.com/workspaces/{WORKSPACE_ID}/source/{ACTOR_ID}"


@pytest.mark.parametrize(
    (
        "metadata_status",
        "metadata_body",
        "check_status",
        "check_body",
        "expected",
        "checks",
    ),
    [
        pytest.param(
            200,
            _create_body("source"),
            200,
            {"status": "succeeded"},
            ("succeeded", "continue", SETTINGS_URL),
            1,
            id="succeeded",
        ),
        pytest.param(
            200,
            _create_body("source"),
            200,
            {"status": "failed", "message": CANARY},
            ("failed", "complete_in_cloud", SETTINGS_URL),
            1,
            id="failed",
        ),
        pytest.param(
            200,
            _create_body("source"),
            200,
            {"status": "running", "message": CANARY},
            ("unknown", "inspect_cloud_before_retry", SETTINGS_URL),
            1,
            id="indeterminate",
        ),
        pytest.param(
            200,
            _create_body("source"),
            200,
            {"m": CANARY},
            ("unknown", "inspect_cloud_before_retry", SETTINGS_URL),
            1,
            id="unrecognized_body",
        ),
        pytest.param(
            200,
            _create_body("source"),
            500,
            {"m": CANARY},
            ("unknown", "inspect_cloud_before_retry", SETTINGS_URL),
            1,
            id="check_server_error",
        ),
        pytest.param(
            200,
            _create_body("source"),
            403,
            {"m": CANARY},
            ("not_accessible", "verify_access", SETTINGS_URL),
            1,
            id="check_forbidden",
        ),
        pytest.param(
            200,
            _create_body("source", workspaceId=OTHER_ID),
            None,
            None,
            ("not_accessible", "verify_workspace", None),
            0,
            id="foreign_workspace",
        ),
        pytest.param(
            404,
            {"m": CANARY},
            None,
            None,
            ("not_accessible", "verify_workspace", None),
            0,
            id="missing_actor",
        ),
        pytest.param(
            403,
            {"m": CANARY},
            None,
            None,
            ("not_accessible", "verify_workspace", None),
            0,
            id="unauthorized_actor",
        ),
        pytest.param(
            500,
            {"m": CANARY},
            None,
            None,
            ("unknown", "verify_workspace", None),
            0,
            id="metadata_server_error",
        ),
        pytest.param(
            200,
            {"garbage": CANARY},
            None,
            None,
            ("unknown", "verify_workspace", None),
            0,
            id="metadata_malformed",
        ),
    ],
)
@responses.activate
def test_check_connector_setup_outcomes(
    metadata_status: int,
    metadata_body: Any,
    check_status: int | None,
    check_body: Any,
    expected: tuple[str, str, str | None],
    checks: int,
) -> None:
    responses.add(
        responses.GET,
        f"{CLOUD_API_ROOT}/sources/{ACTOR_ID}",
        json=metadata_body,
        status=metadata_status,
    )
    if check_status is not None:
        responses.add(responses.POST, CHECK_URL, json=check_body, status=check_status)

    outcome = _check()

    assert (outcome.status, outcome.next_action, outcome.settings_url) == expected
    assert outcome.actor_id == ACTOR_ID
    assert outcome.workspace_id == WORKSPACE_ID
    assert CANARY not in outcome.model_dump_json()
    check_calls = _requests_to(CHECK_URL, "POST")
    assert len(check_calls) == checks, "at most one check POST, never retried"
    if check_calls:
        assert json.loads(check_calls[0].request.body) == {"sourceId": ACTOR_ID}


@pytest.mark.parametrize(
    ("overrides", "expected_ids"),
    [
        pytest.param({"actor_id": "bad"}, (None, WORKSPACE_ID), id="actor"),
        pytest.param({"workspace_id": None}, (ACTOR_ID, None), id="no_workspace"),
        pytest.param({"workspace_id": "bad"}, (ACTOR_ID, None), id="workspace"),
    ],
)
@responses.activate
def test_check_connector_setup_validates_ids_before_network(
    overrides: dict[str, Any], expected_ids: tuple[str | None, str | None]
) -> None:
    outcome = _check(**overrides)
    assert (outcome.status, outcome.next_action) == (
        "not_accessible",
        "verify_workspace",
    )
    assert (outcome.actor_id, outcome.workspace_id) == expected_ids
    assert outcome.settings_url is None
    assert not responses.calls


@responses.activate
def test_check_connector_setup_token_denial_before_check() -> None:
    responses.add(
        responses.GET,
        f"{CLOUD_API_ROOT}/sources/{ACTOR_ID}",
        json=_create_body("source"),
        status=200,
    )
    responses.add(
        responses.POST,
        f"{CLOUD_API_ROOT}/applications/token",
        json={"m": CANARY},
        status=401,
    )
    outcome = _check(
        client_id=SecretString("id"),
        client_secret=SecretString("secret"),
        bearer_token=None,
    )
    assert (outcome.status, outcome.next_action) == ("not_accessible", "verify_access")
    assert not _requests_to(CHECK_URL, "POST")
    assert CANARY not in outcome.model_dump_json()


@responses.activate
def test_check_connector_setup_destination_uses_destination_paths() -> None:
    responses.add(
        responses.GET,
        f"{CLOUD_API_ROOT}/destinations/{ACTOR_ID}",
        json=_create_body("destination"),
        status=200,
    )
    check_url = f"{CONFIG_API_ROOT}/destinations/check_connection"
    responses.add(responses.POST, check_url, json={"status": "succeeded"}, status=200)
    outcome = _check(actor_type="destination")
    assert outcome.status == "succeeded"
    assert outcome.settings_url == (
        f"https://cloud.airbyte.com/workspaces/{WORKSPACE_ID}/destination/{ACTOR_ID}"
    )
    assert json.loads(_requests_to(check_url, "POST")[0].request.body) == {
        "destinationId": ACTOR_ID
    }


# MCP presentation layer


def _mcp_client(workspace_id: str | None = WORKSPACE_ID) -> CloudClient:
    return CloudClient(
        client_id=None,
        client_secret=None,
        bearer_token=TOKEN,
        public_api_root=CLOUD_API_ROOT,
        config_api_root=CONFIG_API_ROOT,
        workspace_id=workspace_id,
    )


def _metadata(actor_type: str) -> ConnectorMetadata:
    return ConnectorMetadata(
        name=f"{actor_type}-github",
        connector_type=actor_type,
        definition_id=DEFINITION_ID,
        latest_available_version=None,
        pypi_package_name=None,
        language=None,
        install_types=set(),
    )


_DEPLOY_DEFAULTS: dict[str, Any] = {
    "workspace_id": None,
    "config": None,
    "config_secret_name": None,
    "unique": True,
    "defer_credentials": True,
}


def _deploy_tool(actor_type: str, **kwargs: Any) -> dict[str, Any]:
    """Call the deploy tool as FastMCP would, filling the `Field` defaults it applies."""
    tool = (
        cloud_mcp.deploy_source_to_cloud
        if actor_type == "source"
        else (cloud_mcp.deploy_destination_to_cloud)
    )
    raw = tool(
        cast(Context, object()),
        **{
            f"{actor_type}_name": "My Connector",
            f"{actor_type}_connector_name": f"{actor_type}-github",
        },
        **{**_DEPLOY_DEFAULTS, **kwargs},
    )
    return json.loads(raw)


def test_mcp_tools_declare_deferred_parameters() -> None:
    for tool in (
        cloud_mcp.deploy_source_to_cloud,
        cloud_mcp.deploy_destination_to_cloud,
    ):
        parameter = inspect.signature(tool).parameters["defer_credentials"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        (field,) = get_args(parameter.annotation)[1:]
        assert field.default is False


def test_mcp_check_tool_is_not_read_only_or_idempotent() -> None:
    annotations = next(
        a for f, a in _REGISTERED_TOOLS if f is cloud_mcp.check_cloud_connector_setup
    )
    assert annotations["readOnlyHint"] is False
    assert annotations["idempotentHint"] is False
    assert annotations["destructiveHint"] is False
    assert annotations["openWorldHint"] is True


@pytest.mark.parametrize(
    "actor_type",
    [
        pytest.param("source", id="source"),
        pytest.param("destination", id="destination"),
    ],
)
@responses.activate
def test_mcp_deploy_deferred_returns_safe_result(
    monkeypatch: pytest.MonkeyPatch,
    actor_type: str,
) -> None:
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _: _mcp_client())
    monkeypatch.setattr(
        cloud_mcp, "get_connector_metadata", lambda name, **_: _metadata(actor_type)
    )

    def never(*_: Any, **__: Any) -> None:
        raise AssertionError(
            "local connectors must not be constructed in deferred mode"
        )

    monkeypatch.setattr(cloud_mcp, "get_source", never)
    monkeypatch.setattr(cloud_mcp, "get_destination", never)
    monkeypatch.setattr(cloud_mcp, "resolve_connector_config", never)
    url = _collection_url(actor_type)
    responses.add(responses.GET, url, json={"data": [], "next": None}, status=200)
    responses.add(responses.POST, url, json=_create_body(actor_type), status=200)

    result = _deploy_tool(actor_type, config=json.dumps(CONFIG))
    raw = json.dumps(result)

    assert result["status"] == "awaiting_user"
    assert result["next_action"] == "complete_in_cloud"
    assert result["connector_type"] == actor_type
    assert result["connector_id"] == ACTOR_ID
    assert result["workspace_id"] == WORKSPACE_ID
    assert result["settings_url"] == (
        f"https://cloud.airbyte.com/workspaces/{WORKSPACE_ID}/{actor_type}/{ACTOR_ID}"
    )
    assert "check_cloud_connector_setup" in result["guidance"]
    assert set(result) == {
        "connector_type",
        "status",
        "next_action",
        "reason",
        "connector_id",
        "workspace_id",
        "settings_url",
        "message",
        "guidance",
        "issues",
        "issues_truncated",
        "auth_options",
    }
    assert "**********" not in raw
    assert "__airbyte_deferred_credential__" not in raw
    assert "repositories" not in raw


@pytest.mark.parametrize(
    ("kwargs", "metadata", "expected"),
    [
        pytest.param(
            {"config_secret_name": "SECRET"},
            _metadata("source"),
            ("invalid_config", "correct_nonsecret_config", "invalid_input"),
            id="secret_name",
        ),
        pytest.param(
            {"unique": False},
            _metadata("source"),
            ("invalid_config", "correct_nonsecret_config", "invalid_input"),
            id="not_unique",
        ),
        pytest.param(
            {"config": "[1]"},
            _metadata("source"),
            ("invalid_config", "correct_nonsecret_config", "invalid_input"),
            id="bad_config",
        ),
        pytest.param(
            {"workspace_id": "nope"},
            _metadata("source"),
            ("invalid_config", "correct_nonsecret_config", "invalid_input"),
            id="bad_workspace",
        ),
        pytest.param(
            {},
            None,
            ("not_created", "contact_support", "unknown_connector"),
            id="unknown_connector",
        ),
        pytest.param(
            {},
            _metadata("destination"),
            ("not_created", "contact_support", "unknown_connector"),
            id="wrong_connector_type",
        ),
    ],
)
@responses.activate
def test_mcp_deploy_deferred_refusals(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, Any],
    metadata: ConnectorMetadata | None,
    expected: tuple[str, str, str],
) -> None:
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _: _mcp_client())

    def lookup(name: str, **_: Any) -> ConnectorMetadata:
        if metadata is None:
            raise AirbyteConnectorNotRegisteredError(connector_name=name)
        return metadata

    monkeypatch.setattr(cloud_mcp, "get_connector_metadata", lookup)
    result = _deploy_tool("source", **kwargs)
    assert (result["status"], result["next_action"], result["reason"]) == expected
    assert result["connector_id"] is None
    assert not responses.calls


@responses.activate
def test_mcp_deploy_deferred_requires_workspace_without_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_client", lambda _: _mcp_client(workspace_id=None)
    )
    monkeypatch.setattr(
        cloud_mcp, "get_connector_metadata", lambda *_, **__: _metadata("source")
    )
    result = _deploy_tool("source")
    assert (result["status"], result["next_action"], result["reason"]) == (
        "not_created",
        "choose_workspace",
        "workspace_required",
    )
    assert not responses.calls


@responses.activate
def test_mcp_deploy_deferred_registry_outage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _: _mcp_client())

    def outage(*_: Any, **__: Any) -> ConnectorMetadata:
        raise requests.ConnectionError(CANARY)

    monkeypatch.setattr(cloud_mcp, "get_connector_metadata", outage)
    result = _deploy_tool("source")
    raw = json.dumps(result)
    assert (result["status"], result["reason"]) == (
        "not_created",
        "preflight_unavailable",
    )
    assert CANARY not in raw


@responses.activate
def test_mcp_check_cloud_connector_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _: _mcp_client())
    responses.add(
        responses.GET,
        f"{CLOUD_API_ROOT}/sources/{ACTOR_ID}",
        json=_create_body("source"),
        status=200,
    )
    responses.add(
        responses.POST,
        CHECK_URL,
        json={"status": "failed", "message": CANARY},
        status=200,
    )
    raw = cloud_mcp.check_cloud_connector_setup(
        cast(Context, object()),
        connector_type="source",
        connector_id=ACTOR_ID,
        workspace_id=None,
    )
    result = json.loads(raw)
    assert result == {
        "connector_type": "source",
        "connector_id": ACTOR_ID,
        "workspace_id": WORKSPACE_ID,
        "settings_url": SETTINGS_URL,
        "status": "failed",
        "message": "The saved connector did not pass its check. Review the result in Cloud.",
        "next_action": "complete_in_cloud",
    }
    assert CANARY not in raw
    assert len(_requests_to(CHECK_URL, "POST")) == 1
