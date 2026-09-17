# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Deferred-credential connector setup and the safe completion check.

An agent supplies only non-secret configuration; the platform stores a placeholder for each
required credential and a person completes authentication in Airbyte Cloud. Everything here
runs over a dedicated `DeferredSetupSession` (fixed timeouts, no retries, no redirects, one
create) and reports only fixed outcome codes plus canonical UUIDs. Raw responses, exception
text, configuration and provider messages never leave this module.
"""

from __future__ import annotations

import json
import uuid
from http import HTTPStatus
from typing import Any

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte._util.api_util import (
    CLOUD_API_ROOT,
    DeferredSetupSession,
    TypedCreateResult,
    actor_url,
    get_web_url_root,
)
from airbyte._util.deferred_setup import (
    CHECK_MESSAGES,
    ActorType,
    DeferredSetupOutcome,
    DeferredSetupProblem,
    SetupCheckOutcome,
)
from airbyte.constants import SECRETS_HYDRATION_PREFIX
from airbyte.secrets.base import SecretString


_ACTOR_TYPE_KEYS: frozenset[str] = frozenset({"sourceType", "destinationType"})
_CONFIG_MAX_DEPTH = 32
_AUTHORITATIVE_ACCESS_STATUSES = frozenset({HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN})
_NOT_ACCESSIBLE_STATUSES = _AUTHORITATIVE_ACCESS_STATUSES | {HTTPStatus.NOT_FOUND}
_TOKEN_PATH = "/applications/token"


def canonical_uuid(value: object) -> str | None:
    """Canonical lowercase hyphenated form of a UUID string, or `None` when it is not one."""
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError:
        return None


def normalize_deferred_config(config: dict[str, Any] | str | None) -> dict[str, Any]:
    """Normalize agent-supplied non-secret configuration for a deferred create.

    `None` becomes `{}`; a JSON string must parse to an object. Anything else (including the
    string `"null"`, arrays, scalars and file paths) is refused, as is a configuration that
    names a connector type (`sourceType` or `destinationType`, for either actor type) or carries
    a secret reference or `SecretString` at any depth.
    """
    if config is None:
        return {}
    parsed: Any = config
    if isinstance(config, str):
        try:
            parsed = json.loads(config)
        except ValueError as ex:
            raise exc.PyAirbyteInputError(
                message="Deferred configuration must be a JSON object.",
            ) from ex
    if not isinstance(parsed, dict):
        raise exc.PyAirbyteInputError(
            message="Deferred configuration must be a JSON object.",
        )
    present_type_keys = sorted(_ACTOR_TYPE_KEYS & parsed.keys())
    if present_type_keys:
        raise exc.PyAirbyteInputError(
            message=f"Deferred configuration must not contain `{present_type_keys[0]}`.",
            guidance="The connector type comes from the definition ID, not the configuration.",
        )
    _reject_secret_values(parsed, depth=0)
    return parsed


def _reject_secret_values(node: object, *, depth: int) -> None:
    """Refuse secret references and `SecretString` values anywhere in the configuration."""
    if depth > _CONFIG_MAX_DEPTH:
        raise exc.PyAirbyteInputError(message="Deferred configuration is nested too deeply.")
    if isinstance(node, str):
        if isinstance(node, SecretString) or node.startswith(SECRETS_HYDRATION_PREFIX):
            raise exc.PyAirbyteInputError(
                message="Deferred configuration must not contain secrets or secret references.",
                guidance="Omit credentials; the user supplies them in Airbyte Cloud.",
            )
        return
    if isinstance(node, dict):
        for key, value in node.items():
            _reject_secret_values(key, depth=depth + 1)
            _reject_secret_values(value, depth=depth + 1)
    elif isinstance(node, list):
        for value in node:
            _reject_secret_values(value, depth=depth + 1)


def settings_url(api_root: str, workspace_id: str, actor_type: ActorType, actor_id: str) -> str:
    """Cloud settings page for a saved connector; only defined for the Airbyte Cloud API root."""
    return f"{get_web_url_root(api_root)}/workspaces/{workspace_id}/{actor_type}/{actor_id}"


def _list_by_name(
    *,
    actor_type: ActorType,
    name: str,
    workspace_id: str,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    session: DeferredSetupSession,
) -> int:
    lister = api_util.list_sources if actor_type == "source" else api_util.list_destinations
    return len(
        lister(
            workspace_id=workspace_id,
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            name=name,
            http_session=session,
        )
    )


def _create(  # noqa: PLR0913
    *,
    actor_type: ActorType,
    name: str,
    config: dict[str, Any],
    definition_id: str,
    workspace_id: str,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    session: DeferredSetupSession,
) -> TypedCreateResult:
    creator = api_util.create_source if actor_type == "source" else api_util.create_destination
    return api_util.typed_create_result(
        creator(
            name,
            workspace_id=workspace_id,
            config=config,
            definition_id=definition_id,
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            defer_credentials=True,
            http_session=session,
        )
    )


def _typed_result_confirms(
    typed: TypedCreateResult,
    *,
    actor_type: ActorType,
    actor_id: str,
    workspace_id: str,
    definition_id: str,
) -> bool:
    return (
        typed.actor_type == actor_type
        and typed.credentials_deferred
        and canonical_uuid(typed.actor_id) == actor_id
        and canonical_uuid(typed.workspace_id) == workspace_id
        and canonical_uuid(typed.definition_id) == definition_id
    )


def _refusal_outcome(problem: DeferredSetupProblem) -> DeferredSetupOutcome:
    if problem.reason == "auth_selection_required":
        return DeferredSetupOutcome(
            status="invalid_config",
            next_action="choose_auth_method",
            reason=problem.reason,
            problem=problem,
        )
    if problem.reason == "unsupported_schema":
        return DeferredSetupOutcome(
            status="unsupported",
            next_action="contact_support",
            reason=problem.reason,
            problem=problem,
        )
    return DeferredSetupOutcome(
        status="invalid_config",
        next_action="correct_nonsecret_config",
        reason=problem.reason,
        problem=problem,
    )


def _preflight_failure(session: DeferredSetupSession) -> DeferredSetupOutcome:
    if session.statuses.get("token") in _AUTHORITATIVE_ACCESS_STATUSES or (
        session.statuses.get("list") in _AUTHORITATIVE_ACCESS_STATUSES
    ):
        return DeferredSetupOutcome(
            status="not_created", next_action="verify_access", reason="access_denied"
        )
    return DeferredSetupOutcome(
        status="not_created", next_action="retry_later", reason="preflight_unavailable"
    )


def deploy_deferred(  # noqa: PLR0911, PLR0913
    *,
    actor_type: ActorType,
    name: str,
    config: dict[str, Any],
    definition_id: str,
    workspace_id: str,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> DeferredSetupOutcome:
    """Create a connector with deferred credentials and report a safe outcome.

    Preflight (exact-name uniqueness) and the single create run on one dedicated session that
    is closed afterwards. Failures are mapped by stage from recorded statuses, never from
    exception text. The create is never retried: once the POST was handed to the transport
    the outcome is decided from the raw identity/acknowledgment evidence alone.
    """
    canonical_workspace_id = canonical_uuid(workspace_id)
    canonical_definition_id = canonical_uuid(definition_id)
    if canonical_workspace_id is None or canonical_definition_id is None:
        return DeferredSetupOutcome(
            status="invalid_config", next_action="correct_nonsecret_config", reason="invalid_input"
        )
    api_root = api_root.rstrip("/")
    if api_root != CLOUD_API_ROOT:
        return DeferredSetupOutcome(
            status="not_created", next_action="contact_support", reason="unsupported_api_root"
        )

    credentials: dict[str, Any] = {
        "api_root": api_root,
        "client_id": client_id,
        "client_secret": client_secret,
        "bearer_token": bearer_token,
    }
    collection_url = actor_url(api_root, actor_type)
    with DeferredSetupSession() as session:
        session.watch("token", method="POST", url=f"{api_root}{_TOKEN_PATH}")
        session.watch("list", method="GET", url=collection_url)
        try:
            existing = _list_by_name(
                actor_type=actor_type,
                name=name,
                workspace_id=canonical_workspace_id,
                session=session,
                **credentials,
            )
        except Exception:  # SDK token hook raises bare Exception; mapped by status.
            return _preflight_failure(session)
        if existing:
            return DeferredSetupOutcome(status="name_conflict", next_action="choose_another_name")

        session.expect_create(
            url=collection_url,
            actor_type=actor_type,
            workspace_id=canonical_workspace_id,
            definition_id=canonical_definition_id,
        )
        typed: TypedCreateResult | None = None
        try:
            typed = _create(
                actor_type=actor_type,
                name=name,
                config=config,
                definition_id=canonical_definition_id,
                workspace_id=canonical_workspace_id,
                session=session,
                **credentials,
            )
        except exc.AirbyteDeferredSetupError as ex:
            if ex.problem is not None:
                return _refusal_outcome(ex.problem)
        except Exception:  # Once the POST may be in flight, only evidence decides.
            typed = None

        if not session.create_sent:
            return _preflight_failure(session)
        if session.statuses.get("create") in _AUTHORITATIVE_ACCESS_STATUSES:
            return DeferredSetupOutcome(
                status="not_created", next_action="verify_access", reason="access_denied"
            )
        evidence = session.evidence

    if evidence is None or evidence.actor_id is None:
        return DeferredSetupOutcome(
            status="outcome_unknown",
            next_action="inspect_cloud_before_retry",
            reason="response_unrecognized",
        )
    typed_ok = typed is not None and _typed_result_confirms(
        typed,
        actor_type=actor_type,
        actor_id=evidence.actor_id,
        workspace_id=canonical_workspace_id,
        definition_id=canonical_definition_id,
    )
    if evidence.acknowledged and typed_ok:
        return DeferredSetupOutcome(
            status="awaiting_user", next_action="complete_in_cloud", actor_id=evidence.actor_id
        )
    return DeferredSetupOutcome(
        status="created_unconfirmed",
        next_action="complete_in_cloud",
        reason="acknowledgment_missing",
        actor_id=evidence.actor_id,
    )


def _check_outcome(
    *,
    actor_type: ActorType,
    actor_id: str | None,
    workspace_id: str | None,
    status: str,
    message_key: str,
    next_action: str,
    settings: str | None = None,
) -> SetupCheckOutcome:
    return SetupCheckOutcome.model_validate(
        {
            "actor_type": actor_type,
            "actor_id": actor_id,
            "workspace_id": workspace_id,
            "settings_url": settings,
            "status": status,
            "message": CHECK_MESSAGES[message_key],
            "next_action": next_action,
        }
    )


def check_connector_setup(  # noqa: PLR0911
    *,
    actor_type: ActorType,
    actor_id: str,
    workspace_id: str | None,
    api_root: str,
    config_api_root: str | None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> SetupCheckOutcome:
    """Run one connection check on a saved connector after a person completed setup in Cloud.

    Both IDs are validated locally first. The actor's metadata is fetched and its workspace
    compared with the requested one before the single check POST is sent; missing, foreign,
    unauthorized or malformed actors never reach the check endpoint. Nothing is retried or
    polled, and the result carries fixed messages only.
    """
    maybe_actor_id = canonical_uuid(actor_id)
    maybe_workspace_id = canonical_uuid(workspace_id)
    if maybe_actor_id is None or maybe_workspace_id is None:
        return _check_outcome(
            actor_type=actor_type,
            actor_id=maybe_actor_id,
            workspace_id=maybe_workspace_id,
            status="not_accessible",
            message_key="invalid_ids",
            next_action="verify_workspace",
        )
    canonical_actor_id: str = maybe_actor_id
    canonical_workspace_id: str = maybe_workspace_id
    api_root = api_root.rstrip("/")

    def outcome(
        status: str, message_key: str, next_action: str, *, verified: bool = False
    ) -> SetupCheckOutcome:
        return _check_outcome(
            actor_type=actor_type,
            actor_id=canonical_actor_id,
            workspace_id=canonical_workspace_id,
            status=status,
            message_key=message_key,
            next_action=next_action,
            settings=(
                settings_url(api_root, canonical_workspace_id, actor_type, canonical_actor_id)
                if verified and api_root == CLOUD_API_ROOT
                else None
            ),
        )

    credentials: dict[str, Any] = {
        "api_root": api_root,
        "client_id": client_id,
        "client_secret": client_secret,
        "bearer_token": bearer_token,
    }
    getter = api_util.get_source if actor_type == "source" else api_util.get_destination
    check_url = api_util.get_config_api_root(
        api_root, config_api_root=config_api_root
    ) + api_util.check_connection_path(actor_type)
    with DeferredSetupSession() as session:
        session.watch("token", method="POST", url=f"{api_root}{_TOKEN_PATH}")
        session.watch(
            "metadata", method="GET", url=actor_url(api_root, actor_type, canonical_actor_id)
        )
        session.watch("check", method="POST", url=check_url)
        try:
            actor = getter(canonical_actor_id, http_session=session, **credentials)
        except Exception:  # SDK token hook raises bare Exception; mapped by status.
            if session.statuses.get("token") in _AUTHORITATIVE_ACCESS_STATUSES:
                return outcome("not_accessible", "access", "verify_access")
            if session.statuses.get("metadata") in _NOT_ACCESSIBLE_STATUSES:
                return outcome("not_accessible", "not_in_workspace", "verify_workspace")
            return outcome("unknown", "not_verified", "verify_workspace")
        if canonical_uuid(actor.workspace_id) != canonical_workspace_id:
            return outcome("not_accessible", "not_in_workspace", "verify_workspace")

        try:
            succeeded, _ = api_util.check_connector(
                actor_id=canonical_actor_id,
                connector_type=actor_type,
                workspace_id=canonical_workspace_id,
                config_api_root=config_api_root,
                http_session=session,
                **credentials,
            )
        except Exception:  # Token/check failures are mapped by recorded status.
            if "check" not in session.entered:
                if session.statuses.get("token") in _AUTHORITATIVE_ACCESS_STATUSES:
                    return outcome("not_accessible", "access", "verify_access", verified=True)
                return outcome(
                    "unknown", "not_started", "inspect_cloud_before_retry", verified=True
                )
            if session.statuses.get("check") in _NOT_ACCESSIBLE_STATUSES:
                return outcome("not_accessible", "access", "verify_access", verified=True)
            return outcome("unknown", "unknown", "inspect_cloud_before_retry", verified=True)

    if succeeded:
        return outcome("succeeded", "succeeded", "continue", verified=True)
    return outcome("failed", "failed", "complete_in_cloud", verified=True)
