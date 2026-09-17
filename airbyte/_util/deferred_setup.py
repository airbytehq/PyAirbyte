# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Bounded parsing of the platform's deferred-credential-setup problem response.

The platform refuses an unsafe deferred create with an RFC 7807 problem whose `data` carries
only fixed codes and schema-known JSON pointers. This module validates that body into an
allowlisted model so callers can rebuild guidance from fixed codes without ever forwarding raw
`title`, `detail`, `message` or body text.
"""

from __future__ import annotations

import json
from http import HTTPStatus
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError


DEFERRED_SETUP_PROBLEM_TYPE = (
    "https://reference.airbyte.com/reference/errors#deferred-credential-setup"
)
"""Exact problem `type` emitted by the platform for a refused deferred create."""

DEFERRED_SETUP_PROBLEM_MAX_BYTES = 64 * 1024
"""Largest problem body that is parsed; anything larger is treated as opaque."""

MAX_ISSUES = 20
MAX_AUTH_OPTIONS = 20
MAX_SELECTORS_PER_OPTION = 8
MAX_PATH_LENGTH = 256

DeferredSetupReason = Literal[
    "secret_input_not_allowed",
    "auth_selection_required",
    "unsupported_schema",
    "configuration_invalid",
]
DeferredSetupIssueCode = Literal[
    "omit_secret",
    "select_auth_method",
    "unsupported_schema",
    "required",
    "type",
    "enum",
    "pattern",
    "format",
    "composition",
    "additional_properties",
    "invalid_configuration",
]

_ISSUE_MESSAGES: dict[str, str] = {
    "omit_secret": "Remove this credential; the user supplies it in Airbyte Cloud.",
    "select_auth_method": "Choose one authentication method explicitly.",
    "unsupported_schema": "This connector schema is not supported for deferred setup.",
    "required": "This required non-secret value is missing.",
    "type": "This value has the wrong type.",
    "enum": "This value is not one of the allowed options.",
    "pattern": "This value does not match the required pattern.",
    "format": "This value does not match the required format.",
    "composition": "This value does not match any allowed shape.",
    "additional_properties": "This property is not allowed here.",
    "invalid_configuration": "This value is invalid.",
}


class _Allowlisted(BaseModel):
    """Base model that drops every field the platform contract does not declare."""

    model_config = ConfigDict(extra="ignore", strict=True)


class DeferredSetupIssue(_Allowlisted):
    """A single sanitized issue rebuilt from fixed codes."""

    path: str = Field(max_length=MAX_PATH_LENGTH)
    code: DeferredSetupIssueCode
    message: str = Field(max_length=MAX_PATH_LENGTH)


class DeferredAuthSelector(_Allowlisted):
    """One non-secret selector that, applied together with its siblings, picks an auth method."""

    path: str = Field(max_length=MAX_PATH_LENGTH)
    value: str | int | float | bool


class DeferredAuthOption(_Allowlisted):
    """One complete authentication alternative."""

    selectors: list[DeferredAuthSelector] = Field(min_length=1, max_length=MAX_SELECTORS_PER_OPTION)


class DeferredSetupProblem(_Allowlisted):
    """Validated, allowlisted projection of the platform's refusal."""

    reason: DeferredSetupReason
    issues: list[DeferredSetupIssue] = Field(max_length=MAX_ISSUES)
    issues_truncated: bool = Field(alias="issuesTruncated")
    auth_options: list[DeferredAuthOption] | None = Field(
        default=None,
        alias="authOptions",
        min_length=1,
        max_length=MAX_AUTH_OPTIONS,
    )


class _ProblemEnvelope(_Allowlisted):
    type: str
    status: int
    data: DeferredSetupProblem


def rebuild_issue_message(issue: DeferredSetupIssue) -> DeferredSetupIssue:
    """Return a copy of `issue` whose message is the fixed text for its code."""
    return issue.model_copy(update={"message": _ISSUE_MESSAGES[issue.code]})


def parse_deferred_setup_problem(  # noqa: PLR0911
    *,
    status_code: int,
    body: str | bytes | None,
) -> DeferredSetupProblem | None:
    """Return the allowlisted problem if `body` is a valid deferred-setup refusal.

    Returns `None` for anything else: wrong status, wrong problem type, oversized, malformed or
    non-conforming payloads. Callers must treat `None` as an opaque failure, never as a trusted
    pre-write refusal.
    """
    if status_code != HTTPStatus.UNPROCESSABLE_ENTITY or body is None:
        return None
    raw = body if isinstance(body, bytes) else body.encode("utf-8", errors="strict")
    if len(raw) > DEFERRED_SETUP_PROBLEM_MAX_BYTES:
        return None
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(decoded, dict):
        return None
    try:
        envelope = _ProblemEnvelope.model_validate(decoded)
    except ValidationError:
        return None
    if envelope.type != DEFERRED_SETUP_PROBLEM_TYPE or envelope.status != int(
        HTTPStatus.UNPROCESSABLE_ENTITY
    ):
        return None
    problem = envelope.data
    if (problem.reason == "auth_selection_required") != (problem.auth_options is not None):
        return None
    return problem.model_copy(
        update={"issues": [rebuild_issue_message(issue) for issue in problem.issues]},
    )


# Safe outcomes reported to callers (workspace API and MCP)

ActorType = Literal["source", "destination"]

DeferredSetupStatus = Literal[
    "awaiting_user",
    "created_unconfirmed",
    "outcome_unknown",
    "invalid_config",
    "unsupported",
    "name_conflict",
    "not_created",
]
DeferredSetupNextAction = Literal[
    "complete_in_cloud",
    "inspect_cloud_before_retry",
    "correct_nonsecret_config",
    "choose_auth_method",
    "choose_another_name",
    "verify_access",
    "choose_workspace",
    "retry_later",
    "contact_support",
]
DeferredSetupOutcomeReason = Literal[
    "secret_input_not_allowed",
    "auth_selection_required",
    "unsupported_schema",
    "configuration_invalid",
    "workspace_required",
    "access_denied",
    "preflight_unavailable",
    "invalid_input",
    "unknown_connector",
    "unsupported_api_root",
    "acknowledgment_missing",
    "response_unrecognized",
]

_STATUS_MESSAGES: dict[str, str] = {
    "awaiting_user": (
        "The connector was created with placeholder credentials. A person must open the settings "
        "link in Airbyte Cloud, authenticate or enter the credentials, test and save."
    ),
    "created_unconfirmed": (
        "The connector was created, but Cloud did not confirm deferred-credential mode. A person "
        "must open the settings link, review the configuration, authenticate, test and save."
    ),
    "outcome_unknown": (
        "The create request did not complete with a trusted result. A connector may or may not "
        "exist; inspect the workspace in Airbyte Cloud before trying again."
    ),
    "invalid_config": "The non-secret configuration was refused before anything was created.",
    "unsupported": (
        "This connector schema is not supported for deferred setup; nothing was created."
    ),
    "name_conflict": "A connector with this exact name already exists; nothing was created.",
    "not_created": "The connector was not created.",
}
_REASON_MESSAGES: dict[str, str] = {
    "workspace_required": "No workspace is configured; supply a workspace ID.",
    "access_denied": "The Cloud API rejected the credentials for this operation.",
    "preflight_unavailable": "A preflight request to Airbyte Cloud did not complete.",
    "invalid_input": "The request arguments were invalid.",
    "unknown_connector": "The connector name is not in the Airbyte connector registry.",
    "unsupported_api_root": "Cloud settings links are only available for Airbyte Cloud API roots.",
}
_NEXT_ACTION_GUIDANCE: dict[str, str] = {
    "complete_in_cloud": (
        "Give the person the settings URL and wait. After they say they saved, call "
        "`check_cloud_connector_setup` with this actor ID; do not use, check or recreate the "
        "connector until that check succeeds."
    ),
    "inspect_cloud_before_retry": (
        "Do not retry automatically. Ask the person to inspect the workspace in Airbyte Cloud "
        "for a connector with this name before deciding whether to create it again."
    ),
    "correct_nonsecret_config": (
        "Correct the listed non-secret configuration issues and call the tool again. Never add "
        "credentials, secrets or secret references."
    ),
    "choose_auth_method": (
        "Pick exactly one of the listed authentication options, set every selector it lists in "
        "the configuration, and call the tool again."
    ),
    "choose_another_name": "Choose a different, unused connector name and call the tool again.",
    "verify_access": "Verify the Cloud API credentials and workspace permissions.",
    "choose_workspace": "Provide a workspace ID or configure a default workspace.",
    "retry_later": "Airbyte Cloud or the connector registry could not be reached; try again later.",
    "contact_support": "This case cannot be completed through deferred setup; contact support.",
}


class DeferredSetupOutcome(BaseModel):
    """Safe result of a deferred-credential create attempt.

    Carries only fixed codes, canonical UUIDs and the allowlisted platform problem; never raw
    responses, configuration, names or provider text.
    """

    model_config = ConfigDict(frozen=True)

    status: DeferredSetupStatus
    next_action: DeferredSetupNextAction
    reason: DeferredSetupOutcomeReason | None = None
    actor_id: str | None = None
    """Canonical actor UUID, present only when trusted create evidence identified the actor."""
    problem: DeferredSetupProblem | None = None
    """The platform's allowlisted refusal, when the create was refused before persistence."""


def outcome_message(outcome: DeferredSetupOutcome) -> str:
    """Fixed, agent-facing explanation of an outcome (never provider or exception text)."""
    parts = [_STATUS_MESSAGES[outcome.status]]
    if outcome.reason is not None and outcome.reason in _REASON_MESSAGES:
        parts.append(_REASON_MESSAGES[outcome.reason])
    return " ".join(parts)


def next_action_guidance(next_action: DeferredSetupNextAction) -> str:
    """Fixed workflow instruction for the agent."""
    return _NEXT_ACTION_GUIDANCE[next_action]


# Safe completion check

SetupCheckStatus = Literal["succeeded", "failed", "unknown", "not_accessible"]
SetupCheckNextAction = Literal[
    "continue",
    "complete_in_cloud",
    "inspect_cloud_before_retry",
    "verify_workspace",
    "verify_access",
]


class SetupCheckOutcome(BaseModel):
    """Safe result of a completion check on a saved connector.

    `actor_id`/`workspace_id` are present only when they were valid UUIDs; `settings_url` only
    after the actor's workspace ownership was verified. No provider or internal text is carried.
    """

    model_config = ConfigDict(frozen=True)

    actor_type: ActorType
    actor_id: str | None = None
    workspace_id: str | None = None
    settings_url: str | None = None
    status: SetupCheckStatus
    message: str
    next_action: SetupCheckNextAction


CHECK_MESSAGES: dict[str, str] = {
    "invalid_ids": "Supply a valid connector ID and workspace ID.",
    "not_in_workspace": "The connector could not be accessed in this workspace.",
    "not_verified": "The connector could not be verified, so no check was started.",
    "access": "The Cloud API credentials could not perform the check.",
    "not_started": "The check could not be started. Review the connector in Cloud.",
    "succeeded": "The saved connector passed a connection check.",
    "failed": "The saved connector did not pass its check. Review the result in Cloud.",
    "unknown": (
        "The check result is unknown. Cloud may still be checking; review it before trying again."
    ),
}
