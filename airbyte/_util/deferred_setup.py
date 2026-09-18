# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Parsing of the platform's `deferred-credential-setup` problem response.

The platform refuses an unsafe deferred create with an RFC 7807 problem whose `data` carries
only fixed codes and schema-known JSON pointers. The body is validated into an allowlisted
model whose messages are rebuilt from fixed codes, so callers never forward raw problem text.
"""

from __future__ import annotations

import json
from http import HTTPStatus
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError


DEFERRED_SETUP_PROBLEM_TYPE = (
    "https://reference.airbyte.com/reference/errors#deferred-credential-setup"
)
"""Exact problem `type` emitted by the platform for a refused deferred create."""

DeferredSetupReason = Literal[
    "secret_input_not_allowed",
    "auth_selection_required",
    "configuration_invalid",
]
DeferredSetupIssueCode = Literal[
    "omit_secret",
    "select_auth_method",
    "required",
    "type",
    "enum",
    "pattern",
    "format",
    "composition",
    "additional_properties",
    "invalid_configuration",
]

ISSUE_MESSAGES: dict[str, str] = {
    "omit_secret": "Remove this credential; the user supplies it in Airbyte Cloud.",
    "select_auth_method": "Choose one authentication method explicitly.",
    "required": "This required non-secret value is missing.",
    "type": "This value has the wrong type.",
    "enum": "This value is not one of the allowed options.",
    "pattern": "This value does not match the required pattern.",
    "format": "This value does not match the required format.",
    "composition": "This value does not match any allowed shape.",
    "additional_properties": "This property is not allowed here.",
    "invalid_configuration": "This value is invalid.",
}


_JSON_POINTER_PATTERN = r"^(/[^/]*)*$"
"""RFC 6901 pointer; the only shape the platform emits for issue and selector paths."""

JsonPointer = Annotated[str, Field(max_length=256, pattern=_JSON_POINTER_PATTERN)]


class _Allowlisted(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)


class DeferredSetupIssue(_Allowlisted):
    """A single sanitized issue; `message` is rebuilt from `code`."""

    path: JsonPointer
    code: DeferredSetupIssueCode

    @property
    def message(self) -> str:
        """Fixed, code-derived message."""
        return ISSUE_MESSAGES[self.code]


class DeferredAuthSelector(_Allowlisted):
    """One non-secret `const`/singleton-`enum` value that selects an authentication method."""

    path: Annotated[JsonPointer, Field(min_length=2)]
    value: Annotated[str, Field(max_length=256)] | int | float | bool


class DeferredAuthOption(_Allowlisted):
    """One complete authentication alternative; all selectors are applied together."""

    selectors: list[DeferredAuthSelector] = Field(min_length=1, max_length=20)


class DeferredSetupProblem(_Allowlisted):
    """Allowlisted projection of the platform's refusal."""

    reason: DeferredSetupReason
    issues: list[DeferredSetupIssue] = Field(max_length=20)
    auth_options: list[DeferredAuthOption] = Field(default_factory=list, alias="authOptions")


class _ProblemEnvelope(_Allowlisted):
    type: Literal["https://reference.airbyte.com/reference/errors#deferred-credential-setup"]
    data: DeferredSetupProblem


def parse_deferred_setup_problem(
    *,
    status_code: int,
    body: str | bytes | None,
) -> DeferredSetupProblem | None:
    """Return the platform's deferred-setup refusal, or `None` for any other response."""
    if status_code != HTTPStatus.UNPROCESSABLE_ENTITY or not body:
        return None
    try:
        return _ProblemEnvelope.model_validate(json.loads(body)).data
    except (ValueError, ValidationError):
        return None
