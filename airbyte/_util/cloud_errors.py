# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Bounded parsing of Cloud problem messages and validated reason labels."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, NoReturn


_PROBLEM_SLUG = re.compile(r"[a-z0-9][a-z0-9:._-]{0,99}")
# The last path segment of the generic problem `type` URL; `title` is the slug then.
_GENERIC_PROBLEM_SLUG = "errors"
# A slug names a kind of problem; one that is or holds an ID names a resource.
_ID_LIKE = re.compile(r"\d+|.*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*")
_MAX_PROBLEM_BODY = 65536


def is_valid_problem_slug(value: object) -> bool:
    """Return whether a problem slug is safe to export."""
    return isinstance(value, str) and bool(
        _PROBLEM_SLUG.fullmatch(value) and not _ID_LIKE.fullmatch(value)
    )


@dataclass(frozen=True)
class CloudErrorInfo:
    """Cloud-owned message, transport status, and independently validated reason."""

    status_code: int | None
    slug: str | None
    message: str
    is_actor_not_ready: bool


def _problem_slug(problem: dict[str, Any]) -> str | None:
    """Return the problem slug: the last `type` segment; `title` only for the generic type.

    A `type` whose slug is rejected (ID-like, or failing the slug charset) gives
    `None` rather than a `title` read.
    """

    def slug_of(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        slug = re.split(r"[/#]", value)[-1]
        if is_valid_problem_slug(slug):
            return slug
        return None

    problem_type = problem.get("type")
    if not isinstance(problem_type, str):
        return None
    slug = slug_of(problem_type)
    if slug is None:
        return None
    if slug == _GENERIC_PROBLEM_SLUG:
        return slug_of(problem.get("title"))
    return slug


def _problem_message(payload: dict[str, Any]) -> str | None:
    for field in ("detail", "title"):
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _status_fallback_message(status_code: int | None) -> str:
    if status_code == HTTPStatus.TOO_MANY_REQUESTS:
        return "Airbyte Cloud returned HTTP 429 (rate limited)."
    if status_code is not None:
        return f"Airbyte Cloud returned HTTP {status_code}."
    return "Airbyte Cloud request failed."


def _reject_json_constant(value: str) -> NoReturn:
    raise json.JSONDecodeError("Invalid JSON constant", value, 0)


def parse_cloud_error(status_code: int | None, body: str | None) -> CloudErrorInfo:
    """Select detail/title from a JSON object of at most 64 KiB, or a status fallback."""
    payload: Any = None
    if isinstance(body, str) and len(body) <= _MAX_PROBLEM_BODY:
        try:
            if len(body.encode("utf-8")) <= _MAX_PROBLEM_BODY:
                # Numeric fields are unused; avoid Python's integer-conversion size limit.
                payload = json.loads(
                    body, parse_int=lambda _: None, parse_constant=_reject_json_constant
                )
        except (UnicodeEncodeError, json.JSONDecodeError, RecursionError):
            pass
    problem = payload if isinstance(payload, dict) else {}
    problem_type = problem.get("type")
    return CloudErrorInfo(
        status_code=status_code,
        slug=_problem_slug(problem),
        message=_problem_message(problem) or _status_fallback_message(status_code),
        is_actor_not_ready=status_code == HTTPStatus.CONFLICT
        and (
            problem.get("title") == "actor-not-ready"
            or (isinstance(problem_type, str) and problem_type.endswith("409-actor-not-ready"))
        ),
    )
