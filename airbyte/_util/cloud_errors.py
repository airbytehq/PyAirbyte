# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Fixed agent-facing messages for Airbyte Cloud problem (RFC 9457) error bodies.

`parse_cloud_error` reads only the allow-listed fields of a Cloud error body,
`describe_cloud_error` maps it to the fixed `(message, guidance)` text stored in
`cloud_errors.yaml`, and `cloud_errors.yaml` is the single source of those
strings. `detail`, `data.message` and every other body field can quote caller
values or server internals, so they are never read for message text.
"""

from __future__ import annotations

import functools
import importlib.resources
import json
import re
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import yaml


_PROBLEM_SLUG = re.compile(r"[a-z0-9][a-z0-9:._-]{0,99}")
# The last path segment of the generic problem `type` URL; `title` is the slug then.
_GENERIC_PROBLEM_SLUG = "errors"
# A slug names a kind of problem; one that is or holds an ID names a resource.
_ID_LIKE = re.compile(r"\d+|.*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*")
_MAX_PROBLEM_BODY = 65536
_RESOURCE_TYPE = re.compile(r"[a-z_-]{1,40}")
_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_PROBLEM_TYPE_PREFIX = "https://reference.airbyte.com/reference/errors"


@dataclass(frozen=True)
class CloudErrorInfo:
    """The allow-listed fields of an Airbyte Cloud error body.

    `detail`, `data.message` and every other field can quote caller values or
    server internals, so they are never read for message text.
    """

    status_code: int | None
    slug: str | None  # Last `type`/`title` segment, like `mcp_tool_error_reason`.
    key: str | None  # Normalized `type` path used to look up the fixed message.
    resource_type: str | None  # `data.resourceType`, a fixed word.
    limit: int | None  # `data.limit`.
    error_id: str | None  # Top-level `errorId` of a non-problem body.


def _problem_slug(problem: dict[str, Any]) -> str | None:
    """Return the problem slug: the last `type` segment; `title` only for the generic type.

    A `type` whose slug is rejected (ID-like, or failing the slug charset) gives
    `None` rather than a `title` read.
    """

    def slug_of(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        slug = re.split(r"[/#]", value)[-1]
        if _PROBLEM_SLUG.fullmatch(slug) and not _ID_LIKE.fullmatch(slug):
            return slug
        return None

    slug = slug_of(problem.get("type"))
    if slug is None:
        if isinstance(problem.get("type"), str):
            return None
        return slug_of(problem.get("title"))
    if slug == _GENERIC_PROBLEM_SLUG:
        return slug_of(problem.get("title"))
    return slug


def _problem_key(problem: dict[str, Any]) -> str | None:
    """Return the normalized `type` path used to look up the fixed message.

    Cloud emits `type` as the bare errors URL (slug in `title`), as the URL
    plus `#<slug>`, or as `error:<path>`; all three normalize to the same key.
    """
    value = problem.get("type")
    key = value if isinstance(value, str) else ""
    if key.startswith(_PROBLEM_TYPE_PREFIX):
        key = key.removeprefix(_PROBLEM_TYPE_PREFIX)
        if key[:1] in {"#", "/"}:
            key = key[1:]
    if key.startswith("error:"):
        key = key.removeprefix("error:")
    if key.startswith("409-"):
        key = key.removeprefix("409-")
    if key in {"", _GENERIC_PROBLEM_SLUG}:
        title = problem.get("title")
        key = title if isinstance(title, str) else ""
    if _ID_LIKE.fullmatch(key):
        return None
    return key or None


def parse_cloud_error(status_code: int | None, body: str | None) -> CloudErrorInfo:
    """Read only the allow-listed fields of a Cloud API error body.

    Bodies over 64 KiB or non-JSON give `slug=None`.
    """
    slug = key = resource_type = error_id = None
    limit = None
    if body is not None and len(body) <= _MAX_PROBLEM_BODY:
        try:
            problem = json.loads(body)
        except (ValueError, RecursionError):
            problem = None
        if isinstance(problem, dict):
            slug = _problem_slug(problem)
            key = _problem_key(problem)
            data = problem.get("data")
            if isinstance(data, dict):
                candidate = data.get("resourceType")
                if isinstance(candidate, str) and _RESOURCE_TYPE.fullmatch(candidate):
                    resource_type = candidate
                limit_value = data.get("limit")
                if isinstance(limit_value, int) and not isinstance(limit_value, bool):
                    limit = limit_value
            candidate = problem.get("errorId")
            if isinstance(candidate, str) and _UUID.fullmatch(candidate):
                error_id = candidate
    return CloudErrorInfo(
        status_code=status_code,
        slug=slug,
        key=key,
        resource_type=resource_type,
        limit=limit,
        error_id=error_id,
    )


@functools.cache
def _messages() -> dict[str, Any]:
    """Return the fixed error text table from `cloud_errors.yaml`."""
    path = importlib.resources.files("airbyte._util").joinpath("cloud_errors.yaml")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _status_fallback_message(status_code: int | None) -> tuple[str, str]:
    """Return (message, guidance) for a status without a known problem key."""
    fallbacks = _messages()["status_fallbacks"]
    if status_code in fallbacks:
        entry = fallbacks[status_code]
    elif status_code is None:
        entry = fallbacks["default"]
    elif status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
        entry = fallbacks["4xx"]
    else:
        entry = fallbacks["5xx"]
    return entry["message"], entry["guidance"]


def describe_cloud_error(problem: CloudErrorInfo) -> tuple[str, str]:
    """Return (message, guidance) for a parsed Cloud problem."""
    entry = _messages().get(problem.key or "")
    if entry is None:
        message, guidance = _status_fallback_message(problem.status_code)
    else:
        message, guidance = entry["message"], entry["guidance"]
    # `key` is raw `type`/`title` text and can quote caller input, so it is
    # shown only when it matched a fixed message row.
    known_key = entry is not None
    if problem.key == "resource-not-found" and problem.resource_type:
        message = f"The {problem.resource_type} was not found."
    elif problem.key == "workspace-limit-for-organization-reached" and problem.limit is not None:
        message += f" (limit: {problem.limit} workspaces)"
    if problem.status_code is not None:
        message += (
            f" (Cloud error: {problem.key}, HTTP {problem.status_code})"
            if known_key
            else f" (HTTP {problem.status_code})"
        )
    if (
        problem.status_code is not None
        and problem.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
        and problem.error_id
    ):
        guidance += f" Error ID: {problem.error_id}."
    return message, guidance
