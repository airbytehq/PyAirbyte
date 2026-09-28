# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Bounded, versioned argument pseudonyms; never an anonymity guarantee."""

from __future__ import annotations

import hashlib
import hmac
import json
import math


_DOMAIN = "airbyte.mcp.args_digest/v1"
_MAX_DEPTH = 32
_MAX_NODES = 4096
_MAX_STRING_POINTS = 65536
_MAX_BYTES = 65536
_MIN_INT = -(2**63)
_MAX_INT = 2**63 - 1


def _validate(envelope: object) -> None:
    nodes = 0
    string_points = 0
    ancestors: set[int] = set()

    def visit(value: object, depth: int) -> None:  # noqa: PLR0912
        nonlocal nodes, string_points
        nodes += 1
        if depth > _MAX_DEPTH or nodes > _MAX_NODES:
            raise ValueError
        if value is None or type(value) is bool:
            return
        if type(value) is str:
            string_points += len(value)
            if string_points > _MAX_STRING_POINTS:
                raise ValueError
            value.encode("utf-8")
        elif type(value) is int:
            if not _MIN_INT <= value <= _MAX_INT:
                raise ValueError
        elif type(value) is float:
            if not math.isfinite(value):
                raise ValueError
        elif type(value) is dict or type(value) is list:
            identity = id(value)
            if identity in ancestors:
                raise ValueError
            ancestors.add(identity)
            if type(value) is dict:
                if nodes + 2 * len(value) > _MAX_NODES:
                    raise ValueError
                for key, item in value.items():
                    if type(key) is not str:
                        raise TypeError
                    visit(key, depth + 1)
                    visit(item, depth + 1)
            else:
                if nodes + len(value) > _MAX_NODES:
                    raise ValueError
                for item in value:
                    visit(item, depth + 1)
            ancestors.remove(identity)
        else:
            raise TypeError

    visit(envelope, 0)


def args_digest(tool_name: str, arguments: object, key: bytes) -> str | None:
    """Return a keyed equality fingerprint, or omit on any validation/hash failure.

    v1 uses Python JSON numeric representation, not RFC 8785. The full envelope is
    validated before encoding so encoder allocations are bounded.
    Failures are deliberately silent: exceptions can contain argument/key data.
    """
    try:
        if not key or type(tool_name) is not str:
            return None
        arguments = {} if arguments is None else arguments
        if type(arguments) is not dict:
            return None
        envelope = [_DOMAIN, tool_name, arguments]
        _validate(envelope)
        encoder = json.JSONEncoder(
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
        )
        digest = hmac.new(key, digestmod=hashlib.sha256)
        size = 0
        for chunk in encoder.iterencode(envelope):
            encoded = chunk.encode("utf-8")
            size += len(encoded)
            if size > _MAX_BYTES:
                return None
            digest.update(encoded)
        return digest.hexdigest()[:32]
    except Exception:
        return None
