# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Shared telemetry HMAC master-key loader.

**Placeholder owned by the grouping work, to be replaced.** This is the minimal
Phase P stub that argument tracing consumes. The grouping work owns this module
and its final shape; keep changes here to the loader contract below.

`AIRBYTE_MCP_TELEMETRY_HMAC_KEY` is a hosted-only secret: unpadded base64url that
decodes to exactly 32 bytes. Anything else, including the published test key
(32 x `0x01`), yields `None` and one fixed warning that never contains the value.
"""

from __future__ import annotations

import base64
import binascii
import logging
import re
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Mapping


logger = logging.getLogger(__name__)

TELEMETRY_HMAC_KEY_ENV = "AIRBYTE_MCP_TELEMETRY_HMAC_KEY"
_KEY_LENGTH = 32
_TEST_KEY = b"\x01" * _KEY_LENGTH
_BASE64URL_UNPADDED = re.compile(r"[A-Za-z0-9_-]+")
_INVALID_KEY_WARNING = (
    "AIRBYTE_MCP_TELEMETRY_HMAC_KEY is set but invalid; keyed telemetry is disabled"
)


def load_master(environ: Mapping[str, str]) -> bytes | None:
    """Return the 32-byte master key, or `None` when unset or invalid."""
    raw = environ.get(TELEMETRY_HMAC_KEY_ENV)
    if not raw:
        return None
    key = _decode(raw)
    if key is None or key == _TEST_KEY:
        logger.warning(_INVALID_KEY_WARNING)
        return None
    return key


def _decode(raw: str) -> bytes | None:
    if not _BASE64URL_UNPADDED.fullmatch(raw):
        return None
    try:
        key = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
    except (binascii.Error, ValueError):
        return None
    # Reject non-canonical encodings so one key has exactly one accepted spelling.
    if len(key) != _KEY_LENGTH or base64.urlsafe_b64encode(key).rstrip(b"=").decode() != raw:
        return None
    return key
