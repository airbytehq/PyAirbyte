# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Internal filesystem path helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path


logger = logging.getLogger("airbyte")


def _try_create_dir_if_missing(path: Path, desc: str = "specified") -> Path:
    """Try to create a directory if it does not exist."""
    resolved_path = path.expanduser().resolve()
    try:
        if resolved_path.exists():
            if not resolved_path.is_dir():
                logger.warning(
                    "The %s path exists but is not a directory: '%s'", desc, resolved_path
                )
            return resolved_path
        resolved_path.mkdir(parents=True, exist_ok=True)
    except Exception as ex:
        logger.warning(
            "Could not auto-create missing %s directory at '%s': %s", desc, resolved_path, ex
        )
    return resolved_path
