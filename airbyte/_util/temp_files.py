# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal helper functions for working with temporary files."""

from __future__ import annotations

import json
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Generator


@contextmanager
def as_temp_files(files_contents: list[dict | str]) -> Generator[list[str], Any, None]:
    """Write the given contents to temporary files and yield the file paths as strings."""
    temp_files: list[Any] = []
    try:
        for content in files_contents:
            temp_file = tempfile.NamedTemporaryFile(
                mode="w+t",
                delete=False,
                encoding="utf-8",
            )
            temp_file.write(
                json.dumps(content) if isinstance(content, dict) else content,
            )
            temp_file.flush()
            temp_files.append(temp_file)
        yield [file.name for file in temp_files]
    finally:
        for temp_file in temp_files:
            with suppress(Exception):
                Path(temp_file.name).unlink()
