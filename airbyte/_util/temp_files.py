# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal helper functions for working with temporary files."""

from __future__ import annotations

import json
import tempfile
import warnings
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
            use_json = isinstance(content, dict)
            temp_file = tempfile.NamedTemporaryFile(
                mode="w+t",
                delete=False,
                encoding="utf-8",
                suffix=".json" if use_json else ".txt",
            )
            temp_file.write(
                json.dumps(content) if isinstance(content, dict) else content,
            )
            temp_file.flush()
            temp_file.close()
            temp_files.append(temp_file)
        yield [file.name for file in temp_files]
    finally:
        for temp_file in temp_files:
            with suppress(Exception):
                temp_file.close()
            try:
                Path(temp_file.name).unlink(missing_ok=True)
            except Exception as ex:
                # Something went wrong and the file could not be deleted. Warn the user.
                warnings.warn(
                    f"Failed to remove temporary file: '{temp_file.name}'. {ex}",
                    stacklevel=2,
                )
