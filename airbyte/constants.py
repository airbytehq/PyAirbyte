# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Constants shared across the PyAirbyte codebase."""

from __future__ import annotations

import os
import tempfile
import warnings
from functools import lru_cache
from pathlib import Path


DEBUG_MODE = False  # Set to True to enable additional debug logging.


AB_EXTRACTED_AT_COLUMN = "_airbyte_extracted_at"
"""A column that stores the timestamp when the record was extracted."""

AB_META_COLUMN = "_airbyte_meta"
"""A column that stores metadata about the record."""

AB_RAW_ID_COLUMN = "_airbyte_raw_id"
"""A column that stores a unique identifier for each row in the source data.

Note: The interpretation of this column is slightly different from in Airbyte Dv2 destinations.
In Airbyte Dv2 destinations, this column points to a row in a separate 'raw' table. In PyAirbyte,
this column is simply used as a unique identifier for each record as it is received.

PyAirbyte uses ULIDs for this column, which are identifiers that can be sorted by time
received. This allows us to determine the debug the order of records as they are received, even if
the source provides records that are tied or received out of order from the perspective of their
`emitted_at` (`_airbyte_extracted_at`) timestamps.
"""

AB_INTERNAL_COLUMNS = {
    AB_RAW_ID_COLUMN,
    AB_EXTRACTED_AT_COLUMN,
    AB_META_COLUMN,
}
"""A set of internal columns that are reserved for PyAirbyte's internal use."""

DEFAULT_CACHE_SCHEMA_NAME = "airbyte_raw"
"""The default schema name to use for caches.

Specific caches may override this value with a different schema name.
"""

DEFAULT_ARROW_MAX_CHUNK_SIZE = 100_000
"""The default number of records to include in each batch of an Arrow dataset."""


@lru_cache
def _get_logging_root() -> Path | None:
    """Return the root directory for logs.

    Returns `None` if no valid path can be found.

    This is the directory where logs are stored.
    """
    if "AIRBYTE_LOGGING_ROOT" in os.environ:
        log_root = Path(os.environ["AIRBYTE_LOGGING_ROOT"])
    else:
        log_root = Path(tempfile.gettempdir()) / "airbyte" / "logs"

    try:
        # Attempt to create the log root directory if it does not exist
        log_root.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Handle the error by returning None
        warnings.warn(
            (
                f"Failed to create PyAirbyte logging directory at `{log_root}`. "
                "You can override the default path by setting the `AIRBYTE_LOGGING_ROOT` "
                "environment variable."
            ),
            category=UserWarning,
            stacklevel=0,
        )
        return None
    else:
        return log_root


AIRBYTE_LOGGING_ROOT: Path | None = _get_logging_root()
"""The root directory for Airbyte logs.

This value can be overridden by setting the `AIRBYTE_LOGGING_ROOT` environment variable.

If not provided, PyAirbyte will use `/tmp/airbyte/logs/` where `/tmp/` is the OS's default
temporary directory. If the directory cannot be created, PyAirbyte will log a warning and
set this value to `None`.
"""

TEMP_FILE_CLEANUP = bool(
    os.getenv(
        key="AIRBYTE_TEMP_FILE_CLEANUP",
        default="true",
    )
    .lower()
    .replace("false", "")
    .replace("0", "")
)
"""Whether to clean up temporary files after use.

This value is read from the `AIRBYTE_TEMP_FILE_CLEANUP` environment variable. If the variable is
not set, the default value is `True`.
"""
