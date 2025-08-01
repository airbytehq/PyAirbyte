# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Constants shared across the PyAirbyte codebase."""

from __future__ import annotations

import os
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

DEFAULT_CACHE_ROOT: Path = (
    Path() / ".cache"
    if "AIRBYTE_CACHE_ROOT" not in os.environ
    else Path(os.environ["AIRBYTE_CACHE_ROOT"])
)
"""Default cache root is `.cache` in the current working directory.

The default location can be overridden by setting the `AIRBYTE_CACHE_ROOT` environment variable.

Overriding this can be useful if you always want to store cache files in a specific location.
For example, in ephemeral environments like Google Colab, you might want to store cache files in
your mounted Google Drive by setting this to a path like `/content/drive/MyDrive/Airbyte/cache`.
"""

DEFAULT_ARROW_MAX_CHUNK_SIZE = 100_000
"""The default number of records to include in each batch of an Arrow dataset."""


def _str_to_bool(value: str) -> bool:
    """Convert a string value of an environment values to a boolean value."""
    return bool(value) and value.lower() not in {"", "0", "false", "f", "no", "n", "off"}


TEMP_DIR_OVERRIDE: Path | None = (
    Path(os.environ["AIRBYTE_TEMP_DIR"]) if os.getenv("AIRBYTE_TEMP_DIR") else None
)
"""The directory to use for temporary files.

This value is read from the `AIRBYTE_TEMP_DIR` environment variable. If the variable is not set,
Tempfile will use the system's default temporary directory.

This can be useful if you want to store temporary files in a specific location (or) when you
need your temporary files to exist in user level directories, and not in system level
directories for permissions reasons.
"""

TEMP_FILE_CLEANUP = _str_to_bool(
    os.getenv(
        key="AIRBYTE_TEMP_FILE_CLEANUP",
        default="true",
    )
)
"""Whether to clean up temporary files after use.

This value is read from the `AIRBYTE_TEMP_FILE_CLEANUP` environment variable. If the variable is
not set, the default value is `True`.
"""

AIRBYTE_OFFLINE_MODE = _str_to_bool(
    os.getenv(
        key="AIRBYTE_OFFLINE_MODE",
        default="false",
    )
)
"""Enable or disable offline mode.

When offline mode is enabled, PyAirbyte will attempt to fetch metadata for connectors from the
Airbyte registry but will not raise an error if the registry is unavailable. This can be useful in
environments without internet access or with air-gapped networks.

Offline mode also disables telemetry, similar to a `DO_NOT_TRACK` setting, ensuring no usage data
is sent from your environment. You may also specify a custom registry URL via the`_REGISTRY_ENV_VAR`
environment variable if you prefer to use a different registry source for metadata.

This setting helps you make informed choices about data privacy and operation in restricted and
air-gapped environments.
"""

AIRBYTE_PRINT_FULL_ERROR_LOGS: bool = _str_to_bool(
    os.getenv(
        key="AIRBYTE_PRINT_FULL_ERROR_LOGS",
        default=os.getenv("CI", "false"),
    )
)
"""Whether to print full error logs when an error occurs.
This setting helps in debugging by providing detailed logs when errors occur. This is especially
helpful in ephemeral environments like CI/CD pipelines where log files may not be persisted after
the pipeline run.

If not set, the default value is `False` for non-CI environments.
If running in a CI environment ("CI" env var is set), then the default value is `True`.
"""

NO_UV: bool = os.getenv("AIRBYTE_NO_UV", "").lower() not in {"1", "true", "yes"}
"""Whether to use uv for Python package management.

This value is determined by the `AIRBYTE_NO_UV` environment variable. When `AIRBYTE_NO_UV`
is set to "1", "true", or "yes", uv will be disabled and pip will be used instead.

If the variable is not set or set to any other value, uv will be used by default.
This provides a safe fallback mechanism for environments where uv is not available
or causes issues.
"""

SECRETS_HYDRATION_PREFIX = "secret_reference::"
"""Use this prefix to indicate a secret reference in configuration.

For example, this snippet will populate the `personal_access_token` field with the value of the
secret named `GITHUB_PERSONAL_ACCESS_TOKEN`, for instance from an environment variable.

```json
{
  "credentials": {
    "personal_access_token": "secret_reference::GITHUB_PERSONAL_ACCESS_TOKEN"
  }
}
```

For more information, see the `airbyte.secrets` module documentation.
"""
