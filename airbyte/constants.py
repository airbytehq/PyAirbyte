# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""True constants shared across PyAirbyte.

For environment-backed configuration, see `airbyte.settings.AirbyteSettings`.
"""

from __future__ import annotations


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

DEFAULT_GOOGLE_DRIVE_MOUNT_PATH = "/content/drive"
"""Default path to mount Google Drive in Google Colab environments."""

DEFAULT_ARROW_MAX_CHUNK_SIZE = 100_000
"""The default number of records to include in each batch of an Arrow dataset."""

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


# Cloud Constants

CLOUD_CLIENT_ID_ENV_VAR: str = "AIRBYTE_CLOUD_CLIENT_ID"
"""The environment variable name for the Airbyte Cloud client ID."""

CLOUD_CLIENT_SECRET_ENV_VAR: str = "AIRBYTE_CLOUD_CLIENT_SECRET"
"""The environment variable name for the Airbyte Cloud client secret."""

CLOUD_API_ROOT_ENV_VAR: str = "AIRBYTE_CLOUD_API_URL"
"""The environment variable name for the Airbyte Cloud API URL."""

CLOUD_CONFIG_API_ROOT_ENV_VAR: str = "AIRBYTE_CLOUD_CONFIG_API_URL"
"""The environment variable name for the Airbyte Cloud Config API URL.

The Config API is a separate internal API used for certain operations like
connector builder projects and custom source definitions. This environment
variable allows overriding the default Config API URL, which is useful when
the public API URL has been overridden and the Config API cannot be derived
from it automatically.
"""

CLOUD_WORKSPACE_ID_ENV_VAR: str = "AIRBYTE_CLOUD_WORKSPACE_ID"
"""The environment variable name for the Airbyte Cloud workspace ID."""

CLOUD_ORGANIZATION_ID_ENV_VAR: str = "AIRBYTE_CLOUD_ORGANIZATION_ID"
"""The environment variable name for the Airbyte Cloud organization ID."""

CLOUD_BEARER_TOKEN_ENV_VAR: str = "AIRBYTE_CLOUD_BEARER_TOKEN"
"""The environment variable name for the Airbyte Cloud bearer token.

When set, this bearer token will be used for authentication instead of
client credentials (client_id + client_secret). This is useful when you
already have a valid bearer token and want to skip the OAuth2 token exchange.
"""

CLOUD_API_ROOT: str = "https://api.airbyte.com/v1"
"""The Airbyte Cloud API root URL.

This is the root URL for the Airbyte Cloud API. It is used to interact with the Airbyte Cloud API
and is the default API root for the `CloudWorkspace` class.
- https://reference.airbyte.com/reference/getting-started
"""

CLOUD_CONFIG_API_ROOT: str = "https://cloud.airbyte.com/api/v1"
"""Internal-Use API Root, aka Airbyte "Config API".

Documentation:
- https://docs.airbyte.com/api-documentation#configuration-api-deprecated
- https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml
"""
