# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Secrets management for PyAirbyte.

PyAirbyte provides a secrets management system that allows you to securely store and retrieve
sensitive information. This module provides the secrets functionality.

## Secrets Management

PyAirbyte can auto-import secrets from the following sources:

1. Environment variables.
2. Variables defined in a local `.env` ("Dotenv") file.
3. [Google Colab secrets](https://medium.com/@parthdasawant/how-to-use-secrets-in-google-colab-450c38e3ec75).
4. Manual entry via [`getpass`](https://docs.python.org/3.10/library/getpass.html).

**Note:** You can also build your own secret manager by subclassing the `CustomSecretManager`
implementation. For more information, see the `airbyte.secrets.CustomSecretManager` reference docs.

### Retrieving Secrets

To retrieve a secret, use the `get_secret()` function. For example:

```python
import airbyte as ab

source = ab.get_source("source-github")
source.set_config(
   "credentials": {
      "personal_access_token": ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN"),
   }
)
```

By default, PyAirbyte will search all available secrets sources. The `get_secret()` function also
accepts an optional `sources` argument of specific source names (`SecretSourceEnum`) and/or secret
manager objects to check.

By default, PyAirbyte will prompt the user for any requested secrets that are not provided via other
secret managers. You can disable this prompt by passing `allow_prompt=False` to `get_secret()`.

### Secrets Auto-Discovery

If you have a secret matching an expected name, PyAirbyte will automatically use it. For example, if
you have a secret named `GITHUB_PERSONAL_ACCESS_TOKEN`, PyAirbyte will automatically use it when
configuring the GitHub source.

The naming convention for secrets is as `{CONNECTOR_NAME}_{PROPERTY_NAME}`, for instance
`SNOWFLAKE_PASSWORD` and `BIGQUERY_CREDENTIALS_PATH`.

PyAirbyte will also auto-discover secrets for interop with hosted Airbyte: `AIRBYTE_CLOUD_API_URL`,
`AIRBYTE_CLOUD_API_KEY`, etc.

## Custom Secret Managers

If you need to build your own secret manager, you can subclass the
`airbyte.secrets.CustomSecretManager` class. This allows you to build a custom secret manager that
can be used with the `get_secret()` function, securely storing and retrieving secrets as needed.

## API Reference

_Below are the classes and functions available in the `airbyte.secrets` module._

"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.secrets.base import SecretHandle, SecretManager, SecretSourceEnum, SecretString
from airbyte.secrets.config import disable_secret_source, register_secret_manager
from airbyte.secrets.custom import CustomSecretManager
from airbyte.secrets.env_vars import DotenvSecretManager, EnvVarSecretManager
from airbyte.secrets.google_colab import ColabSecretManager
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from airbyte.secrets.prompt import SecretsPrompt
from airbyte.secrets.util import get_secret


# Submodules imported here for documentation reasons: https://github.com/mitmproxy/pdoc/issues/757
if TYPE_CHECKING:
    # ruff: noqa: TC004  # imports used for more than type checking
    from airbyte.secrets import (
        base,
        config,
        custom,
        env_vars,
        google_colab,
        google_gsm,
        prompt,
        util,
    )


__all__ = [
    # Submodules
    "base",
    "config",
    "custom",
    "env_vars",
    "google_colab",
    "google_gsm",
    "prompt",
    "util",
    # Secret Access
    "get_secret",
    # Secret Classes
    "SecretSourceEnum",
    "SecretString",
    "SecretHandle",
    # Secret Managers
    "SecretManager",
    "EnvVarSecretManager",
    "DotenvSecretManager",
    "ColabSecretManager",
    "SecretsPrompt",
    "CustomSecretManager",
    "GoogleGSMSecretManager",
    # Registration Functions`
    "register_secret_manager",
    "disable_secret_source",
]
