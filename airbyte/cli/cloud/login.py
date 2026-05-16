# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""`airbyte cloud login` command."""

from __future__ import annotations

from airbyte.cli._input import (  # noqa: TC001  # Cyclopts resolves aliases at runtime.
    AirbyteApiRootArg,
    ClientIdArg,
    ClientSecretArg,
    ConfigApiRootArg,
)
from airbyte.cli._output import json_output
from airbyte.cli.cloud._cli import cloud_app
from airbyte.cloud.credentials import login_with_client_credentials


@cloud_app.command
def login(
    *,
    client_id: ClientIdArg = None,
    client_secret: ClientSecretArg = None,
    airbyte_api_root: AirbyteApiRootArg = None,
    config_api_root: ConfigApiRootArg = None,
) -> None:
    """Log in to Airbyte Cloud or a self-managed Airbyte server.

    Providing `--client-id` and `--client-secret` performs non-interactive login and
    stores a bearer token in `~/.airbyte/credentials`. Self-managed servers must also
    provide `--airbyte-api-root` and `--config-api-root`.
    """
    result = login_with_client_credentials(
        client_id=client_id,
        client_secret=client_secret,
        airbyte_api_root=airbyte_api_root,
        config_api_root=config_api_root,
    )
    json_output(
        {
            "credentials_file": str(result.credentials_file_path),
            "airbyte_api_root": result.airbyte_api_root,
            "config_api_root": result.config_api_root,
        }
    )
