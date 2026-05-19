# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""`airbyte cloud login` command."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from airbyte.cli._input import (  # noqa: TC001  # Cyclopts resolves aliases at runtime.
    ApiUrlArg,
    ClientIdArg,
    ClientSecretArg,
    ConfigApiRootArg,
    OrganizationIdArg,
)
from airbyte.cli._output import json_output
from airbyte.cli.cloud._cli import cloud_app
from airbyte.cloud.workspaces import CloudClient


@cloud_app.command
def login(
    *,
    interactive: Annotated[
        bool | None,
        Parameter(
            negative="--no-interactive",
            help="Use browser-based interactive login.",
        ),
    ] = None,
    client_id: ClientIdArg = None,
    client_secret: ClientSecretArg = None,
    organization_id: OrganizationIdArg = None,
    public_api_root: ApiUrlArg = None,
    config_api_root: ConfigApiRootArg = None,
) -> None:
    """Log in to Airbyte Cloud or a self-managed Airbyte server.

    Providing `--client-id` and `--client-secret` performs non-interactive login and
    stores a bearer token in `~/.airbyte/credentials`. Self-managed servers must also
    provide `--public-api-root` and `--config-api-root`.
    """
    result = CloudClient.from_explicit_credentials(
        client_id=client_id,
        client_secret=client_secret,
        organization_id=organization_id,
        public_api_root=public_api_root,
        config_api_root=config_api_root,
    ).login(interactive=interactive)
    json_output(
        {
            "credentials_file": str(result.credentials_file_path),
            "airbyte_api_root": result.airbyte_api_root,
            "config_api_root": result.config_api_root,
            "organization_id": result.organization_id,
        }
    )


@cloud_app.command
def logout() -> None:
    """Log out by removing locally stored Airbyte credentials."""
    CloudClient().logout()
    json_output({"logged_out": True})
