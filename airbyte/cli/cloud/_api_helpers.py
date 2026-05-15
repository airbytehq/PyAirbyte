# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""API and serialization helpers for `airbyte cloud` commands."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.cli._cli_auth import (
    resolve_api_url,
    resolve_client_id,
    resolve_client_secret,
    resolve_workspace_id,
)
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    import click
    from airbyte_api import models


def get_auth_context(ctx: click.Context) -> tuple[str, SecretString, SecretString, str]:
    """Resolve and return `(api_url, client_id, client_secret, workspace_id)`."""
    api_url = resolve_api_url(ctx.obj["_raw_api_url"])
    client_id = SecretString(resolve_client_id(ctx.obj["_raw_client_id"]))
    client_secret = SecretString(resolve_client_secret(ctx.obj["_raw_client_secret"]))
    workspace_id = resolve_workspace_id(ctx.obj["_raw_workspace_id"])
    return api_url, client_id, client_secret, workspace_id


def get_auth_no_workspace(ctx: click.Context) -> tuple[str, SecretString, SecretString]:
    """Resolve and return auth credentials without requiring `workspace_id`."""
    api_url = resolve_api_url(ctx.obj["_raw_api_url"])
    client_id = SecretString(resolve_client_id(ctx.obj["_raw_client_id"]))
    client_secret = SecretString(resolve_client_secret(ctx.obj["_raw_client_secret"]))
    return api_url, client_id, client_secret


def workspace_to_dict(ws: models.WorkspaceResponse) -> dict[str, object]:
    return {
        "workspace_id": ws.workspace_id,
        "name": ws.name,
    }


def source_to_dict(src: models.SourceResponse) -> dict[str, object]:
    return {
        "source_id": src.source_id,
        "name": src.name,
        "source_type": src.source_type,
    }


def destination_to_dict(dst: models.DestinationResponse) -> dict[str, object]:
    return {
        "destination_id": dst.destination_id,
        "name": dst.name,
        "destination_type": dst.destination_type,
    }


def connection_to_dict(conn: models.ConnectionResponse) -> dict[str, object]:
    return {
        "connection_id": conn.connection_id,
        "name": conn.name,
        "source_id": conn.source_id,
        "destination_id": conn.destination_id,
        "status": str(conn.status) if conn.status else None,
    }


def job_to_dict(job: models.JobResponse) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "status": str(job.status) if job.status else None,
        "job_type": str(job.job_type) if job.job_type else None,
        "start_time": str(job.start_time) if job.start_time else None,
        "bytes_synced": job.bytes_synced,
        "rows_synced": job.rows_synced,
    }
