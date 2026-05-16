# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""API and serialization helpers for `airbyte cloud` commands."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import yaml

from airbyte.cli._cli_auth import (
    resolve_api_url,
    resolve_client_id,
    resolve_client_secret,
    resolve_workspace_id,
)
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.sync_results import SyncResult
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from pathlib import Path

    from airbyte_api import models


def get_cloud_workspace(
    *,
    workspace_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    api_url: str | None = None,
) -> CloudWorkspace:
    """Resolve CLI auth and return a cloud workspace."""
    return CloudWorkspace(
        workspace_id=resolve_workspace_id(workspace_id),
        api_root=resolve_api_url(api_url),
        client_id=SecretString(resolve_client_id(client_id)),
        client_secret=SecretString(resolve_client_secret(client_secret)),
    )


def parse_config_options(
    *,
    config_json: str | None = None,
    config_file: Path | None = None,
) -> dict[str, object]:
    """Parse connector configuration from JSON text or a YAML/JSON file."""
    if bool(config_json) == bool(config_file):
        raise PyAirbyteInputError(
            message="Exactly one config input is required.",
            context={"options": "--config-json, --config-file"},
        )

    if config_json:
        parsed_json = json.loads(config_json)
        if not isinstance(parsed_json, dict):
            raise PyAirbyteInputError(message="Config JSON must be an object.")
        return parsed_json

    if not config_file:
        raise PyAirbyteInputError(message="Config file is required.")
    if not config_file.exists():
        raise PyAirbyteInputError(message="Config file does not exist.")
    parsed_file = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    if not isinstance(parsed_file, dict):
        raise PyAirbyteInputError(message="Config file must contain an object.")
    return parsed_file


def parse_csv(value: str | None) -> list[str]:
    """Parse a comma-separated CLI option value."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_entity_id(
    args: tuple[str, ...],
    option_value: str | None,
    *,
    option_name: str,
) -> str:
    """Resolve an entity ID from one positional argument or a named option."""
    if len(args) > 1:
        raise PyAirbyteInputError(message="Only one entity ID argument is allowed.")

    arg_value = args[0] if args else None
    if arg_value and option_value and arg_value != option_value:
        raise PyAirbyteInputError(message="Entity ID arguments must match.")

    entity_id = arg_value or option_value
    if not entity_id:
        raise PyAirbyteInputError(
            message="Entity ID is required.",
            context={"option": option_name},
        )

    return entity_id


def workspace_to_dict(ws: models.WorkspaceResponse) -> dict[str, object]:
    return {
        "workspace_id": ws.workspace_id,
        "name": ws.name,
    }


def source_to_dict(src: models.SourceResponse | CloudSource) -> dict[str, object]:
    if isinstance(src, CloudSource):
        src = src.get_info()
    return {
        "source_id": src.source_id,
        "name": src.name,
        "source_type": src.source_type,
    }


def destination_to_dict(dst: models.DestinationResponse | CloudDestination) -> dict[str, object]:
    if isinstance(dst, CloudDestination):
        dst = dst.get_info()
    return {
        "destination_id": dst.destination_id,
        "name": dst.name,
        "destination_type": dst.destination_type,
    }


def connection_to_dict(conn: models.ConnectionResponse | CloudConnection) -> dict[str, object]:
    if isinstance(conn, CloudConnection):
        conn = conn.get_info()
    return {
        "connection_id": conn.connection_id,
        "name": conn.name,
        "source_id": conn.source_id,
        "destination_id": conn.destination_id,
        "status": str(conn.status) if conn.status else None,
    }


def job_to_dict(job: models.JobResponse | SyncResult) -> dict[str, object]:
    if isinstance(job, SyncResult):
        job = job.get_info()
    return {
        "job_id": job.job_id,
        "status": str(job.status) if job.status else None,
        "job_type": str(job.job_type) if job.job_type else None,
        "start_time": str(job.start_time) if job.start_time else None,
        "bytes_synced": job.bytes_synced,
        "rows_synced": job.rows_synced,
    }


def sync_result_to_dict(sync_result: SyncResult) -> dict[str, object]:
    """Return sync job details without forcing an API refresh."""
    return {
        "job_id": sync_result.job_id,
        "job_url": sync_result.job_url,
    }
