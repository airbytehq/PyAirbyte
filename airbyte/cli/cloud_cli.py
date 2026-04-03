# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte CLI — agent-first command-line interface for Airbyte Cloud operations.

Invokable as:

```bash
uvx airbyte --help
airbyte workspaces list
airbyte sources list --workspace-id <id>
airbyte connections sync --workspace-id <id> --json '{"connection_id": "..."}'
```

All commands output structured JSON by default for agent consumption.
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any

import click

from airbyte._util import api_util


if TYPE_CHECKING:
    from airbyte_api import models

from airbyte.cli._cli_auth import (
    resolve_api_url,
    resolve_client_id,
    resolve_client_secret,
    resolve_workspace_id,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json_output(data: list[dict[str, object]] | dict[str, object]) -> None:
    """Print a JSON-serializable object to stdout."""
    click.echo(json.dumps(data, indent=2, default=str))


def _error_json(message: str, **extra: object) -> None:
    """Print an error object to stderr and exit."""
    payload: dict[str, object] = {"error": message, **extra}
    click.echo(json.dumps(payload, indent=2, default=str), err=True)
    sys.exit(1)


def _parse_json_option(raw: str | None) -> dict[str, Any]:
    """Parse a `--json` option value into a dict."""
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        _error_json("--json value must be a JSON object (dict).")
    return parsed


def _describe_output(
    description: str,
    required_params: dict[str, str] | None = None,
    optional_params: dict[str, str] | None = None,
) -> None:
    """Print a `--describe` schema and exit."""
    schema: dict[str, Any] = {
        "description": description,
    }
    if required_params:
        schema["required_params"] = required_params
    if optional_params:
        schema["optional_params"] = optional_params
    _json_output(schema)
    sys.exit(0)


def _get_auth_context(ctx: click.Context) -> tuple[str, SecretString, SecretString, str]:
    """Resolve and return (api_url, client_id, client_secret, workspace_id).

    Credentials are resolved lazily from raw values stored in `ctx.obj`.
    """
    api_url = resolve_api_url(ctx.obj["_raw_api_url"])
    client_id = SecretString(resolve_client_id(ctx.obj["_raw_client_id"]))
    client_secret = SecretString(resolve_client_secret(ctx.obj["_raw_client_secret"]))
    workspace_id = resolve_workspace_id(ctx.obj["_raw_workspace_id"])
    return api_url, client_id, client_secret, workspace_id


def _get_auth_no_workspace(ctx: click.Context) -> tuple[str, SecretString, SecretString]:
    """Resolve and return auth credentials without requiring workspace_id."""
    api_url = resolve_api_url(ctx.obj["_raw_api_url"])
    client_id = SecretString(resolve_client_id(ctx.obj["_raw_client_id"]))
    client_secret = SecretString(resolve_client_secret(ctx.obj["_raw_client_secret"]))
    return api_url, client_id, client_secret


# ---------------------------------------------------------------------------
# Serializers — convert SDK response objects to plain dicts
# ---------------------------------------------------------------------------


def _workspace_to_dict(ws: models.WorkspaceResponse) -> dict[str, object]:
    return {
        "workspace_id": ws.workspace_id,
        "name": ws.name,
    }


def _source_to_dict(src: models.SourceResponse) -> dict[str, object]:
    return {
        "source_id": src.source_id,
        "name": src.name,
        "source_type": src.source_type,
    }


def _destination_to_dict(dst: models.DestinationResponse) -> dict[str, object]:
    return {
        "destination_id": dst.destination_id,
        "name": dst.name,
        "destination_type": dst.destination_type,
    }


def _connection_to_dict(conn: models.ConnectionResponse) -> dict[str, object]:
    return {
        "connection_id": conn.connection_id,
        "name": conn.name,
        "source_id": conn.source_id,
        "destination_id": conn.destination_id,
        "status": str(conn.status) if conn.status else None,
    }


def _job_to_dict(job: models.JobResponse) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "status": str(job.status) if job.status else None,
        "job_type": str(job.job_type) if job.job_type else None,
        "start_time": str(job.start_time) if job.start_time else None,
        "bytes_synced": job.bytes_synced,
        "rows_synced": job.rows_synced,
    }


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------


@click.group()
@click.option("--client-id", envvar="AIRBYTE_CLIENT_ID", default=None, help="Airbyte client ID.")
@click.option(
    "--client-secret", envvar="AIRBYTE_CLIENT_SECRET", default=None, help="Airbyte client secret."
)
@click.option("--workspace-id", default=None, help="Airbyte workspace ID.")
@click.option("--api-url", default=None, help="Airbyte API URL override.")
@click.pass_context
def cli(
    ctx: click.Context,
    client_id: str | None,
    client_secret: str | None,
    workspace_id: str | None,
    api_url: str | None,
) -> None:
    """Airbyte CLI — agent-first interface for Airbyte Cloud.

    Manage workspaces, sources, destinations, connections, and jobs
    via structured JSON commands.

    Authentication is resolved from (in order): CLI flags, env vars
    (`AIRBYTE_CLIENT_ID` / `AIRBYTE_CLIENT_SECRET`), or
    `~/.airbyte/credentials` file.
    """
    ctx.ensure_object(dict)
    # Store raw values — credentials are resolved lazily when subcommands need them.
    # This allows `--describe` to work without any auth configured.
    ctx.obj["_raw_client_id"] = client_id
    ctx.obj["_raw_client_secret"] = client_secret
    ctx.obj["_raw_api_url"] = api_url
    ctx.obj["_raw_workspace_id"] = workspace_id


# ---------------------------------------------------------------------------
# Workspaces
# ---------------------------------------------------------------------------


@cli.group()
@click.pass_context
def workspaces(ctx: click.Context) -> None:
    """Manage Airbyte workspaces."""
    pass


@workspaces.command("list")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def workspaces_list(ctx: click.Context, describe: bool) -> None:  # noqa: FBT001
    """List workspaces accessible with the current credentials."""
    if describe:
        _describe_output(
            description="List all workspaces accessible with the current credentials.",
            optional_params={"workspace_id": "Filter to a specific workspace ID."},
        )
    api_url, client_id, client_secret = _get_auth_no_workspace(ctx)
    raw_ws: str | None = ctx.obj["_raw_workspace_id"]
    workspace_id: str | None = resolve_workspace_id(raw_ws) if raw_ws else None
    if workspace_id is None:
        _error_json(
            "workspace_id is required. Provide --workspace-id, set AIRBYTE_WORKSPACE_ID, "
            "or add workspace_id to ~/.airbyte/credentials.",
            type="MissingWorkspaceId",
        )
        return  # unreachable; _error_json calls sys.exit(1)
    results = api_util.list_workspaces(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_workspace_to_dict(w) for w in results])


@workspaces.command("get")
@click.option("--workspace-id", "cmd_workspace_id", default=None, help="Workspace ID to retrieve.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def workspaces_get(ctx: click.Context, cmd_workspace_id: str | None, describe: bool) -> None:  # noqa: FBT001
    """Get details of a specific workspace."""
    if describe:
        _describe_output(
            description="Get details of a specific workspace.",
            required_params={"workspace_id": "The workspace ID to retrieve."},
        )
    api_url, client_id, client_secret = _get_auth_no_workspace(ctx)
    workspace_id = resolve_workspace_id(cmd_workspace_id or ctx.obj["_raw_workspace_id"])
    result = api_util.get_workspace(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_workspace_to_dict(result))


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


@cli.group()
@click.pass_context
def sources(ctx: click.Context) -> None:
    """Manage Airbyte sources."""
    pass


@sources.command("list")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def sources_list(ctx: click.Context, describe: bool) -> None:  # noqa: FBT001
    """List sources in the workspace."""
    if describe:
        _describe_output(
            description="List all sources in the workspace.",
            required_params={"workspace_id": "The workspace ID."},
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.list_sources(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_source_to_dict(s) for s in results])


@sources.command("get")
@click.option("--source-id", required=True, help="The source ID to retrieve.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def sources_get(ctx: click.Context, source_id: str, describe: bool) -> None:  # noqa: FBT001
    """Get details of a specific source."""
    if describe:
        _describe_output(
            description="Get details of a specific source.",
            required_params={
                "source_id": "The source ID to retrieve.",
            },
        )
    api_url, client_id, client_secret = _get_auth_no_workspace(ctx)
    result = api_util.get_source(
        source_id=source_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_source_to_dict(result))


@sources.command("create")
@click.option(
    "--json",
    "json_str",
    required=True,
    help='JSON config: {"name": "...", "sourceType": "...", ...}',
)
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def sources_create(ctx: click.Context, json_str: str, describe: bool) -> None:  # noqa: FBT001
    """Create a new source in the workspace."""
    if describe:
        _describe_output(
            description="Create a new source in the workspace.",
            required_params={
                "workspace_id": "The workspace ID.",
                "name": "Display name for the source.",
                "sourceType": "The source connector type (e.g. 'postgres').",
            },
            optional_params={
                "...": "Additional connector-specific configuration fields.",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    config = _parse_json_option(json_str)
    name = config.pop("name", None)
    if not name:
        _error_json("'name' is required in --json config.")
    result = api_util.create_source(
        name=name,
        workspace_id=workspace_id,
        config=config,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_source_to_dict(result))


@sources.command("delete")
@click.option("--source-id", required=True, help="The source ID to delete.")
@click.option("--force", is_flag=True, default=False, help="Skip delete safety checks.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def sources_delete(ctx: click.Context, source_id: str, force: bool, describe: bool) -> None:  # noqa: FBT001
    """Delete a source."""
    if describe:
        _describe_output(
            description="Delete a source by ID.",
            required_params={
                "workspace_id": "The workspace ID.",
                "source_id": "The source ID to delete.",
            },
            optional_params={
                "force": "Skip delete safety checks (default: false).",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    api_util.delete_source(
        source_id=source_id,
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
        safe_mode=not force,
    )
    _json_output({"deleted": True, "source_id": source_id})


# ---------------------------------------------------------------------------
# Destinations
# ---------------------------------------------------------------------------


@cli.group()
@click.pass_context
def destinations(ctx: click.Context) -> None:
    """Manage Airbyte destinations."""
    pass


@destinations.command("list")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def destinations_list(ctx: click.Context, describe: bool) -> None:  # noqa: FBT001
    """List destinations in the workspace."""
    if describe:
        _describe_output(
            description="List all destinations in the workspace.",
            required_params={"workspace_id": "The workspace ID."},
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.list_destinations(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_destination_to_dict(d) for d in results])


@destinations.command("get")
@click.option("--destination-id", required=True, help="The destination ID to retrieve.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def destinations_get(ctx: click.Context, destination_id: str, describe: bool) -> None:  # noqa: FBT001
    """Get details of a specific destination."""
    if describe:
        _describe_output(
            description="Get details of a specific destination.",
            required_params={
                "destination_id": "The destination ID to retrieve.",
            },
        )
    api_url, client_id, client_secret = _get_auth_no_workspace(ctx)
    result = api_util.get_destination(
        destination_id=destination_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_destination_to_dict(result))


@destinations.command("create")
@click.option(
    "--json",
    "json_str",
    required=True,
    help='JSON config: {"name": "...", "destinationType": "...", ...}',
)
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def destinations_create(ctx: click.Context, json_str: str, describe: bool) -> None:  # noqa: FBT001
    """Create a new destination in the workspace."""
    if describe:
        _describe_output(
            description="Create a new destination in the workspace.",
            required_params={
                "workspace_id": "The workspace ID.",
                "name": "Display name for the destination.",
                "destinationType": "The destination connector type (e.g. 'bigquery').",
            },
            optional_params={
                "...": "Additional connector-specific configuration fields.",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    config = _parse_json_option(json_str)
    name = config.pop("name", None)
    if not name:
        _error_json("'name' is required in --json config.")
    result = api_util.create_destination(
        name=name,
        workspace_id=workspace_id,
        config=config,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_destination_to_dict(result))


@destinations.command("delete")
@click.option("--destination-id", required=True, help="The destination ID to delete.")
@click.option("--force", is_flag=True, default=False, help="Skip delete safety checks.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def destinations_delete(
    ctx: click.Context,
    destination_id: str,
    force: bool,  # noqa: FBT001
    describe: bool,  # noqa: FBT001
) -> None:
    """Delete a destination."""
    if describe:
        _describe_output(
            description="Delete a destination by ID.",
            required_params={
                "workspace_id": "The workspace ID.",
                "destination_id": "The destination ID to delete.",
            },
            optional_params={
                "force": "Skip delete safety checks (default: false).",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    api_util.delete_destination(
        destination_id=destination_id,
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
        safe_mode=not force,
    )
    _json_output({"deleted": True, "destination_id": destination_id})


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


@cli.group()
@click.pass_context
def connections(ctx: click.Context) -> None:
    """Manage Airbyte connections."""
    pass


@connections.command("list")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def connections_list(ctx: click.Context, describe: bool) -> None:  # noqa: FBT001
    """List connections in the workspace."""
    if describe:
        _describe_output(
            description="List all connections in the workspace.",
            required_params={"workspace_id": "The workspace ID."},
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.list_connections(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_connection_to_dict(c) for c in results])


@connections.command("get")
@click.option("--connection-id", required=True, help="The connection ID to retrieve.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def connections_get(ctx: click.Context, connection_id: str, describe: bool) -> None:  # noqa: FBT001
    """Get details of a specific connection."""
    if describe:
        _describe_output(
            description="Get details of a specific connection.",
            required_params={
                "workspace_id": "The workspace ID.",
                "connection_id": "The connection ID to retrieve.",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    result = api_util.get_connection(
        workspace_id=workspace_id,
        connection_id=connection_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_connection_to_dict(result))


@connections.command("create")
@click.option(
    "--json",
    "json_str",
    required=True,
    help='JSON config: {"name": "...", "source_id": "...", "destination_id": "...", ...}',
)
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def connections_create(ctx: click.Context, json_str: str, describe: bool) -> None:  # noqa: FBT001
    """Create a new connection."""
    if describe:
        _describe_output(
            description="Create a new connection between a source and destination.",
            required_params={
                "workspace_id": "The workspace ID.",
                "name": "Display name for the connection.",
                "source_id": "The source ID.",
                "destination_id": "The destination ID.",
            },
            optional_params={
                "selected_stream_names": "List of stream names to sync.",
                "prefix": "Optional table prefix for destination.",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    config = _parse_json_option(json_str)
    name = config.get("name")
    source_id = config.get("source_id")
    destination_id = config.get("destination_id")
    if not name or not source_id or not destination_id:
        _error_json("'name', 'source_id', and 'destination_id' are required in --json config.")
    selected_streams: list[str] = config.get("selected_stream_names", [])
    prefix: str = config.get("prefix", "")
    result = api_util.create_connection(
        name=str(name),
        source_id=str(source_id),
        destination_id=str(destination_id),
        workspace_id=workspace_id,
        prefix=prefix,
        selected_stream_names=selected_streams,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_connection_to_dict(result))


@connections.command("delete")
@click.option("--connection-id", required=True, help="The connection ID to delete.")
@click.option("--force", is_flag=True, default=False, help="Skip delete safety checks.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def connections_delete(ctx: click.Context, connection_id: str, force: bool, describe: bool) -> None:  # noqa: FBT001
    """Delete a connection."""
    if describe:
        _describe_output(
            description="Delete a connection by ID.",
            required_params={
                "workspace_id": "The workspace ID.",
                "connection_id": "The connection ID to delete.",
            },
            optional_params={
                "force": "Skip delete safety checks (default: false).",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    api_util.delete_connection(
        connection_id,
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
        safe_mode=not force,
    )
    _json_output({"deleted": True, "connection_id": connection_id})


@connections.command("sync")
@click.option("--connection-id", required=True, help="The connection ID to sync.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def connections_sync(ctx: click.Context, connection_id: str, describe: bool) -> None:  # noqa: FBT001
    """Trigger a sync for a connection."""
    if describe:
        _describe_output(
            description="Trigger a sync job for a connection.",
            required_params={
                "workspace_id": "The workspace ID.",
                "connection_id": "The connection ID to sync.",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    result = api_util.run_connection(
        workspace_id,
        connection_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_job_to_dict(result))


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@cli.group()
@click.pass_context
def jobs(ctx: click.Context) -> None:
    """View Airbyte sync jobs."""
    pass


@jobs.command("list")
@click.option("--connection-id", required=True, help="The connection ID to list jobs for.")
@click.option("--limit", default=20, help="Maximum number of jobs to return.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def jobs_list(ctx: click.Context, connection_id: str, limit: int, describe: bool) -> None:  # noqa: FBT001
    """List recent jobs for a connection."""
    if describe:
        _describe_output(
            description="List recent sync jobs for a connection.",
            required_params={
                "workspace_id": "The workspace ID.",
                "connection_id": "The connection ID.",
            },
            optional_params={
                "limit": "Maximum number of jobs to return (default: 20).",
            },
        )
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.get_job_logs(
        workspace_id,
        connection_id,
        limit,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_job_to_dict(j) for j in results])


@jobs.command("get")
@click.option("--job-id", required=True, type=int, help="The job ID to retrieve.")
@click.option("--describe", is_flag=True, help="Print operation schema and exit.")
@click.pass_context
def jobs_get(ctx: click.Context, job_id: int, describe: bool) -> None:  # noqa: FBT001
    """Get details of a specific job."""
    if describe:
        _describe_output(
            description="Get details of a specific job by ID.",
            required_params={
                "job_id": "The job ID to retrieve.",
            },
        )
    api_url, client_id, client_secret = _get_auth_no_workspace(ctx)
    result = api_util.get_job_info(
        job_id=job_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_job_to_dict(result))


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for `uvx airbyte` / `airbyte` command.

    Wraps the CLI invocation to ensure all errors produce structured JSON
    output on stderr, maintaining the agent-first error contract.
    """
    try:
        cli(standalone_mode=False)
    except SystemExit:
        raise
    except (KeyboardInterrupt, click.Abort):
        _error_json("Operation cancelled.")
    except click.ClickException as exc:
        _error_json(exc.format_message(), type=exc.__class__.__name__)
    except json.JSONDecodeError as exc:
        _error_json(str(exc), type="JSONDecodeError")
    except PyAirbyteInputError as exc:
        _error_json(str(exc), type="PyAirbyteInputError")
    except Exception as exc:
        _error_json(str(exc), type=exc.__class__.__name__)


if __name__ == "__main__":
    main()
