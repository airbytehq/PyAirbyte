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
from typing import TYPE_CHECKING, Any, NoReturn

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


def _error_json(message: str, **extra: object) -> NoReturn:
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


# ---------------------------------------------------------------------------
# JSON help infrastructure
# ---------------------------------------------------------------------------

# Registry mapping command function names to their JSON help metadata.
_COMMAND_SCHEMAS: dict[str, dict[str, Any]] = {}


def _register_schema(
    func_name: str,
    description: str,
    required_params: dict[str, str] | None = None,
    optional_params: dict[str, str] | None = None,
) -> None:
    """Register JSON-help metadata for a command."""
    schema: dict[str, Any] = {"description": description}
    if required_params:
        schema["required_params"] = required_params
    if optional_params:
        schema["optional_params"] = optional_params
    _COMMAND_SCHEMAS[func_name] = schema


def _emit_json_help(ctx: click.Context) -> None:
    """If `--format json` is active, print JSON help and exit (used by --help)."""
    if not _is_json_format(ctx):
        return  # fall through to normal Click help

    # Walk context chain to build the full command name.
    cmd_name = ctx.info_name or ""
    parent = ctx.parent
    while parent and parent.info_name:
        cmd_name = f"{parent.info_name}_{cmd_name}"
        parent = parent.parent

    # Try the leaf command, then the subcommand function name.
    func = ctx.command.callback
    func_name = func.__name__ if func else cmd_name
    schema = _COMMAND_SCHEMAS.get(func_name)
    if schema:
        _json_output(schema)
    else:
        # Fallback: emit a minimal JSON help with the docstring.
        _json_output({"description": ctx.command.help or cmd_name})
    ctx.exit(0)


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


def _is_json_format(ctx: click.Context) -> bool:
    """Check if ``--format json`` was requested.

    Click's ``--help`` is an eager option that fires before the group
    callback, so ``ctx.params`` may be empty at the root level.  We
    fall back to inspecting ``sys.argv`` when the param is missing.
    """
    fmt = ctx.find_root().params.get("output_format")
    if fmt:
        return fmt == "json"
    # Fallback: scan raw argv for ``--format json`` or ``--format=json``.
    for i, arg in enumerate(sys.argv):
        if arg == "--format" and i + 1 < len(sys.argv) and sys.argv[i + 1].lower() == "json":
            return True
        if arg.lower() == "--format=json":
            return True
    return False


class _JsonHelpGroup(click.Group):
    """Click group that emits JSON help when ``--format json --help`` is used."""

    def get_help(self, ctx: click.Context) -> str:
        if _is_json_format(ctx):
            # List subcommands as JSON.
            commands: dict[str, str] = {}
            for name in self.list_commands(ctx):
                cmd = self.get_command(ctx, name)
                if cmd:
                    commands[name] = cmd.get_short_help_str(limit=300)
            _json_output({"description": self.help or "", "commands": commands})
            ctx.exit(0)
        return super().get_help(ctx)


class _JsonHelpCommand(click.Command):
    """Click command that emits JSON help when ``--format json --help`` is used."""

    def get_help(self, ctx: click.Context) -> str:
        _emit_json_help(ctx)
        return super().get_help(ctx)


@click.group(cls=_JsonHelpGroup)
@click.option("--client-id", envvar="AIRBYTE_CLIENT_ID", default=None, help="Airbyte client ID.")
@click.option(
    "--client-secret", envvar="AIRBYTE_CLIENT_SECRET", default=None, help="Airbyte client secret."
)
@click.option("--workspace-id", default=None, help="Airbyte workspace ID.")
@click.option("--api-url", default=None, help="Airbyte API URL override.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    help="Output format for --help (default: text).",
)
@click.pass_context
def cli(
    ctx: click.Context,
    client_id: str | None,
    client_secret: str | None,
    workspace_id: str | None,
    api_url: str | None,
    output_format: str,  # noqa: ARG001
) -> None:
    """Airbyte CLI — agent-first interface for Airbyte Cloud.

    Manage workspaces, sources, destinations, connections, and jobs
    via structured JSON commands.

    Authentication is resolved from (in order): CLI flags, env vars
    (`AIRBYTE_CLIENT_ID` / `AIRBYTE_CLIENT_SECRET`), or
    `~/.airbyte/credentials` file.

    Use `--format json --help` on any command for machine-readable parameter
    schemas (no auth required).
    """
    ctx.ensure_object(dict)
    # Store raw values — credentials are resolved lazily when subcommands need them.
    ctx.obj["_raw_client_id"] = client_id
    ctx.obj["_raw_client_secret"] = client_secret
    ctx.obj["_raw_api_url"] = api_url
    ctx.obj["_raw_workspace_id"] = workspace_id


# ---------------------------------------------------------------------------
# Workspaces
# ---------------------------------------------------------------------------


@cli.group(cls=_JsonHelpGroup)
@click.pass_context
def workspaces(ctx: click.Context) -> None:
    """Manage Airbyte workspaces."""
    pass


_register_schema(
    "workspaces_list",
    description="List all workspaces accessible with the current credentials.",
    optional_params={"workspace_id": "Filter to a specific workspace ID."},
)


@workspaces.command("list", cls=_JsonHelpCommand)
@click.pass_context
def workspaces_list(ctx: click.Context) -> None:
    """List workspaces accessible with the current credentials."""
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.list_workspaces(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_workspace_to_dict(w) for w in results])


_register_schema(
    "workspaces_get",
    description="Get details of a specific workspace.",
    required_params={"workspace_id": "The workspace ID to retrieve."},
)


@workspaces.command("get", cls=_JsonHelpCommand)
@click.option("--workspace-id", "cmd_workspace_id", default=None, help="Workspace ID to retrieve.")
@click.pass_context
def workspaces_get(ctx: click.Context, cmd_workspace_id: str | None) -> None:
    """Get details of a specific workspace."""
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


@cli.group(cls=_JsonHelpGroup)
@click.pass_context
def sources(ctx: click.Context) -> None:
    """Manage Airbyte sources."""
    pass


_register_schema(
    "sources_list",
    description="List all sources in the workspace.",
    required_params={"workspace_id": "The workspace ID."},
)


@sources.command("list", cls=_JsonHelpCommand)
@click.pass_context
def sources_list(ctx: click.Context) -> None:
    """List sources in the workspace."""
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.list_sources(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_source_to_dict(s) for s in results])


_register_schema(
    "sources_get",
    description="Get details of a specific source.",
    required_params={"source_id": "The source ID to retrieve."},
)


@sources.command("get", cls=_JsonHelpCommand)
@click.option("--source-id", default=None, help="The source ID to retrieve.")
@click.pass_context
def sources_get(ctx: click.Context, source_id: str | None) -> None:
    """Get details of a specific source."""
    if not source_id:
        _error_json("--source-id is required.", type="MissingParameter")
        return
    api_url, client_id, client_secret = _get_auth_no_workspace(ctx)
    result = api_util.get_source(
        source_id=source_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_source_to_dict(result))


_register_schema(
    "sources_create",
    description="Create a new source in the workspace.",
    required_params={
        "workspace_id": "The workspace ID.",
        "name": "Display name for the source.",
        "sourceType": "The source connector type (e.g. 'postgres').",
    },
    optional_params={"...": "Additional connector-specific configuration fields."},
)


@sources.command("create", cls=_JsonHelpCommand)
@click.option(
    "--json",
    "json_str",
    default=None,
    help='JSON config: {"name": "...", "sourceType": "...", ...}',
)
@click.pass_context
def sources_create(ctx: click.Context, json_str: str | None) -> None:
    """Create a new source in the workspace."""
    if not json_str:
        _error_json("--json is required.", type="MissingParameter")
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


_register_schema(
    "sources_delete",
    description="Delete a source by ID.",
    required_params={
        "workspace_id": "The workspace ID.",
        "source_id": "The source ID to delete.",
    },
    optional_params={"force": "Skip delete safety checks (default: false)."},
)


@sources.command("delete", cls=_JsonHelpCommand)
@click.option("--source-id", default=None, help="The source ID to delete.")
@click.option("--force", is_flag=True, default=False, help="Skip delete safety checks.")
@click.pass_context
def sources_delete(ctx: click.Context, source_id: str | None, force: bool) -> None:  # noqa: FBT001
    """Delete a source."""
    if not source_id:
        _error_json("--source-id is required.", type="MissingParameter")
        return
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


@cli.group(cls=_JsonHelpGroup)
@click.pass_context
def destinations(ctx: click.Context) -> None:
    """Manage Airbyte destinations."""
    pass


_register_schema(
    "destinations_list",
    description="List all destinations in the workspace.",
    required_params={"workspace_id": "The workspace ID."},
)


@destinations.command("list", cls=_JsonHelpCommand)
@click.pass_context
def destinations_list(ctx: click.Context) -> None:
    """List destinations in the workspace."""
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.list_destinations(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_destination_to_dict(d) for d in results])


_register_schema(
    "destinations_get",
    description="Get details of a specific destination.",
    required_params={"destination_id": "The destination ID to retrieve."},
)


@destinations.command("get", cls=_JsonHelpCommand)
@click.option("--destination-id", default=None, help="The destination ID to retrieve.")
@click.pass_context
def destinations_get(ctx: click.Context, destination_id: str | None) -> None:
    """Get details of a specific destination."""
    if not destination_id:
        _error_json("--destination-id is required.", type="MissingParameter")
        return
    api_url, client_id, client_secret = _get_auth_no_workspace(ctx)
    result = api_util.get_destination(
        destination_id=destination_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output(_destination_to_dict(result))


_register_schema(
    "destinations_create",
    description="Create a new destination in the workspace.",
    required_params={
        "workspace_id": "The workspace ID.",
        "name": "Display name for the destination.",
        "destinationType": "The destination connector type (e.g. 'bigquery').",
    },
    optional_params={"...": "Additional connector-specific configuration fields."},
)


@destinations.command("create", cls=_JsonHelpCommand)
@click.option(
    "--json",
    "json_str",
    default=None,
    help='JSON config: {"name": "...", "destinationType": "...", ...}',
)
@click.pass_context
def destinations_create(ctx: click.Context, json_str: str | None) -> None:
    """Create a new destination in the workspace."""
    if not json_str:
        _error_json("--json is required.", type="MissingParameter")
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


_register_schema(
    "destinations_delete",
    description="Delete a destination by ID.",
    required_params={
        "workspace_id": "The workspace ID.",
        "destination_id": "The destination ID to delete.",
    },
    optional_params={"force": "Skip delete safety checks (default: false)."},
)


@destinations.command("delete", cls=_JsonHelpCommand)
@click.option("--destination-id", default=None, help="The destination ID to delete.")
@click.option("--force", is_flag=True, default=False, help="Skip delete safety checks.")
@click.pass_context
def destinations_delete(
    ctx: click.Context,
    destination_id: str | None,
    force: bool,  # noqa: FBT001
) -> None:
    """Delete a destination."""
    if not destination_id:
        _error_json("--destination-id is required.", type="MissingParameter")
        return
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


@cli.group(cls=_JsonHelpGroup)
@click.pass_context
def connections(ctx: click.Context) -> None:
    """Manage Airbyte connections."""
    pass


_register_schema(
    "connections_list",
    description="List all connections in the workspace.",
    required_params={"workspace_id": "The workspace ID."},
)


@connections.command("list", cls=_JsonHelpCommand)
@click.pass_context
def connections_list(ctx: click.Context) -> None:
    """List connections in the workspace."""
    api_url, client_id, client_secret, workspace_id = _get_auth_context(ctx)
    results = api_util.list_connections(
        workspace_id=workspace_id,
        api_root=api_url,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=None,
    )
    _json_output([_connection_to_dict(c) for c in results])


_register_schema(
    "connections_get",
    description="Get details of a specific connection.",
    required_params={
        "workspace_id": "The workspace ID.",
        "connection_id": "The connection ID to retrieve.",
    },
)


@connections.command("get", cls=_JsonHelpCommand)
@click.option("--connection-id", default=None, help="The connection ID to retrieve.")
@click.pass_context
def connections_get(ctx: click.Context, connection_id: str | None) -> None:
    """Get details of a specific connection."""
    if not connection_id:
        _error_json("--connection-id is required.", type="MissingParameter")
        return
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


_register_schema(
    "connections_create",
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


@connections.command("create", cls=_JsonHelpCommand)
@click.option(
    "--json",
    "json_str",
    default=None,
    help='JSON config: {"name": "...", "source_id": "...", "destination_id": "...", ...}',
)
@click.pass_context
def connections_create(ctx: click.Context, json_str: str | None) -> None:
    """Create a new connection."""
    if not json_str:
        _error_json("--json is required.", type="MissingParameter")
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


_register_schema(
    "connections_delete",
    description="Delete a connection by ID.",
    required_params={
        "workspace_id": "The workspace ID.",
        "connection_id": "The connection ID to delete.",
    },
    optional_params={"force": "Skip delete safety checks (default: false)."},
)


@connections.command("delete", cls=_JsonHelpCommand)
@click.option("--connection-id", default=None, help="The connection ID to delete.")
@click.option("--force", is_flag=True, default=False, help="Skip delete safety checks.")
@click.pass_context
def connections_delete(
    ctx: click.Context,
    connection_id: str | None,
    force: bool,  # noqa: FBT001
) -> None:
    """Delete a connection."""
    if not connection_id:
        _error_json("--connection-id is required.", type="MissingParameter")
        return
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


_register_schema(
    "connections_sync",
    description="Trigger a sync job for a connection.",
    required_params={
        "workspace_id": "The workspace ID.",
        "connection_id": "The connection ID to sync.",
    },
)


@connections.command("sync", cls=_JsonHelpCommand)
@click.option("--connection-id", default=None, help="The connection ID to sync.")
@click.pass_context
def connections_sync(ctx: click.Context, connection_id: str | None) -> None:
    """Trigger a sync for a connection."""
    if not connection_id:
        _error_json("--connection-id is required.", type="MissingParameter")
        return
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


@cli.group(cls=_JsonHelpGroup)
@click.pass_context
def jobs(ctx: click.Context) -> None:
    """View Airbyte sync jobs."""
    pass


_register_schema(
    "jobs_list",
    description="List recent sync jobs for a connection.",
    required_params={
        "workspace_id": "The workspace ID.",
        "connection_id": "The connection ID.",
    },
    optional_params={"limit": "Maximum number of jobs to return (default: 20)."},
)


@jobs.command("list", cls=_JsonHelpCommand)
@click.option("--connection-id", default=None, help="The connection ID to list jobs for.")
@click.option("--limit", default=20, help="Maximum number of jobs to return.")
@click.pass_context
def jobs_list(ctx: click.Context, connection_id: str | None, limit: int) -> None:
    """List recent jobs for a connection."""
    if not connection_id:
        _error_json("--connection-id is required.", type="MissingParameter")
        return
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


_register_schema(
    "jobs_get",
    description="Get details of a specific job by ID.",
    required_params={"job_id": "The job ID to retrieve."},
)


@jobs.command("get", cls=_JsonHelpCommand)
@click.option("--job-id", default=None, type=int, help="The job ID to retrieve.")
@click.pass_context
def jobs_get(ctx: click.Context, job_id: int | None) -> None:
    """Get details of a specific job."""
    if job_id is None:
        _error_json("--job-id is required.", type="MissingParameter")
        return
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

    Wraps the CLI invocation to catch known failure modes and emit
    structured JSON on stderr.  Unknown exceptions propagate naturally
    so they surface with a full traceback for debugging.
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


if __name__ == "__main__":
    main()
