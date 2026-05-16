# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal Click app logic for `airbyte cloud`.

Invokable as:

```bash
uvx airbyte cloud --help
airbyte cloud workspaces list
airbyte cloud sources list --workspace-id <id>
airbyte cloud connections sync --workspace-id <id> <connection-id>
```

Pass `--format json` before a command path for machine-readable parameter
schemas, for example `airbyte cloud --format json sources --help`.
"""

from __future__ import annotations

import click

from airbyte.cli.cloud import connections, destinations, jobs, sources, workspaces
from airbyte.cli.cloud._json_helpers import JsonHelpGroup


@click.group(name="cloud", cls=JsonHelpGroup)
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
def cloud(
    ctx: click.Context,
    client_id: str | None,
    client_secret: str | None,
    workspace_id: str | None,
    api_url: str | None,
    output_format: str,  # noqa: ARG001
) -> None:
    """Airbyte Cloud CLI.

    Manage workspaces, sources, destinations, connections, and jobs
    via structured JSON commands.

    Authentication is resolved from (in order): CLI flags, env vars
    (`AIRBYTE_CLIENT_ID` / `AIRBYTE_CLIENT_SECRET`), or
    `~/.airbyte/credentials` file.

    Use `--format json --help` on any command for machine-readable parameter
    schemas.
    """
    ctx.ensure_object(dict)
    # Store raw values — credentials are resolved lazily when subcommands need them.
    ctx.obj["_raw_client_id"] = client_id
    ctx.obj["_raw_client_secret"] = client_secret
    ctx.obj["_raw_api_url"] = api_url
    ctx.obj["_raw_workspace_id"] = workspace_id


cloud.add_command(workspaces.workspaces)
cloud.add_command(sources.sources)
cloud.add_command(destinations.destinations)
cloud.add_command(connections.connections)
cloud.add_command(jobs.jobs)
