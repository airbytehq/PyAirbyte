#!/usr/bin/env python3
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Helper script to clean up stale connector and connection definitions from cloud tests.

This script scans the source code for hardcoded UUIDs/GUIDs and exempts them from cleanup.
It uses the existing PyAirbyte cloud operations to list and delete resources.

Usage:
    python bin/cleanup_cloud_artifacts.py --dry-run   # Default, lists what would be deleted
    python bin/cleanup_cloud_artifacts.py --no-dry-run  # Actually performs deletions
    python bin/cleanup_cloud_artifacts.py --help      # Show help

Examples:
    python bin/cleanup_cloud_artifacts.py

    python bin/cleanup_cloud_artifacts.py --no-dry-run
"""

import re
import sys
from pathlib import Path

import click

from airbyte.cloud.auth import (
    resolve_cloud_api_url,
    resolve_cloud_client_id,
    resolve_cloud_client_secret,
    resolve_cloud_workspace_id,
)
from airbyte.cloud.workspaces import CloudWorkspace


UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)


def scan_source_code_for_uuids(repo_path: Path) -> set[str]:
    """Scan the source code for hardcoded UUIDs/GUIDs to exempt from cleanup."""
    hardcoded_uuids = set()

    file_patterns = ["**/*.py", "**/*.yaml", "**/*.yml", "**/*.json", "**/*.md"]

    for pattern in file_patterns:
        for file_path in repo_path.glob(pattern):
            if any(part.startswith(".") for part in file_path.parts):
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
                matches = UUID_PATTERN.findall(content)
                hardcoded_uuids.update(matches)
            except (UnicodeDecodeError, PermissionError):
                continue

    return hardcoded_uuids


def get_cloud_workspace() -> CloudWorkspace:
    """Get an authenticated CloudWorkspace using environment variables."""
    return CloudWorkspace(
        workspace_id=resolve_cloud_workspace_id(),
        client_id=resolve_cloud_client_id(),
        client_secret=resolve_cloud_client_secret(),
        api_root=resolve_cloud_api_url(),
    )


def scan_connections(
    workspace: CloudWorkspace, hardcoded_uuids: set[str], *, is_dry_run: bool
) -> list:
    """Scan and optionally delete connections."""
    connections_to_delete = []

    click.echo("\n🔗 Scanning connections...")
    try:
        connections = workspace.list_connections()
        for connection in connections:
            if connection.connection_id not in hardcoded_uuids:
                connections_to_delete.append(connection)
                if is_dry_run:
                    click.echo(
                        f"  Would delete connection: {connection.connection_id} - "
                        f"{connection.connection_url}"
                    )
    except Exception as e:
        click.echo(f"⚠️  Failed to list connections: {e}", err=True)

    return connections_to_delete


def scan_sources(workspace: CloudWorkspace, hardcoded_uuids: set[str], *, is_dry_run: bool) -> list:
    """Scan and optionally delete sources."""
    sources_to_delete = []

    click.echo("\n📥 Scanning sources...")
    try:
        sources = workspace.list_sources()
        for source in sources:
            if source.connector_id not in hardcoded_uuids:
                sources_to_delete.append(source)
                if is_dry_run:
                    click.echo(
                        f"  Would delete source: {source.connector_id} - {source.connector_url}"
                    )
    except Exception as e:
        click.echo(f"⚠️  Failed to list sources: {e}", err=True)

    return sources_to_delete


def scan_destinations(
    workspace: CloudWorkspace, hardcoded_uuids: set[str], *, is_dry_run: bool
) -> list:
    """Scan and optionally delete destinations."""
    destinations_to_delete = []

    click.echo("\n📤 Scanning destinations...")
    try:
        destinations = workspace.list_destinations()
        for destination in destinations:
            if destination.connector_id not in hardcoded_uuids:
                destinations_to_delete.append(destination)
                if is_dry_run:
                    click.echo(
                        f"  Would delete destination: {destination.connector_id} - "
                        f"{destination.connector_url}"
                    )
    except Exception as e:
        click.echo(f"⚠️  Failed to list destinations: {e}", err=True)

    return destinations_to_delete


def perform_deletions(connections: list, sources: list, destinations: list) -> int:
    """Perform actual deletions and return count of successful deletions."""
    deleted_count = 0

    for connection in connections:
        try:
            connection.permanently_delete()
            click.echo(f"  ✅ Deleted connection: {connection.connection_id}")
            deleted_count += 1
        except Exception as e:
            click.echo(
                f"  ❌ Failed to delete connection {connection.connection_id}: {e}", err=True
            )

    for source in sources:
        try:
            source.permanently_delete()
            click.echo(f"  ✅ Deleted source: {source.connector_id}")
            deleted_count += 1
        except Exception as e:
            click.echo(f"  ❌ Failed to delete source {source.connector_id}: {e}", err=True)

    for destination in destinations:
        try:
            destination.permanently_delete()
            click.echo(f"  ✅ Deleted destination: {destination.connector_id}")
            deleted_count += 1
        except Exception as e:
            click.echo(
                f"  ❌ Failed to delete destination {destination.connector_id}: {e}", err=True
            )

    return deleted_count


@click.command()
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    help="If true (default), only list what would be deleted. If false, actually delete resources.",
)
@click.option(
    "--repo-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=Path.cwd(),
    help="Path to the repository to scan for hardcoded UUIDs. Defaults to current directory.",
)
def main(*, dry_run: bool, repo_path: Path) -> None:
    """Clean up stale connector and connection definitions from cloud integration tests."""
    click.echo(f"🔍 Scanning source code for hardcoded UUIDs in: {repo_path}")

    hardcoded_uuids = scan_source_code_for_uuids(repo_path)
    click.echo(f"📋 Found {len(hardcoded_uuids)} hardcoded UUIDs to exempt from cleanup")

    if hardcoded_uuids:
        click.echo("🔒 Exempted UUIDs:")
        for uuid in sorted(hardcoded_uuids):
            click.echo(f"  - {uuid}")

    try:
        workspace = get_cloud_workspace()
        workspace.connect()
        click.echo(f"✅ Connected to workspace: {workspace.workspace_url}")
    except Exception as e:
        click.echo(f"❌ Failed to connect to cloud workspace: {e}", err=True)
        sys.exit(1)

    connections_to_delete = scan_connections(workspace, hardcoded_uuids, is_dry_run=dry_run)
    sources_to_delete = scan_sources(workspace, hardcoded_uuids, is_dry_run=dry_run)
    destinations_to_delete = scan_destinations(workspace, hardcoded_uuids, is_dry_run=dry_run)

    total_items = len(connections_to_delete) + len(sources_to_delete) + len(destinations_to_delete)

    if dry_run:
        click.echo("\n📊 Summary (DRY RUN):")
        click.echo(f"  - {len(connections_to_delete)} connections would be deleted")
        click.echo(f"  - {len(sources_to_delete)} sources would be deleted")
        click.echo(f"  - {len(destinations_to_delete)} destinations would be deleted")
        click.echo(f"  - {total_items} total items would be deleted")

        if total_items > 0:
            click.echo("\n💡 To actually delete these items, run with --no-dry-run")
        else:
            click.echo("\n✨ No stale artifacts found to clean up!")
    else:
        click.echo("\n🗑️  Performing deletions...")

        deleted_count = perform_deletions(
            connections_to_delete, sources_to_delete, destinations_to_delete
        )

        click.echo(
            f"\n📊 Cleanup completed: {deleted_count}/{total_items} items successfully deleted"
        )


if __name__ == "__main__":
    main()
