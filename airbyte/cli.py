# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""CLI for PyAirbyte."""

from __future__ import annotations

from typing import TYPE_CHECKING

import click

from airbyte.destinations.util import get_destination, get_noop_destination
from airbyte.exceptions import PyAirbyteInputError
from airbyte.sources.util import get_benchmark_source, get_source


if TYPE_CHECKING:
    from airbyte.destinations.base import Destination
    from airbyte.sources.base import Source


def _resolve_source_job(
    *,
    source: str | None = None,
    source_job: str | None = None,
) -> Source:
    """Resolve the source job into a configured Source object."""
    # TODO: Implement this function.
    raise NotImplementedError("Not implemented.")
    _ = get_source(...)


def _resolve_destination_job(
    *,
    destination: str,
) -> Destination:
    """Resolve the destination job into a configured Destination object."""
    # TODO: Implement this function.
    raise NotImplementedError("Not implemented.")
    _ = get_destination(...)


@click.command()
def validate() -> None:
    """Validate the data."""
    click.echo("Validating data...")


@click.command()
def benchmark(
    source: str | None = None,
    destination: str | None = None,
    source_job: str | None = None,
) -> None:
    """Run benchmarks.

    Args:
        source: The source name, with an optional version declaration.
            If a path is provided, the source will be loaded from the local path.
            If the string `'.'` is provided, the source will be loaded from the current
            working directory.
        destination: The destination name, with an optional version declaration.
            If a path is provided, the destination will be loaded from the local path.
            If the string `'.'` is provided, the destination will be loaded from the current
            working directory.
            Destination can be omitted - in which case the source will be run in isolation.
        source_job: The source job reference.
    """
    if source and destination:
        raise PyAirbyteInputError(
            message="For benchmarking, source or destination can be provided, but not both.",
        )
    destination_obj: Destination
    source_obj: Source

    source_obj = (
        _resolve_source_job(
            source=source,
            source_job=source_job,
        )
        if source
        else get_benchmark_source()
    )
    destination_obj = (
        _resolve_destination_job(
            destination=destination,
        )
        if destination
        else get_noop_destination()
    )

    click.echo("Running benchmarks...")
    destination_obj.write(
        source_data=source_obj,
        cache=False,
        state_cache=False,
    )


@click.group()
def cli() -> None:
    """PyAirbyte CLI."""
    pass


cli.add_command(validate)
cli.add_command(benchmark)

if __name__ == "__main__":
    cli()
