# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""CLI for PyAirbyte."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import click
import dpath
import yaml

from airbyte.destinations.util import get_destination, get_noop_destination
from airbyte.exceptions import PyAirbyteInputError
from airbyte.sources.util import get_benchmark_source, get_source


if TYPE_CHECKING:
    from airbyte.destinations.base import Destination
    from airbyte.sources.base import Source


def _resolve_source_job(
    *,
    source: str | None = None,
    config: Path | None = None,
    job_file: str | None = None,
    job_dpath: str | None = None,
) -> Source:
    """Resolve the source job into a configured Source object.

    Args:
        source: The source name, with an optional version declaration.
            If a path is provided, the source will be loaded from the local path.
            If the string `'.'` is provided, the source will be loaded from the current
            working directory.
        config: The path to a configuration file for the named source or destination.
            If `config` is provided, the `job_file` and `job_dpath` options will be ignored.
        job_file: A yaml file containing the job definition.
        job_dpath: The dpath expression pointing to a job definition within the job file.
    """
    source_obj: Source
    if source and (source.startswith(".") or "/" in source):
        # Treat the source as a path.
        source_executable = Path(source)
        if not source_executable.exists():
            raise PyAirbyteInputError(
                message=f"Source executable not found: {source}",
            )
        source_obj = get_source(
            name=source_executable.stem,
            local_executable=source_executable,
        )
        return source_obj

    if not source or not source.startswith("source-"):
        raise PyAirbyteInputError(
            message="Expected a source name or path to executable.",
            input_value=source,
        )
    source_name: str = source

    config_dict: dict[str, Any] = {}
    if config:
        config_dict = yaml.safe_load(config.read_text(encoding="utf-8"))

    elif job_file and job_file.endswith(".json"):
        # Treat the job file as a config file.
        config_dict = json.loads(
            Path(job_file).read_text(encoding="utf-8"),
        )

    elif job_file and job_file.endswith(".yaml"):
        # Load the source from the job file.
        job_file_data = yaml.safe_load(job_file)
        if job_dpath:
            job_data = dpath.get(
                obj=job_file_data,
                glob=job_dpath,
            )
            if not isinstance(job_data, dict):
                raise PyAirbyteInputError(
                    message="Invalid job definition.",
                    input_value=str(job_data),
                )
            config_path = Path(job_data["config_path"])
            if not config_path.exists():
                raise PyAirbyteInputError(
                    message="Config file not found.",
                    input_value=str(config_path),
                )
            config_dict = yaml.safe_load(
                config_path.read_text(encoding="utf-8"),
            )

    if not config_dict:
        raise PyAirbyteInputError(
            message="No configuration found.",
        )

    source_obj = get_source(
        name=source_name,
        config=config_dict,
        streams="*",
    )
    return source_obj


def _resolve_destination_job(
    *,
    destination: str,
    config: Path | None = None,
) -> Destination:
    """Resolve the destination job into a configured Destination object.

    Args:
        destination: The destination name, with an optional version declaration.
            If a path is provided, the destination will be loaded from the local path.
            If the string `'.'` is provided, the destination will be loaded from the current
            working directory.
        config: The path to a configuration file for the named source or destination.
    """
    if not config:
        raise PyAirbyteInputError(
            message="No configuration found.",
        )
    config_dict = cast(
        dict,
        json.loads(config.read_text(encoding="utf-8")),
    )

    if destination and (destination.startswith(".") or "/" in destination):
        # Treat the destination as a path.
        destination_executable = Path(destination)
        if not destination_executable.exists():
            raise PyAirbyteInputError(
                message=f"Destination executable not found: {destination}",
            )
        return get_destination(
            name=destination_executable.stem,
            local_executable=destination_executable,
            config=config_dict,
        )

    # else: # Treat the destination as a name.

    return get_destination(
        name=destination,
        config=config_dict,
    )


@click.command(
    help=(
        "Validate the connector has a valid CLI and is able to run `spec`. "
        "If 'config' is provided, we will also run a `check` on the connector "
        "with the provided config."
    ),
)
@click.option(
    "--connector",
    type=str,
    help="The connector name or path.",
)
@click.option(
    "--config",
    type=Path,
    required=False,
    help="The path to a configuration file for the named source or destination.",
)
@click.option(
    "--install",
    is_flag=True,
    default=False,
    help=(
        "Whether to install the connector if it is not available locally. "
        "Defaults to False, meaning the connector is expected to be already be installed."
    ),
)
def validate(
    connector: str | None = None,
    config: Path | None = None,
    *,
    install: bool = False,
) -> None:
    """Validate the connector."""
    local_executable: Path | None = None
    if not connector:
        raise PyAirbyteInputError(
            message="No connector provided.",
        )
    if connector.startswith(".") or "/" in connector:
        # Treat the connector as a path.
        local_executable = Path(connector)
        if not local_executable.exists():
            raise PyAirbyteInputError(
                message=f"Connector executable not found: {connector}",
            )
        connector_name = local_executable.stem
    else:
        connector_name = connector

    if not connector_name.startswith("source-") and not connector_name.startswith("destination-"):
        raise PyAirbyteInputError(
            message="Expected a connector name or path to executable.",
            input_value=connector,
        )

    connector_obj: Source | Destination
    if connector_name.startswith("source-"):
        connector_obj = get_source(
            name=connector_name,
            local_executable=local_executable,
            install_if_missing=install,
        )
    else:  # destination
        connector_obj = get_destination(
            name=connector_name,
            local_executable=local_executable,
            install_if_missing=install,
        )

    print("Getting `spec` output from connector...")
    connector_obj.print_config_spec()

    if config:
        print("Running connector check...")
        connector_obj.set_config(json.loads(config.read_text(encoding="utf-8")))
        connector_obj.check()


@click.command()
@click.option(
    "--source",
    type=str,
    help="The source name, with an optional version declaration. If a path is provided, the source will be loaded from the local path. If the string `'.'` is provided, the source will be loaded from the current working directory.",
)
@click.option(
    "--num-records",
    type=str,
    default="5e5",
    help="The number of records to generate for the benchmark. Ignored if a source is provided. You can specify the number of records to generate using scientific notation. For example, `5e6` will generate 5 million records. By default, 500,000 records will be generated (`5e5` records). If underscores are providing within a numeric a string, they will be ignored.",
)
@click.option(
    "--destination",
    type=str,
    help="The destination name, with an optional version declaration. If a path is provided, the destination will be loaded from the local path. If the string `'.'` is provided, the destination will be loaded from the current working directory. Destination can be omitted - in which case the source will be run in isolation.",
)
@click.option(
    "--config",
    type=Path,
    help=(
        "The path to a configuration file for the named source or destination."
        "If `--config` is provided, the `--job-file` and `--job-dpath` options "
        "will be ignored."
    ),
)
@click.option(
    "--job-file",
    type=str,
    help="A yaml file containing the job definition.",
)
@click.option(
    "--job-dpath",
    type=str,
    help="The dpath expression pointing to a job definition within the job file.",
)
def benchmark(
    source: str | None = None,
    num_records: int | str = "5e5",  # 500,000 records
    destination: str | None = None,
    config: Path | None = None,
    job_file: str | None = None,
    job_dpath: str | None = None,
) -> None:
    """Run benchmarks.

    Args:
        source: The source name, with an optional version declaration.
            If a path is provided, the source will be loaded from the local path.
            If the string `'.'` is provided, the source will be loaded from the current
            working directory.
        num_records: The number of records to generate for the benchmark. Ignored if a source
            is provided. You can specify the number of records to generate using scientific
            notation. For example, `"5e6"` will generate 5 million records. By default, 500,000
            records will be generated ("5e5" records).
            If underscores are providing within a numeric a string, they will be ignored.
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
            config=config,
            job_file=job_file,
            job_dpath=job_dpath,
        )
        if source
        else get_benchmark_source(
            num_records=num_records,
        )
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
