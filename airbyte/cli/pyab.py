# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""CLI for PyAirbyte.

The PyAirbyte CLI provides a command-line interface for testing connectors and running benchmarks.

After installing PyAirbyte, the CLI can be invoked with the `pyairbyte` CLI executable, or the
shorter `pyab` alias.

These are equivalent:

```bash
pyairbyte --help
pyab --help
```

You can also use `pipx` or the fast and powerful `uv` tool to run the PyAirbyte CLI
without pre-installing:

```bash
# Install `uv` if you haven't already:
brew install uv

# Run the PyAirbyte CLI using `uvx`:
uvx --from=airbyte pyab --help
```

Example `benchmark` Usage:

```bash
# PyAirbyte System Benchmark (no-op):
pyab benchmark --num-records=2.4e6

# Source Benchmark:
pyab benchmark --source=source-hardcoded-records --config='{count: 400000}'
pyab benchmark --source=source-hardcoded-records --config='{count: 400000}' --streams='*'
pyab benchmark --source=source-hardcoded-records --config='{count: 4000}' --streams=dummy_fields

# Source Benchmark from Docker Image:
pyab benchmark --source=airbyte/source-hardcoded-records:latest --config='{count: 400_000}'
pyab benchmark --source=airbyte/source-hardcoded-records:dev --config='{count: 400_000}'

# Destination Benchmark:
pyab benchmark --destination=destination-dev-null --config=/path/to/config.json

# Benchmark a Local Python Source (source-s3):
pyab benchmark --source=$(poetry run which source-s3) --config=./secrets/config.json
# Equivalent to:
LOCAL_EXECUTABLE=$(poetry run which source-s3)
CONFIG_PATH=$(realpath ./secrets/config.json)
pyab benchmark --source=$LOCAL_EXECUTABLE --config=$CONFIG_PATH
```

Example `validate` Usage:

```bash
pyab validate --connector=source-hardcoded-records
pyab validate --connector=source-hardcoded-records --config='{count: 400_000}'
```
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import yaml

from airbyte.destinations.util import get_destination, get_noop_destination
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.util import get_secret
from airbyte.sources.util import get_benchmark_source, get_source


if TYPE_CHECKING:
    from airbyte.destinations.base import Destination
    from airbyte.sources.base import Source


CLI_GUIDANCE = """
----------------------

PyAirbyte CLI Guidance

Providing connector configuration:

When providing configuration via `--config`, you can providing any of the following:

1. A path to a configuration file, in yaml or json format.

2. An inline yaml string, e.g. `--config='{key: value}'`, --config='{key: {nested: value}}'.

When providing an inline yaml string, it is recommended to use single quotes to avoid shell
interpolation.

Providing secrets:

You can provide secrets in your configuration file by prefixing the secret value with `SECRET:`.
For example, --config='{password: "SECRET:my_password"'} will look for a secret named `my_password`
in the secret store. By default, PyAirbyte will look for secrets in environment variables and
dotenv (.env) files. If a secret is not found, you'll be prompted to provide the secret value
interactively in the terminal.

It is highly recommended to use secrets when using inline yaml strings, in order to avoid
exposing secrets in plain text in the terminal history. Secrets provided interactively will
not be echoed to the terminal.
"""

# Add the CLI guidance to the module docstring.
globals()["__doc__"] = globals().get("__doc__", "") + CLI_GUIDANCE

CONFIG_HELP = (
    "Either a path to a configuration file for the named source or destination, "
    "or an inline yaml string. If providing an inline yaml string, use single quotes "
    "to avoid shell interpolation. For example, --config='{key: value}' or "
    "--config='{key: {nested: value}}'. \n"
    "PyAirbyte secrets can be accessed by prefixing the secret name with 'SECRET:'. "
    """For example, --config='{password: "SECRET:MY_PASSWORD"}'."""
)

PIP_URL_HELP = (
    "This can be anything pip accepts, including: a PyPI package name, a local path, "
    "a git repository, a git branch ref, etc. Use '.' to install from the current local "
    "directory."
)


def _resolve_config(
    config: str,
) -> dict[str, Any]:
    """Resolve the configuration file into a dictionary."""

    def _inject_secrets(config_dict: dict[str, Any]) -> None:
        """Inject secrets into the configuration dictionary."""
        for key, value in config_dict.items():
            if isinstance(value, dict):
                _inject_secrets(value)
            elif isinstance(value, str) and value.startswith("SECRET:"):
                config_dict[key] = get_secret(value.removeprefix("SECRET:").strip())

    config_dict: dict[str, Any]
    if config.startswith("{"):
        # Treat this as an inline yaml string:
        config_dict = yaml.safe_load(config)
    else:
        # Treat this as a path to a config file:
        config_path = Path(config)
        if not config_path.exists():
            raise PyAirbyteInputError(
                message="Config file not found.",
                input_value=str(config_path),
            )
        config_dict = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    _inject_secrets(config_dict)
    return config_dict


def _is_docker_image(image: str | None) -> bool:
    """Check if the source or destination is a docker image."""
    return image is not None and ":" in image


def _is_executable_path(connector_str: str) -> bool:
    return connector_str.startswith(".") or "/" in connector_str


def _get_connector_name(connector: str) -> str:
    if _is_docker_image(connector):
        return connector.split(":", maxsplit=1)[0].rsplit("/", maxsplit=1)[-1]

    return connector


def _parse_use_python(use_python_str: str | None) -> bool | Path | str | None:
    r"""Parse the use_python CLI parameter.

    Args:
        use_python_str: The raw string value from CLI input.

    Returns:
        - None: No parameter provided
        - True: Use current Python interpreter ("true")
        - False: Use Docker instead ("false")
        - Path: Use interpreter at this path (paths containing / or \ or starting with .)
        - str: Use uv-managed Python version (semver patterns like "3.12", "3.11.5")
               or existing interpreter name (non-semver strings like "python3.10")
    """
    if use_python_str is None:
        return None
    if use_python_str.lower() == "true":
        return True
    if use_python_str.lower() == "false":
        return False
    if "/" in use_python_str or "\\" in use_python_str or use_python_str.startswith("."):
        return Path(use_python_str)

    semver_pattern = r"^\d+\.\d+(?:\.\d+)?$"
    if re.match(semver_pattern, use_python_str):
        return use_python_str  # Return as string for uv-managed version

    return Path(use_python_str)


def _resolve_source_job(
    *,
    source: str | None = None,
    config: str | None = None,
    streams: str | None = None,
    pip_url: str | None = None,
    use_python: str | None = None,
) -> Source:
    """Resolve the source job into a configured Source object.

    Args:
        source: The source name or source reference.
            If a path is provided, the source will be loaded from the local path.
            If the source contains a colon (':'), it will be interpreted as a docker image and tag.
        config: The path to a configuration file for the named source or destination.
        streams: A comma-separated list of stream names to select for reading. If set to "*",
            all streams will be selected. If not provided, all streams will be selected.
        pip_url: Optional. A location from which to install the connector.
        use_python: Optional. Python interpreter specification.
    """
    config_dict = _resolve_config(config) if config else None
    streams_list: str | list[str] = streams or "*"
    if isinstance(streams, str) and streams != "*":
        streams_list = [stream.strip() for stream in streams.split(",")]

    use_python_parsed = _parse_use_python(use_python)

    source_obj: Source
    if source and _is_docker_image(source):
        source_obj = get_source(
            name=_get_connector_name(source),
            docker_image=source,
            config=config_dict,
            streams=streams_list,
            pip_url=pip_url,
            use_python=use_python_parsed,
        )
        return source_obj

    if source and _is_executable_path(source):
        # Treat the source as a path.
        source_executable = Path(source)
        if not source_executable.exists():
            raise PyAirbyteInputError(
                message="Source executable not found.",
                context={
                    "source": source,
                },
            )
        source_obj = get_source(
            name=source_executable.stem,
            local_executable=source_executable,
            config=config_dict,
            streams=streams_list,
            pip_url=pip_url,
            use_python=use_python_parsed,
        )
        return source_obj

    if not source or not source.startswith("source-"):
        raise PyAirbyteInputError(
            message="Expected a source name, docker image, or path to executable.",
            input_value=source,
        )

    source_name: str = source

    return get_source(
        name=source_name,
        config=config_dict,
        streams=streams_list,
        pip_url=pip_url,
        use_python=use_python_parsed,
    )


def _get_noop_destination_config() -> dict[str, Any]:
    return {
        "test_destination": {
            "test_destination_type": "SILENT",
        }
    }


def _resolve_destination_job(
    *,
    destination: str,
    config: str | None = None,
    pip_url: str | None = None,
    use_python: str | None = None,
) -> Destination:
    """Resolve the destination job into a configured Destination object.

    Args:
        destination: The destination name or source reference.
            If a path is provided, the source will be loaded from the local path.
            If the destination contains a colon (':'), it will be interpreted as a docker image
            and tag.
        config: The path to a configuration file for the named source or destination.
        pip_url: Optional. A location from which to install the connector.
        use_python: Optional. Python interpreter specification.
    """
    config_dict = _resolve_config(config) if config else {}
    use_python_parsed = _parse_use_python(use_python)

    destination_name = _get_connector_name(destination)

    if destination_name == "destination-dev-null" and not config:
        config_dict = _get_noop_destination_config()

    if _is_docker_image(destination):
        return get_destination(
            name=destination_name,
            docker_image=destination,
            config=config_dict,
            pip_url=pip_url,
            use_python=use_python_parsed,
        )

    if destination and (destination.startswith(".") or "/" in destination):
        # Treat the destination as a path.
        destination_executable = Path(destination)
        if not destination_executable.exists():
            raise PyAirbyteInputError(
                message="Destination executable not found.",
                context={
                    "destination": destination,
                },
            )
        return get_destination(
            name=destination_executable.stem,
            local_executable=destination_executable,
            config=config_dict,
            pip_url=pip_url,
            use_python=use_python_parsed,
        )

    # else: # Treat the destination as a name.

    return get_destination(
        name=destination,
        config=config_dict,
        pip_url=pip_url,
        use_python=use_python_parsed,
    )


@click.command(
    help=(
        "Validate the connector has a valid CLI and is able to run `spec`. "
        "If 'config' is provided, we will also run a `check` on the connector "
        "with the provided config.\n\n" + CLI_GUIDANCE
    ),
)
@click.option(
    "--connector",
    type=str,
    help="The connector name or a path to the local executable.",
)
@click.option(
    "--pip-url",
    type=str,
    help=(
        "Optional. The location from which to install the connector. "
        "This can be a anything pip accepts, including: a PyPI package name, a local path, "
        "a git repository, a git branch ref, etc."
    ),
)
@click.option(
    "--config",
    type=str,
    required=False,
    help=CONFIG_HELP,
)
@click.option(
    "--use-python",
    type=str,
    help=(
        "Python interpreter specification. Use 'true' for current Python, "
        "'false' for Docker, a path for specific interpreter, or a version "
        "string for uv-managed Python (e.g., '3.11', 'python3.12')."
    ),
)
def validate(
    connector: str | None = None,
    config: str | None = None,
    pip_url: str | None = None,
    use_python: str | None = None,
) -> None:
    """CLI command to run a `benchmark` operation."""
    if not connector:
        raise PyAirbyteInputError(
            message="No connector provided.",
        )

    connector_obj: Source | Destination
    if "source-" in connector:
        connector_obj = _resolve_source_job(
            source=connector,
            config=None,
            streams=None,
            pip_url=pip_url,
            use_python=use_python,
        )
    else:  # destination
        connector_obj = _resolve_destination_job(
            destination=connector,
            config=None,
            pip_url=pip_url,
            use_python=use_python,
        )

    print("Getting `spec` output from connector...", file=sys.stderr)
    connector_obj.print_config_spec(stderr=True)

    if config:
        print("Running connector check...")
        config_dict: dict[str, Any] = _resolve_config(config)
        connector_obj.set_config(config_dict)
        connector_obj.check()


@click.command()
@click.option(
    "--source",
    type=str,
    help=(
        "The source name, with an optional version declaration. "
        "If the name contains a colon (':'), it will be interpreted as a docker image and tag. "
    ),
)
@click.option(
    "--streams",
    type=str,
    default="*",
    help=(
        "A comma-separated list of stream names to select for reading. If set to '*', all streams "
        "will be selected. Defaults to '*'."
    ),
)
@click.option(
    "--num-records",
    type=str,
    default="5e5",
    help=(
        "The number of records to generate for the benchmark. Ignored if a source is provided. "
        "You can specify the number of records to generate using scientific notation. "
        "For example, `5e6` will generate 5 million records. By default, 500,000 records will "
        "be generated (`5e5` records). If underscores are providing within a numeric a string, "
        "they will be ignored."
    ),
)
@click.option(
    "--destination",
    type=str,
    help=(
        "The destination name, with an optional version declaration. "
        "If a path is provided, it will be interpreted as a path to the local executable. "
    ),
)
@click.option(
    "--config",
    type=str,
    help=CONFIG_HELP,
)
@click.option(
    "--use-python",
    type=str,
    help=(
        "Python interpreter specification. Use 'true' for current Python, "
        "'false' for Docker, a path for specific interpreter, or a version "
        "string for uv-managed Python (e.g., '3.11', 'python3.12')."
    ),
)
def benchmark(
    source: str | None = None,
    streams: str = "*",
    num_records: int | str = "5e5",  # 500,000 records
    destination: str | None = None,
    config: str | None = None,
    use_python: str | None = None,
) -> None:
    """CLI command to run a `benchmark` operation.

    You can provide either a source or a destination, but not both. If a destination is being
    benchmarked, you can use `--num-records` to specify the number of records to generate for the
    benchmark.

    If a source is being benchmarked, you can provide a configuration file or a job
    definition file to run the source job.
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
            streams=streams,
            use_python=use_python,
        )
        if source
        else get_benchmark_source(
            num_records=num_records,
        )
    )
    destination_obj = (
        _resolve_destination_job(
            destination=destination,
            config=config,
            use_python=use_python,
        )
        if destination
        else get_noop_destination()
    )

    click.echo("Running benchmarks...", sys.stderr)
    destination_obj.write(
        source_data=source_obj,
        cache=False,
        state_cache=False,
    )


@click.command()
@click.option(
    "--source",
    type=str,
    help=(
        "The source name, with an optional version declaration. "
        "If the name contains a colon (':'), it will be interpreted as a docker image and tag. "
    ),
)
@click.option(
    "--destination",
    type=str,
    help=(
        "The destination name, with an optional version declaration. "
        "If a path is provided, it will be interpreted as a path to the local executable. "
    ),
)
@click.option(
    "--streams",
    type=str,
    help=(
        "A comma-separated list of stream names to select for reading. If set to '*', all streams "
        "will be selected. Defaults to '*'."
    ),
)
@click.option(
    "--Sconfig",
    "source_config",
    type=str,
    help="The source config. " + CONFIG_HELP,
)
@click.option(
    "--Dconfig",
    "destination_config",
    type=str,
    help="The destination config. " + CONFIG_HELP,
)
@click.option(
    "--Spip-url",
    "source_pip_url",
    type=str,
    help="Optional pip URL for the source (Python connectors only). " + PIP_URL_HELP,
)
@click.option(
    "--Dpip-url",
    "destination_pip_url",
    type=str,
    help="Optional pip URL for the destination (Python connectors only). " + PIP_URL_HELP,
)
@click.option(
    "--use-python",
    type=str,
    help=(
        "Python interpreter specification. Use 'true' for current Python, "
        "'false' for Docker, a path for specific interpreter, or a version "
        "string for uv-managed Python (e.g., '3.11', 'python3.12')."
    ),
)
def sync(
    *,
    source: str,
    source_config: str | None = None,
    source_pip_url: str | None = None,
    destination: str,
    destination_config: str | None = None,
    destination_pip_url: str | None = None,
    streams: str | None = None,
    use_python: str | None = None,
) -> None:
    """CLI command to run a `sync` operation.

    Currently, this only supports full refresh syncs. Incremental syncs are not yet supported.
    Custom catalog syncs are not yet supported.
    """
    destination_obj: Destination
    source_obj: Source

    source_obj = _resolve_source_job(
        source=source,
        config=source_config,
        streams=streams,
        pip_url=source_pip_url,
        use_python=use_python,
    )
    destination_obj = _resolve_destination_job(
        destination=destination,
        config=destination_config,
        pip_url=destination_pip_url,
        use_python=use_python,
    )

    click.echo("Running sync...")
    destination_obj.write(
        source_data=source_obj,
        cache=False,
        state_cache=False,
    )


def _get_smoke_test_source(
    *,
    scenarios: str = "all",
    custom_scenarios: str | None = None,
) -> Source:
    """Create a smoke test source with the given configuration.

    The smoke test source generates synthetic data across predefined scenarios
    that cover common destination failure patterns.

    `scenarios` controls which scenarios to run: `'all'` runs all scenarios
    (including large batch), or provide a comma-separated list of scenario names.

    `custom_scenarios` is an optional path to a JSON or YAML file containing additional
    scenario definitions. Each scenario should have `name`, `json_schema`,
    and optionally `records` and `primary_key`.
    """
    is_all = scenarios.strip().lower() == "all"
    source_config: dict[str, Any] = {
        "all_fast_streams": is_all,
        "all_slow_streams": is_all,
    }
    if not is_all:
        source_config["scenario_filter"] = [s.strip() for s in scenarios.split(",") if s.strip()]
    if custom_scenarios:
        custom_path = Path(custom_scenarios)
        if not custom_path.exists():
            raise PyAirbyteInputError(
                message="Custom scenarios file not found.",
                input_value=str(custom_path),
            )
        loaded = yaml.safe_load(custom_path.read_text(encoding="utf-8"))
        if isinstance(loaded, list):
            source_config["custom_scenarios"] = loaded
        elif isinstance(loaded, dict) and "custom_scenarios" in loaded:
            source_config["custom_scenarios"] = loaded["custom_scenarios"]
        else:
            raise PyAirbyteInputError(
                message=(
                    "Custom scenarios file must contain a list of scenarios "
                    "or a dict with a 'custom_scenarios' key."
                ),
                input_value=str(custom_path),
            )

    return get_source(
        name="source-smoke-test",
        config=source_config,
        streams="*",
        local_executable="source-smoke-test",
    )


@click.command(name="destination-smoke-test")
@click.option(
    "--destination",
    type=str,
    required=True,
    help=(
        "The destination connector to test. Can be a connector name "
        "(e.g. 'destination-snowflake'), a Docker image with tag "
        "(e.g. 'airbyte/destination-snowflake:3.14.0'), or a path to a local executable."
    ),
)
@click.option(
    "--config",
    type=str,
    help="The destination configuration. " + CONFIG_HELP,
)
@click.option(
    "--pip-url",
    type=str,
    help="Optional pip URL for the destination (Python connectors only). " + PIP_URL_HELP,
)
@click.option(
    "--use-python",
    type=str,
    help=(
        "Python interpreter specification. Use 'true' for current Python, "
        "'false' for Docker, a path for specific interpreter, or a version "
        "string for uv-managed Python (e.g., '3.11', 'python3.12')."
    ),
)
@click.option(
    "--scenarios",
    type=str,
    default="all",
    help=(
        "Which smoke test scenarios to run. Use 'all' (default) to run every "
        "predefined scenario including large batch, or provide a comma-separated "
        "list of scenario names. "
        "Available scenarios: basic_types_stream, timestamp_stream, "
        "large_decimal_stream, nested_json_stream, null_values_stream, "
        "column_name_edge_cases, table_name_with_dots, table_name_with_spaces, "
        "CamelCaseStream, wide_table_50_cols, empty_stream, single_record_stream, "
        "unicode_special_strings, no_pk_stream, long_column_names, large_batch_stream."
    ),
)
@click.option(
    "--custom-scenarios",
    type=str,
    default=None,
    help=(
        "Path to a JSON or YAML file containing additional custom test scenarios. "
        "Each scenario should define 'name', 'json_schema', and optionally 'records' "
        "and 'primary_key'. These are unioned with the predefined scenarios."
    ),
)
def destination_smoke_test(
    *,
    destination: str,
    config: str | None = None,
    pip_url: str | None = None,
    use_python: str | None = None,
    scenarios: str = "all",
    custom_scenarios: str | None = None,
) -> None:
    """Run smoke tests against a destination connector.

    Sends synthetic test data from the smoke test source to the specified
    destination and reports success or failure. The smoke test source
    generates data across predefined scenarios covering common destination
    failure patterns: type variations, null handling, naming edge cases,
    schema variations, and batch sizes.

    This command does NOT read back data from the destination or compare
    results. It only verifies that the destination accepts the data without
    errors.

    Usage examples:

    `pyab destination-smoke-test --destination=destination-dev-null`

    `pyab destination-smoke-test --destination=destination-snowflake
    --config=./secrets/snowflake.json`

    `pyab destination-smoke-test --destination=destination-motherduck
    --scenarios=basic_types_stream,null_values_stream`
    """
    click.echo("Resolving destination...", file=sys.stderr)
    destination_obj = _resolve_destination_job(
        destination=destination,
        config=config,
        pip_url=pip_url,
        use_python=use_python,
    )

    click.echo("Creating smoke test source...", file=sys.stderr)
    source_obj = _get_smoke_test_source(
        scenarios=scenarios,
        custom_scenarios=custom_scenarios,
    )

    click.echo("Running destination smoke test...", file=sys.stderr)
    start_time = time.monotonic()
    success = False
    error_message: str | None = None
    records_delivered = 0
    try:
        write_result = destination_obj.write(
            source_data=source_obj,
            cache=False,
            state_cache=False,
        )
        records_delivered = write_result.processed_records
        success = True
    except Exception as ex:
        error_message = (
            f"{type(ex).__name__}: {ex.get_message() if hasattr(ex, 'get_message') else ex}"
        )
        click.echo(f"Smoke test FAILED: {error_message}", file=sys.stderr)

    elapsed = time.monotonic() - start_time

    result = {
        "success": success,
        "destination": destination_obj.name,
        "records_delivered": records_delivered,
        "scenarios_requested": scenarios,
        "elapsed_seconds": round(elapsed, 2),
    }
    if error_message:
        result["error"] = error_message

    click.echo(json.dumps(result, indent=2))

    if not success:
        sys.exit(1)


@click.group()
def cli() -> None:
    """@private PyAirbyte CLI."""
    pass


cli.add_command(validate)
cli.add_command(benchmark)
cli.add_command(sync)
cli.add_command(destination_smoke_test)

if __name__ == "__main__":
    cli()
