# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""CLI interfaces for PyAirbyte."""

from __future__ import annotations

import json
from pathlib import Path

import click

from airbyte import get_source


@click.command()
@click.option("--pip-url", help="The Pip URL to install the package from.")
@click.option("--name", help="The name of the source.")
@click.option("--config", help="The path to a config JSON for the source.")
@click.option("--perf-mb-per-sec-min", type=float, help="Fail if slower than this.")
@click.option("--perf-baseline", help="Fail if slower than this.")
@click.option("--perf-std-dev-max", type=float, help="Fail if slower than this.")
@click.option("--max-retries", type=int, help="Max retries.")
def test_source(
    name: str,
    pip_url: str,
    config: str,
    perf_mb_per_sec_min: float,
    perf_baseline: str,
    perf_std_dev_max: float,
    max_retries: int,
) -> None:
    """Test a source."""
    source = get_source(
        name=name,
        pip_url=pip_url,
        config=json.loads(Path(config).read_text()),
        streams="*",
    )

    source.check()
    for _ in range(max_retries):
        try:
            _ = source.read()
        except Exception as e:
            print(e)
            continue
