# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests for the `DockerExecutor` CLI arg mapping."""

from __future__ import annotations

from pathlib import Path

from airbyte._executors.docker import (
    DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR,
    DockerExecutor,
)


def _make_executor(local_volume: Path) -> DockerExecutor:
    return DockerExecutor(
        name="source-faker",
        image_name_full="airbyte/source-faker:latest",
        executable=["docker", "run", "airbyte/source-faker:latest"],
        volumes={local_volume: DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR},
    )


def test_map_cli_args_emits_posix_container_paths(tmp_path: Path) -> None:
    r"""Mapped paths must use POSIX separators, since the container is always Linux.

    Regression test: on Windows, joining via `pathlib.Path` produced backslash paths
    such as `\airbyte\tmp\config.json`, which the connector could not open.
    """
    config_file = tmp_path / "config.json"
    config_file.write_text("{}")

    executor = _make_executor(tmp_path)
    mapped = executor.map_cli_args(["check", "--config", str(config_file)])

    assert mapped == ["check", "--config", "/airbyte/tmp/config.json"]


def test_map_cli_args_maps_nested_paths_with_posix_separators(tmp_path: Path) -> None:
    """Nested files below the volume root keep POSIX separators in every segment."""
    nested_dir = tmp_path / "sub" / "dir"
    nested_dir.mkdir(parents=True)
    catalog_file = nested_dir / "catalog.json"
    catalog_file.write_text("{}")

    executor = _make_executor(tmp_path)
    mapped = executor.map_cli_args([str(catalog_file)])

    assert mapped == ["/airbyte/tmp/sub/dir/catalog.json"]


def test_map_cli_args_leaves_non_path_args_untouched(tmp_path: Path) -> None:
    """Args that are not existing local files pass through unchanged."""
    executor = _make_executor(tmp_path)
    args = ["read", "--state", "--catalog"]

    assert executor.map_cli_args(args) == args


def test_map_cli_args_passes_through_unmapped_paths(tmp_path: Path) -> None:
    """A file outside every mapped volume is passed through as-is."""
    volume_dir = tmp_path / "volume"
    volume_dir.mkdir()
    outside_file = tmp_path / "outside.json"
    outside_file.write_text("{}")

    executor = _make_executor(volume_dir)
    mapped = executor.map_cli_args([str(outside_file)])

    assert mapped == [str(outside_file)]
