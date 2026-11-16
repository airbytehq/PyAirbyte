# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests for the `DockerExecutor` CLI arg mapping and volume mounts."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from airbyte._executors.docker import (
    DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR,
    DockerExecutor,
    get_local_mount_dir,
)
from airbyte._util.meta import is_windows
from airbyte.caches._utils._dest_to_cache import duckdb_destination_to_cache


@pytest.fixture
def read_only_dir(tmp_path: Path) -> Path:
    """Return an existing directory that the current user cannot create files in."""
    if is_windows() or os.geteuid() == 0:
        pytest.skip("Directory permissions are not enforced on Windows or for root.")
    path = tmp_path / "read-only"
    path.mkdir()
    path.chmod(0o555)
    return path


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

    assert mapped == [
        "check",
        "--config",
        f"{DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR}/config.json",
    ]


def test_map_cli_args_maps_nested_paths_with_posix_separators(tmp_path: Path) -> None:
    """Nested files below the volume root keep POSIX separators in every segment."""
    nested_dir = tmp_path / "sub" / "dir"
    nested_dir.mkdir(parents=True)
    catalog_file = nested_dir / "catalog.json"
    catalog_file.write_text("{}")

    executor = _make_executor(tmp_path)
    mapped = executor.map_cli_args([str(catalog_file)])

    assert mapped == [
        f"{DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR}/sub/dir/catalog.json",
    ]


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


def test_docker_executor_from_read_only_cwd(
    read_only_dir: Path, tmp_path: Path
) -> None:
    """A read-only working directory must not prevent creating a Docker executor.

    Regression test for #792. With `AIRBYTE_PROJECT_DIR` unset, the project dir is
    the working directory at import time, so this runs in a subprocess started from
    a read-only directory.
    """
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    env = {
        **os.environ,
        # Disable the connector registry so the test stays offline.
        "AIRBYTE_LOCAL_REGISTRY": "0",
        "AIRBYTE_TEMP_DIR": str(temp_dir),
    }
    env.pop("AIRBYTE_PROJECT_DIR", None)
    script = """
import json
from airbyte._executors.util import get_connector_executor

executor = get_connector_executor(
    name="source-faker", docker_image="airbyte/source-faker:latest"
)
volumes = {container: str(host) for host, container in executor.volumes.items()}
print(json.dumps(volumes))
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=read_only_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    volumes = json.loads(result.stdout.splitlines()[-1])
    assert Path(volumes["/local"]) == temp_dir / "airbyte" / "source-faker"
    assert Path(volumes["/local"]).is_dir()


def test_get_local_mount_dir_uses_project_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """By default the `/local` volume is `<project dir>/<connector name>`."""
    monkeypatch.setattr("airbyte._executors.docker.DEFAULT_PROJECT_DIR", tmp_path)

    local_mount_dir = get_local_mount_dir("source-faker")

    assert local_mount_dir == tmp_path / "source-faker"
    assert local_mount_dir.is_dir()


def test_get_local_mount_dir_falls_back_to_temp_dir(
    read_only_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the project dir isn't writable, use the temp dir and warn about it."""
    temp_dir = tmp_path / "temp"
    monkeypatch.setattr("airbyte._executors.docker.DEFAULT_PROJECT_DIR", read_only_dir)
    monkeypatch.setattr("airbyte._executors.docker.TEMP_DIR_OVERRIDE", temp_dir)

    with pytest.warns(UserWarning, match="AIRBYTE_PROJECT_DIR"):
        local_mount_dir = get_local_mount_dir("source-faker")

    assert local_mount_dir == temp_dir / "airbyte" / "source-faker"
    assert local_mount_dir.is_dir()


def test_duckdb_destination_to_cache_uses_fallback_mount_dir(
    read_only_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`/local` DuckDB paths resolve to the directory the executor actually mounted."""
    temp_dir = tmp_path / "temp"
    monkeypatch.setattr("airbyte._executors.docker.DEFAULT_PROJECT_DIR", read_only_dir)
    monkeypatch.setattr("airbyte._executors.docker.TEMP_DIR_OVERRIDE", temp_dir)

    with pytest.warns(UserWarning, match="AIRBYTE_PROJECT_DIR"):
        cache = duckdb_destination_to_cache({"destination_path": "/local/db.duckdb"})

    assert (
        Path(cache.db_path) == temp_dir / "airbyte" / "destination-duckdb" / "db.duckdb"
    )
