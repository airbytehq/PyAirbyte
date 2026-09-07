# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Regression tests for executor config propagation and container path mapping."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path, PurePosixPath

import pytest

from airbyte._executors.declarative import DeclarativeExecutor
from airbyte._executors.docker import DockerExecutor
from airbyte._util.temp_files import as_temp_files


MINIMAL_MANIFEST = {
    "version": "4.6.2",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": []},
    "streams": [],
    "spec": {
        "type": "Spec",
        "connection_specification": {
            "type": "object",
            "properties": {"api_key": {"type": "string"}},
        },
    },
}


class TestDeclarativeExecutorConfig:
    """`{{ config[...] }}` cannot resolve unless the executor sees the real config."""

    def test_config_from_args_is_merged(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.json"
        config_file.write_text(
            json.dumps({"api_key": "secret-value"}), encoding="utf-8"
        )

        executor = DeclarativeExecutor(name="source-test", manifest=MINIMAL_MANIFEST)
        assert "api_key" not in executor._config_dict

        executor._load_config_from_args(["check", "--config", str(config_file)])

        assert executor._config_dict["api_key"] == "secret-value"

    def test_injected_components_are_not_overwritten(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.json"
        config_file.write_text(
            json.dumps({"api_key": "x", "__injected_components_py": "MALICIOUS"}),
            encoding="utf-8",
        )

        executor = DeclarativeExecutor(
            name="source-test",
            manifest=MINIMAL_MANIFEST,
            components_py="# real components",
        )
        executor._load_config_from_args(["check", "--config", str(config_file)])

        assert executor._config_dict["__injected_components_py"] == "# real components"
        assert executor._config_dict["api_key"] == "x"

    @pytest.mark.parametrize(
        "args",
        [
            ["spec"],  # no --config at all
            ["check", "--config"],  # --config with no value
        ],
    )
    def test_missing_config_arg_is_a_noop(self, args: list[str]) -> None:
        executor = DeclarativeExecutor(name="source-test", manifest=MINIMAL_MANIFEST)
        before = dict(executor._config_dict)
        executor._load_config_from_args(args)
        assert executor._config_dict == before

    def test_unreadable_config_does_not_raise(self, tmp_path: Path) -> None:
        executor = DeclarativeExecutor(name="source-test", manifest=MINIMAL_MANIFEST)
        executor._load_config_from_args([
            "check",
            "--config",
            str(tmp_path / "nope.json"),
        ])
        # The entrypoint reports the real error; the executor must not mask it.
        assert "api_key" not in executor._config_dict


class TestTempFilePermissions:
    """Connectors declaring `config_migrations` rewrite the config file in place."""

    def test_temp_files_are_writable(self) -> None:
        with as_temp_files([{"api_key": "x"}]) as [path]:
            mode = os.stat(path).st_mode
            assert mode & stat.S_IWUSR, "owner cannot write"
            assert mode & stat.S_IROTH, "other cannot read"
            if os.name != "nt":
                assert mode & stat.S_IWOTH, "container uid cannot write"

    def test_temp_file_can_actually_be_rewritten(self) -> None:
        with as_temp_files([{"api_key": "x"}]) as [path]:
            Path(path).write_text(json.dumps({"api_key": "migrated"}), encoding="utf-8")
            assert (
                json.loads(Path(path).read_text(encoding="utf-8"))["api_key"]
                == "migrated"
            )


class TestDockerPathMapping:
    """Container paths must be POSIX regardless of the host OS."""

    def test_mapped_path_is_posix(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.json"
        config_file.write_text("{}", encoding="utf-8")

        executor = DockerExecutor(
            name="source-test",
            image_name_full="airbyte/source-test:latest",
            executable=["docker", "run", "airbyte/source-test:latest"],
            volumes={tmp_path: "/airbyte/tmp"},
        )
        mapped = executor.map_cli_args(["check", "--config", str(config_file)])

        container_path = mapped[-1]
        assert "\\" not in container_path, f"backslash leaked into {container_path!r}"
        assert container_path == str(PurePosixPath("/airbyte/tmp/config.json"))

    def test_non_path_args_pass_through(self, tmp_path: Path) -> None:
        executor = DockerExecutor(
            name="source-test",
            image_name_full="airbyte/source-test:latest",
            executable=["docker", "run", "airbyte/source-test:latest"],
            volumes={tmp_path: "/airbyte/tmp"},
        )
        assert executor.map_cli_args(["read", "--catalog"]) == ["read", "--catalog"]
