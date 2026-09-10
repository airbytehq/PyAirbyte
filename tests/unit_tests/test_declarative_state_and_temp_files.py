# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Incremental state reaching the declarative source, and writable temp configs.

Two defects, both invisible from the outside:

* `ConcurrentDeclarativeSource` builds its `ConnectorStateManager` in `__init__`
  and its cursors in `streams()`, while the `state` argument to `read()` is
  accepted and never used. A source constructed without state re-reads every
  stream from its start date, so incremental sync silently becomes a full
  refresh -- and with an append write strategy, the destination accumulates a
  duplicate copy on every run.

* `as_temp_files()` wrote config files read-only, so connectors declaring
  `config_migrations` raised PermissionError when rewriting them in place.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from airbyte._executors.declarative import DeclarativeExecutor
from airbyte._executors.docker import DockerExecutor
from airbyte._util.temp_files import as_temp_files


MANIFEST = {
    "version": "4.6.2",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["items"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "items",
            "primary_key": ["id"],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://example.com",
                    "path": "/items",
                    # The interpolation the config fix exists to make work.
                    "authenticator": {
                        "type": "BearerAuthenticator",
                        "api_token": "{{ config['api_key'] }}",
                    },
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": ["data"]},
                },
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                },
            },
        }
    ],
    "spec": {
        "type": "Spec",
        "connection_specification": {
            "type": "object",
            "required": ["api_key"],
            "properties": {"api_key": {"type": "string"}},
        },
    },
}


def _config_file(tmp_path: Path, api_key: str = "secret-value") -> Path:
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"api_key": api_key}), encoding="utf-8")
    return path


def _state_file(tmp_path: Path, cursor: str) -> Path:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps([
            {
                "type": "STREAM",
                "stream": {
                    "stream_descriptor": {"name": "items"},
                    "stream_state": {"updated_at": cursor},
                },
            }
        ]),
        encoding="utf-8",
    )
    return path


def _catalog_file(tmp_path: Path) -> Path:
    path = tmp_path / "catalog.json"
    path.write_text(
        json.dumps({
            "streams": [
                {
                    "stream": {
                        "name": "items",
                        "json_schema": {},
                        "supported_sync_modes": ["full_refresh"],
                    },
                    "sync_mode": "full_refresh",
                    "destination_sync_mode": "overwrite",
                }
            ]
        }),
        encoding="utf-8",
    )
    return path


class TestDeclarativeState:
    """State supplied as `--state` has to reach the source's constructor."""

    def test_state_reaches_the_state_manager(self, tmp_path: Path) -> None:
        """The cursor must be readable from the constructed source.

        This is the whole defect: without it the state manager is empty and the
        connector restarts from its configured start date.
        """
        executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
        args = [
            "read",
            "--config",
            str(_config_file(tmp_path)),
            "--state",
            str(_state_file(tmp_path, "2026-05-05T00:00:00Z")),
        ]

        source = executor._build_declarative_source(
            executor._config_from_args(args),
            state=executor._state_from_args(args),
            catalog=executor._catalog_from_args(args),
        )

        stored = source._connector_state_manager.get_stream_state("items", None)
        assert stored == {"updated_at": "2026-05-05T00:00:00Z"}

    def test_no_state_arg_leaves_the_manager_empty(self, tmp_path: Path) -> None:
        """A first sync passes no `--state`; that must not error."""
        executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
        args = ["read", "--config", str(_config_file(tmp_path))]

        source = executor._build_declarative_source(
            executor._config_from_args(args),
            state=executor._state_from_args(args),
        )

        assert source._connector_state_manager.get_stream_state("items", None) == {}

    def test_state_is_not_reused_between_executions(self, tmp_path: Path) -> None:
        """A later command without `--state` must not inherit an earlier cursor.

        One executor serves every command, and `spec`, `check` and `discover`
        pass no `--state`. Reading the flag per call rather than storing it on
        the executor makes reuse structurally impossible; this pins that.
        """
        executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
        with_state = [
            "read",
            "--state",
            str(_state_file(tmp_path, "2026-05-05T00:00:00Z")),
        ]
        assert executor._state_from_args(with_state) is not None

        assert executor._state_from_args(["discover", "--config", "x.json"]) is None
        assert executor._catalog_from_args(["check", "--config", "x.json"]) is None

    @pytest.mark.parametrize(
        "args",
        [
            ["read", "--state"],  # flag with no value
            ["read", "--state", "does-not-exist.json"],  # missing file
        ],
    )
    def test_malformed_state_arg_is_not_fatal(self, args: list[str]) -> None:
        """Argument validation belongs to the CDK entrypoint, not here."""
        executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
        assert executor._state_from_args(args) is None

    def test_catalog_reaches_the_source(self, tmp_path: Path) -> None:
        """The catalog is a constructor argument too, for the same reason."""
        executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
        args = ["read", "--catalog", str(_catalog_file(tmp_path))]

        catalog = executor._catalog_from_args(args)
        assert catalog is not None
        assert [s.stream.name for s in catalog.streams] == ["items"]


class TestConfigActuallyResolves:
    """Asserting on a private dict proves nothing; resolve the interpolation."""

    def test_interpolated_token_resolves_from_config(self, tmp_path: Path) -> None:
        """`{{ config['api_key'] }}` must produce the real value.

        The original symptom was `'dict object' has no attribute 'api_key'`, so
        the meaningful assertion is on the token the authenticator resolves --
        not on whether a dict happens to hold the key.
        """
        executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
        args = ["check", "--config", str(_config_file(tmp_path, "secret-value"))]

        source = executor._build_declarative_source(executor._config_from_args(args))
        stream = source.streams(executor._config_from_args(args))[0]

        authenticator = stream._stream_partition_generator._partition_factory._retriever.requester.authenticator
        assert authenticator.token_provider.get_token() == "secret-value"

    def test_missing_config_does_not_resolve_to_a_stale_value(
        self, tmp_path: Path
    ) -> None:
        """Config is read per call, so a second command cannot inherit the first."""
        executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
        first = ["check", "--config", str(_config_file(tmp_path, "secret-value"))]
        assert executor._config_from_args(first) == {"api_key": "secret-value"}
        assert executor._config_from_args(["spec"]) == {}


class TestTempFilePermissions:
    """`config_migrations` rewrites the config file in place."""

    def test_temp_config_can_be_rewritten(self) -> None:
        """The real failure: PermissionError on a read-only config."""
        with as_temp_files([{"api_key": "before"}]) as (path,):
            Path(path).write_text(json.dumps({"api_key": "after"}), encoding="utf-8")
            assert json.loads(Path(path).read_text(encoding="utf-8")) == {
                "api_key": "after"
            }

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
    def test_group_and_other_cannot_write(self) -> None:
        """Owner write only -- the file holds secrets (CWE-732)."""
        with as_temp_files([{"api_key": "value"}]) as (path,):
            mode = Path(path).stat().st_mode
            assert mode & stat.S_IWUSR, "owner must be able to write"
            assert not mode & stat.S_IWGRP, "group must not be able to write"
            assert not mode & stat.S_IWOTH, "other must not be able to write"


class _WindowsPath(PureWindowsPath):
    """Windows path semantics on any host, so the test runs in Linux CI.

    `map_cli_args` calls `.exists()` and `.is_relative_to()`. Only the former
    needs stubbing; the rest is pure path arithmetic, which is exactly what the
    fix changed.
    """

    def exists(self) -> bool:
        return True


class TestDockerPathsOnWindowsHosts:
    """Container paths must be POSIX regardless of the host's path flavour."""

    def test_windows_host_produces_posix_container_path(self, monkeypatch) -> None:
        """Without the fix this yields `\\airbyte\\tmp\\config.json`.

        Asserting `"\\\\" not in result` on a Linux runner is vacuous: `Path` is
        already `PosixPath` there, so the assertion holds against the unfixed
        code too. Forcing Windows semantics is what gives the test teeth in CI.
        """
        import airbyte._executors.docker as docker_module

        monkeypatch.setattr(docker_module, "Path", _WindowsPath)

        volume = _WindowsPath(r"C:\Users\dev\AppData\Local\Temp")
        executor = DockerExecutor(
            name="source-test",
            image_name_full="airbyte/source-test:latest",
            executable=["docker", "run", "airbyte/source-test:latest"],
            volumes={volume: "/airbyte/tmp"},
        )

        mapped = executor.map_cli_args([
            "read",
            r"C:\Users\dev\AppData\Local\Temp\config.json",
        ])

        assert "\\" not in mapped[1], f"backslash leaked into {mapped[1]!r}"
        assert mapped[1] == str(PurePosixPath("/airbyte/tmp/config.json"))

    def test_non_path_args_pass_through(self, tmp_path: Path) -> None:
        """Arguments that are not existing files must be untouched."""
        executor = DockerExecutor(
            name="source-test",
            image_name_full="airbyte/source-test:latest",
            executable=["docker", "run", "airbyte/source-test:latest"],
            volumes={tmp_path: "/airbyte/tmp"},
        )
        assert executor.map_cli_args(["read", "--catalog"]) == ["read", "--catalog"]
