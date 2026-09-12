# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests for `DeclarativeExecutor` config handling."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from airbyte._executors.declarative import DeclarativeExecutor

MINIMAL_MANIFEST: dict[str, Any] = {
    "version": "0.1.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": []},
    "streams": [],
}

MANIFEST: dict[str, Any] = {
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


@pytest.fixture
def executor() -> DeclarativeExecutor:
    return DeclarativeExecutor(name="source-test", manifest=dict(MINIMAL_MANIFEST))


def _write_config(tmp_path: Path, config: dict[str, Any]) -> str:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    return str(config_path)


def test_config_from_args_reads_the_config_file(
    executor: DeclarativeExecutor,
    tmp_path: Path,
) -> None:
    """The connector config must be read back from the `--config` arg.

    Regression test: the executor previously built the declarative source with an
    empty config, so manifest interpolations such as `{{ config["client_id"] }}`
    resolved to empty strings and connectors failed to authenticate.
    """
    config = {"client_id": "abc", "client_secret": "shh"}
    args = ["check", "--config", _write_config(tmp_path, config)]

    assert executor._config_from_args(args) == config


def test_config_from_args_with_non_object_json(
    executor: DeclarativeExecutor,
    tmp_path: Path,
) -> None:
    """A config file with a non-object JSON root is ignored."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(["invalid"]))

    assert executor._config_from_args(["check", "--config", str(config_path)]) == {}


def test_config_from_args_without_config_flag(executor: DeclarativeExecutor) -> None:
    """`spec` takes no config, so there is nothing to read."""
    assert executor._config_from_args(["spec"]) == {}


def test_config_from_args_with_missing_file(executor: DeclarativeExecutor) -> None:
    """A missing config path is tolerated rather than raising."""
    assert (
        executor._config_from_args(["check", "--config", "does_not_exist.json"]) == {}
    )


def test_declarative_source_receives_connector_config(
    executor: DeclarativeExecutor,
    mocker: Any,
) -> None:
    """The config read from args must reach the underlying CDK source."""
    captured: dict[str, Any] = {}

    def _capture(*_args: Any, **kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    mocker.patch(
        "airbyte._executors.declarative.ConcurrentDeclarativeSource",
        side_effect=_capture,
    )

    config = {"client_id": "abc"}
    executor._build_declarative_source(config)

    assert captured["config"]["client_id"] == "abc"


def test_injected_components_are_preserved(mocker: Any) -> None:
    """Connector config must merge with, not replace, injected components."""
    captured: dict[str, Any] = {}

    def _capture(*_args: Any, **kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    mocker.patch(
        "airbyte._executors.declarative.ConcurrentDeclarativeSource",
        side_effect=_capture,
    )

    executor = DeclarativeExecutor(
        name="source-test",
        manifest=dict(MINIMAL_MANIFEST),
        components_py="# components",
    )
    executor._build_declarative_source({"client_id": "abc"})

    assert captured["config"]["client_id"] == "abc"
    assert captured["config"]["__injected_components_py"] == "# components"


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


def test_state_reaches_the_state_manager(tmp_path: Path) -> None:
    """The cursor supplied via `--state` must reach the source's state manager."""
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


def test_no_state_arg_leaves_the_manager_empty(tmp_path: Path) -> None:
    """A first sync passes no `--state`; that must not error."""
    executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
    args = ["read", "--config", str(_config_file(tmp_path))]

    source = executor._build_declarative_source(
        executor._config_from_args(args),
        state=executor._state_from_args(args),
    )

    assert source._connector_state_manager.get_stream_state("items", None) == {}


def test_state_is_not_reused_between_executions(tmp_path: Path) -> None:
    """A later command without `--state` must not inherit an earlier cursor."""
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
def test_malformed_state_arg_is_not_fatal(args: list[str]) -> None:
    """Argument validation belongs to the CDK entrypoint, not here."""
    executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
    assert executor._state_from_args(args) is None


def test_catalog_reaches_the_source(tmp_path: Path) -> None:
    """The catalog is a constructor argument too, for the same reason."""
    executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
    args = ["read", "--catalog", str(_catalog_file(tmp_path))]

    catalog = executor._catalog_from_args(args)
    assert catalog is not None
    assert [s.stream.name for s in catalog.streams] == ["items"]


def test_interpolated_token_resolves_from_config(tmp_path: Path) -> None:
    """`{{ config['api_key'] }}` must produce the real config value."""
    executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
    args = ["check", "--config", str(_config_file(tmp_path, "secret-value"))]

    source = executor._build_declarative_source(executor._config_from_args(args))
    stream = source.streams(executor._config_from_args(args))[0]

    authenticator = stream._stream_partition_generator._partition_factory._retriever.requester.authenticator
    assert authenticator.token_provider.get_token() == "secret-value"


def test_missing_config_does_not_resolve_to_a_stale_value(tmp_path: Path) -> None:
    """Config is read per call, so a second command cannot inherit the first."""
    executor = DeclarativeExecutor(name="source-test", manifest=MANIFEST)
    first = ["check", "--config", str(_config_file(tmp_path, "secret-value"))]
    assert executor._config_from_args(first) == {"api_key": "secret-value"}
    assert executor._config_from_args(["spec"]) == {}
