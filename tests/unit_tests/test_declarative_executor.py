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
    tmp_path: Path,
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


def test_injected_components_are_preserved(tmp_path: Path, mocker: Any) -> None:
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
