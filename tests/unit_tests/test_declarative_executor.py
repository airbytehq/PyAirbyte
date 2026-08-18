from __future__ import annotations

import json
from typing import Any

from airbyte._executors import declarative
from airbyte._executors.declarative import DeclarativeExecutor
from airbyte.sources import util as sources_util


def test_get_source_passes_config_to_declarative_executor(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    manifest = {"version": "1.0.0"}
    config = {"api_key": "configured"}

    def fake_get_connector_executor(**kwargs: Any) -> DeclarativeExecutor:
        captured.update(kwargs)
        return DeclarativeExecutor(
            name=kwargs["name"],
            manifest=kwargs["source_manifest"],
            config=kwargs["config"],
            components_py="class Component:\n    pass\n",
        )

    monkeypatch.setattr(
        sources_util,
        "get_connector_executor",
        fake_get_connector_executor,
    )
    monkeypatch.setattr(
        declarative,
        "ConcurrentDeclarativeSource",
        lambda **kwargs: kwargs,
    )

    source = sources_util.get_source(
        name="source-test",
        config=config,
        source_manifest=manifest,
    )

    declarative_config = source.executor.declarative_source["config"]
    assert declarative_config["api_key"] == config["api_key"]
    assert declarative_config["__injected_components_py"]
    assert captured["config"] == config
    assert config == {"api_key": "configured"}


def test_execute_uses_config_set_after_get_source_and_preserves_injected_components(
    monkeypatch,
    tmp_path,
) -> None:
    captured: dict[str, Any] = {}
    manifest = {"version": "1.0.0"}
    late_config = {"api_key": "configured-later"}

    def fake_get_connector_executor(**kwargs: Any) -> DeclarativeExecutor:
        return DeclarativeExecutor(
            name=kwargs["name"],
            manifest=kwargs["source_manifest"],
            config=kwargs["config"],
            components_py="class Component:\n    pass\n",
        )

    class FakeEntrypoint:
        def __init__(self, source: Any) -> None:
            captured["source"] = source

        def parse_args(self, args: list[str]) -> list[str]:
            return args

        def run(self, args: list[str]):
            yield from args

    monkeypatch.setattr(
        sources_util,
        "get_connector_executor",
        fake_get_connector_executor,
    )
    monkeypatch.setattr(
        declarative,
        "ConcurrentDeclarativeSource",
        lambda **kwargs: kwargs,
    )
    monkeypatch.setattr(declarative, "AirbyteEntrypoint", FakeEntrypoint)

    source = sources_util.get_source(
        name="source-test",
        source_manifest=manifest,
    )
    source.set_config(late_config, validate=False)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(source._hydrated_config))

    list(source.executor.execute(["read", "--config", str(config_path)]))

    config = captured["source"]["config"]
    assert config["api_key"] == late_config["api_key"]
    assert config["__injected_components_py"] == "class Component:\n    pass\n"
    assert config["__injected_components_py_checksums"]["md5"]


def test_declarative_executor_copies_config_before_component_injection() -> None:
    config = {"api_key": "configured"}

    DeclarativeExecutor(
        name="source-test",
        manifest={"version": "1.0.0"},
        config=config,
        components_py="class Component:\n    pass\n",
    )

    assert config == {"api_key": "configured"}
