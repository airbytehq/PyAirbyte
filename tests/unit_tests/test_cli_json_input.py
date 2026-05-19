# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for CLI JSON input helpers."""

from __future__ import annotations

import inspect
import json
from typing import Annotated

import pytest
from cyclopts import App, Parameter

from airbyte.cli import _input
from airbyte.cli.cloud import connections, sources
from airbyte.exceptions import PyAirbyteInputError


def _invoke_command(command, args: list[str]) -> None:
    """Invoke a Cyclopts command and allow successful exits."""
    app = App()
    app.command(command)
    try:
        app(["create", *args])
    except SystemExit as exc:
        if exc.code not in (0, None):
            raise


def test_parse_json_input_options(tmp_path) -> None:
    """`parse_json_input_options` parses inline JSON and JSON files."""
    json_file = tmp_path / "input.json"
    json_file.write_text('{"name": "from-file"}', encoding="utf-8")
    relative_json_file = tmp_path / "relative.json"
    relative_json_file.write_text('{"name": "from-relative-file"}', encoding="utf-8")

    assert _input.parse_json_input_options(json_input='{"name": "inline"}') == {
        "name": "inline"
    }
    assert _input.parse_json_input_options(json_input='  {"name": "inline"}') == {
        "name": "inline"
    }
    assert _input.parse_json_input_options(json_file=json_file) == {"name": "from-file"}
    assert _input.parse_json_input_options(json_input=f"@{json_file}") == {
        "name": "from-file"
    }
    assert _input.parse_json_input_options(json_input=f"  @{json_file}") == {
        "name": "from-file"
    }
    assert _input.parse_json_input_options(json_input=json_file.as_posix()) == {
        "name": "from-file"
    }
    assert _input.parse_json_input_options() == {}

    with pytest.raises(PyAirbyteInputError, match="Only one JSON input source"):
        _input.parse_json_input_options(json_input="{}", json_file=json_file)

    with pytest.raises(PyAirbyteInputError, match="JSON input must be an object"):
        _input.parse_json_input_options(json_input="[]")

    with pytest.raises(PyAirbyteInputError, match="JSON input must be an object"):
        _input.parse_json_input_options(json_input='  ["not-an-object"]')

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.chdir(tmp_path)
        assert _input.parse_json_input_options(json_input="./relative.json") == {
            "name": "from-relative-file"
        }


@pytest.mark.parametrize(
    "json_values,named_values,required_fields,json_string_fields,expected",
    [
        pytest.param(
            {"name": "json-name"},
            {"name": _input._MISSING},  # noqa: SLF001
            {"name"},
            set(),
            {"name": "json-name"},
            id="json_supplies_required_value",
        ),
        pytest.param(
            {"config_json": json.dumps({"host": "localhost"})},
            {"config_json": '{"host":"localhost"}'},
            set(),
            {"config_json"},
            {},
            id="json_string_conflict_uses_semantic_equality",
        ),
    ],
)
def test_resolve_json_input(
    json_values: dict[str, object],
    named_values: dict[str, object],
    required_fields: set[str],
    json_string_fields: set[str],
    expected: dict[str, object],
) -> None:
    """`resolve_json_input` merges JSON values into named CLI values."""
    assert (
        _input.resolve_json_input(
            json_values,
            named_values,
            required_fields=required_fields,
            json_string_fields=json_string_fields,
        )
        == expected
    )


@pytest.mark.parametrize(
    "json_values,named_values,required_fields,match",
    [
        pytest.param(
            {"extra": "value"},
            {"name": _input._MISSING},  # noqa: SLF001
            {"name"},
            "JSON input field is not supported",
            id="unsupported_field",
        ),
        pytest.param(
            {"name": "json-name"},
            {"name": "named-name"},
            {"name"},
            "JSON input value conflicts with named CLI argument",
            id="conflicting_named_value",
        ),
        pytest.param(
            {},
            {"name": _input._MISSING},  # noqa: SLF001
            {"name"},
            "Required command input is missing",
            id="missing_required_value",
        ),
    ],
)
def test_resolve_json_input_errors(
    json_values: dict[str, object],
    named_values: dict[str, object],
    required_fields: set[str],
    match: str,
) -> None:
    """`resolve_json_input` rejects ambiguous or incomplete input."""
    with pytest.raises(PyAirbyteInputError, match=match):
        _input.resolve_json_input(
            json_values,
            named_values,
            required_fields=required_fields,
        )


def test_wrapped_cli_patches_cyclopts_signature() -> None:
    """`wrapped_cli` exposes JSON options and makes required args optional."""

    @_input.wrapped_cli
    def command(*, name: Annotated[str, Parameter(help="Name")]) -> str:
        return name

    signature = inspect.signature(command)

    assert signature.parameters["name"].default is None
    assert "json_input" in signature.parameters
    assert "json_file" in signature.parameters
    assert command(json_input='{"name": "json-name"}') == "json-name"  # type: ignore[call-arg]


def test_source_create_help_includes_json_options(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`sources create --help` includes JSON input options."""
    app = App()
    app.command(sources.create)

    with pytest.raises(SystemExit) as exc_info:
        app(["create", "--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "--json" in output
    assert "--json-file" in output


def test_connection_create_help_includes_json_options(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`connections create --help` includes JSON input options."""
    app = App()
    app.command(connections.create)

    with pytest.raises(SystemExit) as exc_info:
        app(["create", "--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "--json" in output
    assert "--json-file" in output


def test_source_create_accepts_json_path_input(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`sources create` accepts command file input from `--json`."""
    calls = {}
    outputs = []
    json_file = tmp_path / "source.json"
    json_file.write_text(
        json.dumps({
            "name": "Test Source",
            "source_type": "postgres",
            "config": {"host": "localhost"},
        }),
        encoding="utf-8",
    )

    class FakeSource:
        def get_info(self) -> dict[str, object]:
            return {"sourceId": "source-id"}

    class FakeWorkspace:
        def __init__(self, **kwargs: object) -> None:
            calls["workspace"] = kwargs

        def deploy_source(self, *, name: str, config: dict[str, object]) -> FakeSource:
            calls["deploy_source"] = {"name": name, "config": config}
            return FakeSource()

    monkeypatch.setattr(sources, "CloudWorkspace", FakeWorkspace)
    monkeypatch.setattr(sources, "json_output", outputs.append)

    _invoke_command(
        sources.create,
        [
            "--json",
            f"@{json_file}",
        ],
    )

    assert calls["workspace"] == {
        "workspace_id": None,
        "client_id": None,
        "client_secret": None,
        "api_root": None,
    }
    assert calls["deploy_source"] == {
        "name": "Test Source",
        "config": {"host": "localhost", "sourceType": "postgres"},
    }
    assert outputs == [{"sourceId": "source-id"}]


def test_source_create_accepts_named_input(monkeypatch: pytest.MonkeyPatch) -> None:
    """`sources create` accepts command input from named CLI args."""
    calls = {}
    outputs = []

    class FakeSource:
        def get_info(self) -> dict[str, object]:
            return {"sourceId": "source-id"}

    class FakeWorkspace:
        def __init__(self, **kwargs: object) -> None:
            calls["workspace"] = kwargs

        def deploy_source(self, *, name: str, config: dict[str, object]) -> FakeSource:
            calls["deploy_source"] = {"name": name, "config": config}
            return FakeSource()

    monkeypatch.setattr(sources, "CloudWorkspace", FakeWorkspace)
    monkeypatch.setattr(sources, "json_output", outputs.append)

    _invoke_command(
        sources.create,
        [
            "--name",
            "Named Source",
            "--source-type",
            "postgres",
            "--config-json",
            '{"host":"localhost"}',
        ],
    )

    assert calls["workspace"] == {
        "workspace_id": None,
        "client_id": None,
        "client_secret": None,
        "api_root": None,
    }
    assert calls["deploy_source"] == {
        "name": "Named Source",
        "config": {"host": "localhost", "sourceType": "postgres"},
    }
    assert outputs == [{"sourceId": "source-id"}]


@pytest.mark.parametrize(
    "args,expected_name",
    [
        pytest.param(
            [
                "--json",
                json.dumps({
                    "name": "Test Connection",
                    "source_id": "source-id",
                    "destination_id": "destination-id",
                    "prefix": "raw_",
                    "selected_streams": ["users", "orders"],
                }),
            ],
            "Test Connection",
            id="json_input",
        ),
        pytest.param(
            [
                "--name",
                "Named Connection",
                "--source-id",
                "source-id",
                "--destination-id",
                "destination-id",
                "--prefix",
                "raw_",
                "--selected-streams",
                "users,orders",
            ],
            "Named Connection",
            id="named_args",
        ),
    ],
)
def test_connection_create_accepts_json_and_named_input(
    args: list[str],
    expected_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`connections create` accepts JSON input and named CLI args."""
    calls = {}
    outputs = []

    class FakeConnection:
        def get_info(self) -> dict[str, object]:
            return {"connectionId": "connection-id"}

    class FakeWorkspace:
        def __init__(self, **kwargs: object) -> None:
            calls["workspace"] = kwargs

        def get_source(self, source_id: str) -> str:
            calls["source_id"] = source_id
            return f"source:{source_id}"

        def get_destination(self, destination_id: str) -> str:
            calls["destination_id"] = destination_id
            return f"destination:{destination_id}"

        def deploy_connection(
            self,
            *,
            connection_name: str,
            source: str,
            destination: str,
            table_prefix: str,
            selected_streams: list[str],
        ) -> FakeConnection:
            calls["deploy_connection"] = {
                "connection_name": connection_name,
                "source": source,
                "destination": destination,
                "table_prefix": table_prefix,
                "selected_streams": selected_streams,
            }
            return FakeConnection()

    monkeypatch.setattr(connections, "CloudWorkspace", FakeWorkspace)
    monkeypatch.setattr(connections, "json_output", outputs.append)

    _invoke_command(connections.create, args)

    assert calls["workspace"] == {
        "workspace_id": None,
        "client_id": None,
        "client_secret": None,
        "api_root": None,
    }
    assert calls["source_id"] == "source-id"
    assert calls["destination_id"] == "destination-id"
    assert calls["deploy_connection"] == {
        "connection_name": expected_name,
        "source": "source:source-id",
        "destination": "destination:destination-id",
        "table_prefix": "raw_",
        "selected_streams": ["users", "orders"],
    }
    assert outputs == [{"connectionId": "connection-id"}]
