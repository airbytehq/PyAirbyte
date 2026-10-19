# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Connector Builder projects in Airbyte Cloud."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from airbyte._util import api_util
from airbyte.cloud import connector_builder
from airbyte.cloud.connector_builder import (
    CloudConnectorBuilderProject,
    ConnectorBuilderPageSummary,
    ConnectorBuilderResolvedManifest,
    ConnectorBuilderStreamReadResult,
    apply_manifest_text_edit,
    render_manifest_text,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteDuplicateResourcesError, AirbyteLibInputError
from airbyte.secrets.base import SecretString

MANIFEST: dict[str, Any] = {
    "version": "6.0.0",
    "streams": [{"name": "users"}, {"name": "orders"}],
    "spec": {
        "connection_specification": {"type": "object", "properties": {}},
        "documentation_url": "https://example.com/docs",
    },
}


@pytest.mark.parametrize(
    ("edit_mode", "new_text", "old_text", "expected"),
    [
        pytest.param("replace_full", "b: 2\n", None, "b: 2\n", id="replace-full"),
        pytest.param(
            "replace_text", "y", "x", "a: y\nb: z\n", id="replace-text-single"
        ),
        pytest.param("replace_all", "q", "z", "a: x\nb: q\n", id="replace-all-single"),
    ],
)
def test_apply_manifest_text_edit(
    edit_mode: connector_builder.ManifestTextEditMode,
    new_text: str,
    old_text: str | None,
    expected: str,
) -> None:
    result = apply_manifest_text_edit(
        "a: x\nb: z\n",
        edit_mode=edit_mode,
        new_text=new_text,
        old_text=old_text,
    )
    assert result == expected


def test_apply_manifest_text_edit_replace_all_replaces_every_match() -> None:
    result = apply_manifest_text_edit(
        "a: x\nb: x\n", edit_mode="replace_all", new_text="y", old_text="x"
    )
    assert result == "a: y\nb: y\n"


@pytest.mark.parametrize(
    ("edit_mode", "old_text", "match"),
    [
        pytest.param("replace_text", "x", "matches 2 places", id="multiple-matches"),
        pytest.param("replace_text", "nope", "was not found", id="no-match"),
        pytest.param("replace_all", "nope", "was not found", id="replace-all-no-match"),
        pytest.param("replace_text", None, "is required", id="missing-old-text"),
        pytest.param("replace_text", "", "is required", id="empty-old-text"),
        pytest.param("replace_full", "x", "not allowed", id="old-text-with-full"),
    ],
)
def test_apply_manifest_text_edit_errors(
    edit_mode: connector_builder.ManifestTextEditMode,
    old_text: str | None,
    match: str,
) -> None:
    with pytest.raises(AirbyteLibInputError, match=match):
        apply_manifest_text_edit(
            "a: x\nb: x\n", edit_mode=edit_mode, new_text="y", old_text=old_text
        )


def test_render_manifest_text_keeps_order_and_does_not_wrap() -> None:
    long_value = "word " * 40
    text = render_manifest_text({"z": 1, "a": long_value.strip()})
    assert text.splitlines()[0] == "z: 1"
    assert len(text.splitlines()) == 2


def test_stream_read_result_from_api_response() -> None:
    response = {
        "logs": [{"level": "ERROR", "message": "boom", "stacktrace": "..."}],
        "slices": [
            {
                "pages": [
                    {
                        "records": [{"id": 1}, {"id": 2}],
                        "request": {"url": "https://api/x", "http_method": "GET"},
                        "response": {"status": 200, "body": "ok"},
                    },
                    {
                        "records": [],
                        "request": {
                            "url": "https://api/x?page=2",
                            "http_method": "GET",
                        },
                        "response": {"status": 401, "body": "unauthorized"},
                    },
                ]
            }
        ],
        "test_read_limit_reached": True,
        "inferred_schema": {"type": "object"},
    }

    result = ConnectorBuilderStreamReadResult.from_api_response("users", response)

    assert result.record_count == 2
    assert result.slice_count == 1
    assert result.page_count == 2
    assert result.test_read_limit_reached is True
    assert [page.status for page in result.pages] == [200, 401]
    assert result.pages[0].response_body is None
    assert result.pages[1].response_body == "unauthorized"
    assert result.logs == [{"level": "ERROR", "message": "boom"}]
    assert result.inferred_schema == {"type": "object"}


def test_resolved_manifest_lists_stream_names() -> None:
    resolved = ConnectorBuilderResolvedManifest.from_manifest(MANIFEST)
    assert resolved.stream_names == ["users", "orders"]


@pytest.mark.parametrize(
    ("helper", "kwargs", "expected_path", "expected_json", "response", "expected"),
    [
        pytest.param(
            api_util.create_connector_builder_project,
            {"workspace_id": "ws", "name": "My API", "draft_manifest": MANIFEST},
            "/connector_builder_projects/create",
            {
                "workspaceId": "ws",
                "builderProject": {"name": "My API", "draftManifest": MANIFEST},
            },
            {"builderProjectId": "bp", "workspaceId": "ws"},
            "bp",
            id="create",
        ),
        pytest.param(
            api_util.read_connector_builder_project_stream,
            {
                "workspace_id": "ws",
                "builder_project_id": "bp",
                "manifest": MANIFEST,
                "stream_name": "users",
                "record_limit": 10,
                "page_limit": 2,
                "slice_limit": 3,
            },
            "/connector_builder_projects/read_stream",
            {
                "workspaceId": "ws",
                "builderProjectId": "bp",
                "manifest": MANIFEST,
                "streamName": "users",
                "recordLimit": 10,
                "pageLimit": 2,
                "sliceLimit": 3,
            },
            {"logs": [], "slices": []},
            {"logs": [], "slices": []},
            id="read-stream",
        ),
        pytest.param(
            api_util.full_resolve_connector_builder_project_manifest,
            {
                "workspace_id": "ws",
                "builder_project_id": "bp",
                "manifest": MANIFEST,
                "stream_limit": 7,
            },
            "/connector_builder_projects/full_resolve",
            {
                "workspaceId": "ws",
                "builderProjectId": "bp",
                "manifest": MANIFEST,
                "streamLimit": 7,
            },
            {"manifest": {"streams": []}},
            {"streams": []},
            id="full-resolve",
        ),
        pytest.param(
            api_util.publish_connector_builder_project,
            {
                "workspace_id": "ws",
                "builder_project_id": "bp",
                "name": "My API",
                "manifest": MANIFEST,
                "spec": {"connectionSpecification": {}},
            },
            "/connector_builder_projects/publish",
            {
                "workspaceId": "ws",
                "builderProjectId": "bp",
                "name": "My API",
                "initialDeclarativeManifest": {
                    "manifest": MANIFEST,
                    "spec": {"connectionSpecification": {}},
                    "version": 1,
                    "description": "",
                },
            },
            {"sourceDefinitionId": "def"},
            "def",
            id="publish",
        ),
    ],
)
def test_connector_builder_api_helpers_forward_requests(
    monkeypatch: pytest.MonkeyPatch,
    helper: Any,
    kwargs: dict[str, Any],
    expected_path: str,
    expected_json: dict[str, Any],
    response: dict[str, Any],
    expected: object,
) -> None:
    captured: dict[str, Any] = {}

    def fake_config_request(**request_kwargs: Any) -> dict[str, Any]:
        captured.update(request_kwargs)
        return response

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = helper(
        **kwargs,
        api_root="https://api.example",
        config_api_root="https://config.example",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result == expected
    assert captured["path"] == expected_path
    assert captured["json"] == expected_json


def _fake_workspace() -> Any:
    return SimpleNamespace(
        workspace_id="ws",
        api_root="https://api.airbyte.com/v1",
        config_api_root=None,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
        get_custom_source_definition=MagicMock(),
    )


def _patch_project_store(
    monkeypatch: pytest.MonkeyPatch,
    *,
    draft: dict[str, Any] | None,
    source_definition_id: str | None = None,
    testing_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store: dict[str, Any] = {"draft": draft, "saved": []}

    def fake_get(**_: Any) -> dict[str, Any]:
        return {
            "builderProject": {
                "name": "My API",
                "builderProjectId": "bp",
                "hasDraft": store["draft"] is not None,
                "sourceDefinitionId": source_definition_id,
            },
            "declarativeManifest": {"manifest": store["draft"], "isDraft": True},
            "testingValues": testing_values,
        }

    def fake_update(**kwargs: Any) -> None:
        store["draft"] = kwargs["draft_manifest"]
        store["saved"].append(kwargs)

    monkeypatch.setattr(api_util, "get_connector_builder_project", fake_get)
    monkeypatch.setattr(api_util, "update_connector_builder_project", fake_update)
    monkeypatch.setattr(api_util, "validate_yaml_manifest", lambda *_, **__: None)
    return store


def test_project_url() -> None:
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    assert (
        project.url
        == "https://cloud.airbyte.com/workspaces/ws/connector-builder/edit/bp"
    )


def test_update_draft_manifest_text_saves_and_returns_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _patch_project_store(monkeypatch, draft={"version": "6.0.0", "streams": []})
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")

    diff = project.update_draft_manifest_text(
        edit_mode="replace_text",
        old_text="version: 6.0.0",
        new_text="version: 6.1.0",
    )

    assert store["draft"] == {"version": "6.1.0", "streams": []}
    assert store["saved"][0]["name"] == "My API"
    assert "-version: 6.0.0" in diff
    assert "+version: 6.1.0" in diff


def test_update_draft_manifest_text_without_draft_requires_replace_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _patch_project_store(monkeypatch, draft=None)
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")

    with pytest.raises(AirbyteLibInputError, match="no draft manifest"):
        project.update_draft_manifest_text(
            edit_mode="replace_all", old_text="a", new_text="b"
        )

    project.update_draft_manifest_text(
        edit_mode="replace_full", new_text="version: 6.0.0\n"
    )
    assert store["draft"] == {"version": "6.0.0"}


def test_set_testing_values_uses_draft_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    update_testing_values = MagicMock()
    monkeypatch.setattr(
        api_util,
        "update_connector_builder_project_testing_values",
        update_testing_values,
    )

    CloudConnectorBuilderProject(_fake_workspace(), "bp").set_testing_values({
        "api_key": "k"
    })

    kwargs = update_testing_values.call_args.kwargs
    assert kwargs["testing_values"] == {"api_key": "k"}
    assert kwargs["spec"] == {
        "documentationUrl": "https://example.com/docs",
        "connectionSpecification": {"type": "object", "properties": {}},
        "advancedAuth": None,
    }


def test_publish_unpublished_project(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    publish = MagicMock(return_value="def")
    monkeypatch.setattr(api_util, "publish_connector_builder_project", publish)
    workspace = _fake_workspace()

    CloudConnectorBuilderProject(workspace, "bp").publish("My API")

    spec = publish.call_args.kwargs["spec"]
    assert (
        spec["connectionSpecification"] == MANIFEST["spec"]["connection_specification"]
    )
    workspace.get_custom_source_definition.assert_called_once_with(
        "def", definition_type="yaml"
    )


def test_publish_already_published_project_adds_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST, source_definition_id="def")
    publish = MagicMock()
    monkeypatch.setattr(api_util, "publish_connector_builder_project", publish)
    workspace = _fake_workspace()

    CloudConnectorBuilderProject(workspace, "bp").publish("My API")

    publish.assert_not_called()
    definition = workspace.get_custom_source_definition.return_value
    definition.update_definition.assert_called_once_with(
        manifest_yaml=MANIFEST, pre_validate=True
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({}, id="neither"),
        pytest.param({"builder_project_id": "bp", "definition_id": "d"}, id="both"),
    ],
)
def test_get_connector_builder_project_requires_exactly_one_id(
    kwargs: dict[str, str],
) -> None:
    workspace = CloudWorkspace(
        workspace_id="ws",
        client_id=SecretString("id"),
        client_secret=SecretString("secret"),
    )
    with pytest.raises(AirbyteLibInputError, match="exactly one"):
        workspace.get_connector_builder_project(**kwargs)


def test_project_uses_owner_workspace() -> None:
    project = CloudConnectorBuilderProject(
        _fake_workspace(), "bp", owner_workspace_id="owner"
    )
    assert project.url.endswith("/workspaces/owner/connector-builder/edit/bp")


def test_get_info_reads_project_once(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_project_store(monkeypatch, draft={"version": "6.0.0"})
    calls: list[dict[str, Any]] = []
    original = api_util.get_connector_builder_project

    def counting_get(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(api_util, "get_connector_builder_project", counting_get)

    info = CloudConnectorBuilderProject(_fake_workspace(), "bp").get_info()

    assert len(calls) == 1
    assert info.has_draft is True
    assert info.draft_manifest_text == "version: 6.0.0\n"
    assert info.definition_id is None
    assert info.published_manifest is None


def test_publish_validates_unpublished_draft_unless_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    validate = MagicMock()
    monkeypatch.setattr(api_util, "validate_yaml_manifest", validate)
    monkeypatch.setattr(
        api_util, "publish_connector_builder_project", MagicMock(return_value="d")
    )
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")

    project.publish("My API", pre_validate=False)
    validate.assert_not_called()

    project.publish("My API")
    validate.assert_called_once()


def _stream_read(
    stream_name: str,
    *,
    record_count: int,
    status: int = 200,
    logs: list[dict[str, Any]] | None = None,
    limit_reached: bool = False,
) -> ConnectorBuilderStreamReadResult:
    return ConnectorBuilderStreamReadResult(
        stream_name=stream_name,
        record_count=record_count,
        slice_count=1,
        page_count=1,
        test_read_limit_reached=limit_reached,
        records=[],
        pages=[ConnectorBuilderPageSummary(status=status, record_count=record_count)],
        logs=logs or [],
    )


def _patch_reads(
    monkeypatch: pytest.MonkeyPatch,
    project: CloudConnectorBuilderProject,
    reads: dict[str, ConnectorBuilderStreamReadResult],
) -> None:
    def fake_read(
        _manifest: Any, stream_name: str, **_: Any
    ) -> ConnectorBuilderStreamReadResult:
        if stream_name == "dropped":
            raise requests.ConnectionError("connection reset")
        if stream_name not in reads:
            raise AirbyteLibInputError(
                message="read failed", guidance="Fix the stream."
            )
        return reads[stream_name]

    monkeypatch.setattr(project, "_test_read_stream", fake_read)


def test_run_readiness_report_judges_each_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = ["ok", "empty", "http_error", "error_log", "warn_log", "broken", "dropped"]
    _patch_project_store(
        monkeypatch, draft={**MANIFEST, "streams": [{"name": name} for name in names]}
    )
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    _patch_reads(
        monkeypatch,
        project,
        {
            "ok": _stream_read("ok", record_count=3),
            "empty": _stream_read("empty", record_count=0),
            "http_error": _stream_read("http_error", record_count=1, status=401),
            "error_log": _stream_read(
                "error_log",
                record_count=1,
                logs=[{"level": "ERROR", "message": "boom"}],
            ),
            "warn_log": _stream_read(
                "warn_log", record_count=1, logs=[{"level": "WARN", "message": "hmm"}]
            ),
        },
    )

    report = project.run_readiness_report(names)

    assert [stream.stream_name for stream in report.streams] == names
    assert {stream.stream_name: stream.passed for stream in report.streams} == {
        "ok": True,
        "empty": False,
        "http_error": False,
        "error_log": False,
        "warn_log": True,
        "broken": False,
        "dropped": False,
    }
    assert report.passed is False
    assert (report.stream_count, report.passed_stream_count) == (7, 2)
    by_name = {stream.stream_name: stream for stream in report.streams}
    assert by_name["http_error"].http_errors[0].status == 401
    assert by_name["warn_log"].problem_logs == [{"level": "WARN", "message": "hmm"}]
    assert by_name["broken"].error == "read failed Fix the stream."
    assert (
        by_name["dropped"].error == "Request to Airbyte Cloud failed: ConnectionError"
    )
    assert (
        report.checklist[4].detail
        == "failed: empty, http_error, error_log, broken, dropped"
    )


@pytest.mark.parametrize("count", [0, 26])
def test_run_readiness_report_rejects_stream_counts_out_of_range(count: int) -> None:
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    with pytest.raises(AirbyteLibInputError):
        project.run_readiness_report([f"s{i}" for i in range(count)])


def test_run_readiness_report_without_draft_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=None)
    with pytest.raises(AirbyteLibInputError):
        CloudConnectorBuilderProject(_fake_workspace(), "bp").run_readiness_report([
            "users"
        ])


def test_readiness_report_passes_only_when_every_stream_is_read_to_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    reads = {
        "users": _stream_read("users", record_count=1),
        "orders": _stream_read("orders", record_count=1, limit_reached=True),
    }
    _patch_reads(monkeypatch, project, reads)

    partial = project.run_readiness_report(["users"])
    capped = project.run_readiness_report(["users", "orders"])
    reads["orders"] = _stream_read("orders", record_count=1)
    full = project.run_readiness_report(["users", "orders", "users"])

    assert (partial.passed, partial.untested_streams) == (False, ["orders"])
    assert capped.passed is False
    assert capped.passed_stream_count == 2
    assert [s.read_to_end for s in capped.streams] == [True, False]
    assert capped.checklist[5].done is False
    assert full.passed is True
    assert full.stream_count == 2
    assert full.next_steps == [
        "Publish the custom source definition",
        "Run a full sync of every stream to a no-op destination",
    ]


def test_readiness_checklist_requires_testing_values_when_spec_has_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    draft = {
        "version": "6.0.0",
        "spec": {"connection_specification": {"properties": {"api_key": {}}}},
        "streams": [{"name": "users"}],
    }
    _patch_project_store(monkeypatch, draft=draft, source_definition_id="def")
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    _patch_reads(monkeypatch, project, {"users": _stream_read("users", record_count=1)})

    report = project.run_readiness_report(["users"])

    assert [item.done for item in report.checklist] == [
        True,
        True,
        True,
        False,
        True,
        True,
        True,
        None,
    ]
    assert report.passed is False
    assert report.checklist[2].detail == "users"


def test_publish_spec_uses_api_field_names() -> None:
    manifest = {
        "spec": {
            "connection_specification": {"type": "object"},
            "documentation_url": "https://example.com/docs",
            "advanced_auth": {"auth_flow_type": "oauth2.0"},
        }
    }

    spec = connector_builder._publish_spec(manifest)

    assert spec["connectionSpecification"] == {"type": "object"}
    assert spec["documentationUrl"] == "https://example.com/docs"
    assert spec["advancedAuth"] == {"auth_flow_type": "oauth2.0"}


def test_publish_duplicate_name_leaves_draft_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _patch_project_store(monkeypatch, draft={"version": "6.0.0"})
    workspace = CloudWorkspace(
        workspace_id="ws",
        client_id=SecretString("id"),
        client_secret=SecretString("secret"),
    )
    monkeypatch.setattr(
        CloudWorkspace,
        "list_custom_source_definitions",
        lambda *_, **__: [SimpleNamespace(name="My API")],
    )

    with pytest.raises(AirbyteDuplicateResourcesError):
        workspace.publish_custom_source_definition(
            "My API",
            builder_project_id="bp",
            manifest_yaml=MANIFEST,
            unique=True,
        )

    assert store["saved"] == []
