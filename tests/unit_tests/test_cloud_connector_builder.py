# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Connector Builder projects in Airbyte Cloud."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import threading
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


def test_run_readiness_checks_judges_each_stream(
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

    report = project.run_readiness_checks(names)

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
    assert report.passed_streams == ["ok", "warn_log"]
    assert report.failed_streams == [
        "empty",
        "http_error",
        "error_log",
        "broken",
        "dropped",
    ]
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


def test_run_readiness_checks_requires_stream_names() -> None:
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    with pytest.raises(AirbyteLibInputError):
        project.run_readiness_checks([])


def test_run_readiness_checks_without_draft_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=None)
    with pytest.raises(AirbyteLibInputError):
        CloudConnectorBuilderProject(_fake_workspace(), "bp").run_readiness_checks([
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

    partial = project.run_readiness_checks(["users"])
    capped = project.run_readiness_checks(["users", "orders"])
    reads["orders"] = _stream_read("orders", record_count=1)
    full = project.run_readiness_checks(["users", "orders", "users"])

    assert partial.passed is True
    assert capped.passed is False
    assert capped.passed_streams == ["users", "orders"]
    assert "hit a test-read cap" in capped.next_steps[-1]
    assert [s.read_to_end for s in capped.streams] == [True, False]
    assert capped.checklist[5].done is False
    assert full.passed is True
    assert full.passed_streams == ["users", "orders"]
    assert full.next_steps[0].startswith("All named streams passed.")


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

    report = project.run_readiness_checks(["users"])

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
    assert report.passed is True


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


def test_readiness_timeout_splits_pending_and_not_attempted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    release = threading.Event()
    slow = {f"slow{i}" for i in range(5)}

    def fake_read(
        _manifest: Any, stream_name: str, **_: Any
    ) -> ConnectorBuilderStreamReadResult:
        if stream_name in slow:
            release.wait(5)
        return _stream_read(stream_name, record_count=1)

    monkeypatch.setattr(project, "_test_read_stream", fake_read)
    names = ["fast", *sorted(slow), "queued"]
    try:
        report = project.run_readiness_checks(names, timeout_seconds=0.5)
    finally:
        release.set()
    assert report.passed_streams == ["fast"]
    assert report.pending_streams == sorted(slow)
    assert report.not_attempted_streams == ["queued"]
    assert report.passed is False
    assert any(
        "not-attempted streams" in step and "queued" in step
        for step in report.next_steps
    )
    assert any("still reading at the timeout" in step for step in report.next_steps)


def test_readiness_bounds_read_requests_by_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    timeouts: list[float | None] = []

    def fake_read(
        _manifest: Any, stream_name: str, **kwargs: Any
    ) -> ConnectorBuilderStreamReadResult:
        timeouts.append(kwargs["timeout_seconds"])
        return _stream_read(stream_name, record_count=1)

    monkeypatch.setattr(project, "_test_read_stream", fake_read)
    project.run_readiness_checks(["users"], timeout_seconds=30)
    project.run_readiness_checks(["users"])

    assert timeouts[0] is not None and 0 < timeouts[0] <= 30
    assert timeouts[1] is None


def test_drop_masked_secrets() -> None:
    values = {
        "api_key": "**********",
        "region": "us",
        "auth": {"token": "**********", "kind": "bearer"},
        "items": [{"secret": "**********", "id": 1}],
    }

    assert connector_builder._drop_masked_secrets(values) == {
        "region": "us",
        "auth": {"kind": "bearer"},
        "items": [{"id": 1}],
    }


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        pytest.param({"type": "object", "properties": {}}, False, id="no-inputs"),
        pytest.param(
            {"properties": {"region": {"type": "string"}}}, False, id="no-secrets"
        ),
        pytest.param(
            {
                "properties": {
                    "auth": {
                        "oneOf": [{"properties": {"key": {"airbyte_secret": True}}}]
                    }
                }
            },
            True,
            id="nested-secret",
        ),
    ],
)
def test_spec_has_secrets(spec: dict[str, Any], expected: bool) -> None:
    assert connector_builder._spec_has_secrets(spec) is expected


def _secret_manifest() -> dict[str, Any]:
    return {
        **MANIFEST,
        "spec": {
            "connection_specification": {
                "type": "object",
                "properties": {
                    "api_key": {"type": "string", "airbyte_secret": True},
                    "region": {"type": "string"},
                },
            },
        },
    }


@pytest.mark.parametrize(
    ("manifest", "deferred"),
    [
        pytest.param(_secret_manifest(), True, id="secret-spec-deferred"),
        pytest.param(MANIFEST, False, id="no-secret-spec-ready"),
        pytest.param(
            {
                **MANIFEST,
                "spec": {
                    "type": "Spec",
                    "connection_specification": {
                        "type": "object",
                        "required": ["base_url"],
                        "properties": {"base_url": {"type": "string"}},
                    },
                },
            },
            True,
            id="missing-required-deferred",
        ),
    ],
)
def test_create_test_source_uses_non_secret_testing_values(
    monkeypatch: pytest.MonkeyPatch,
    manifest: dict[str, Any],
    deferred: bool,
) -> None:
    _patch_project_store(
        monkeypatch,
        draft=manifest,
        source_definition_id="def",
        testing_values={"api_key": "**********", "region": "us"},
    )
    workspace = _fake_workspace()
    workspace.deploy_source = MagicMock()
    project = CloudConnectorBuilderProject(workspace, "bp")

    project.create_test_source()

    workspace.deploy_source.assert_called_once_with(
        name="My API (Connector Builder test)",
        source={"region": "us"},
        definition_id="def",
        defer_credentials=deferred,
    )


def test_create_test_source_requires_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")

    with pytest.raises(AirbyteLibInputError, match="Publish"):
        project.create_test_source()


def _full_sync_project(
    monkeypatch: pytest.MonkeyPatch,
    *,
    destinations: list[Any],
    connections: list[Any],
    sources: list[Any] | None = None,
    draft: dict[str, Any] | None = MANIFEST,
    resolved_streams: list[dict[str, Any]] | None = None,
) -> tuple[CloudConnectorBuilderProject, Any]:
    _patch_project_store(monkeypatch, draft=draft, source_definition_id="def")
    workspace = _fake_workspace()
    workspace.get_source = MagicMock(return_value=SimpleNamespace(definition_id="def"))
    workspace.list_sources = MagicMock(return_value=sources or [])
    workspace.list_destinations = MagicMock(return_value=destinations)
    workspace.list_connections = MagicMock(return_value=connections)
    workspace.deploy_destination = MagicMock(
        return_value=SimpleNamespace(connector_id="new-dest")
    )
    new_connection = MagicMock(connection_id="conn")
    new_connection.run_sync.return_value = SimpleNamespace(
        job_id=7, job_url="https://job"
    )
    workspace.deploy_connection = MagicMock(return_value=new_connection)
    project = CloudConnectorBuilderProject(workspace, "bp")
    monkeypatch.setattr(project, "publish", MagicMock())
    monkeypatch.setattr(
        project,
        "resolve_manifest",
        MagicMock(
            return_value=SimpleNamespace(
                stream_names=["users", "orders"],
                manifest={"streams": resolved_streams or []},
            )
        ),
    )
    monkeypatch.setattr(project, "_published_manifest", lambda _id: MANIFEST)
    return project, workspace


def test_run_full_sync_creates_noop_destination_and_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, workspace = _full_sync_project(
        monkeypatch,
        destinations=[],
        connections=[],
        sources=[
            SimpleNamespace(connector_id="other", definition_id="other-def"),
            SimpleNamespace(connector_id="src", definition_id="def"),
        ],
    )

    result = project.run_full_sync()

    project.publish.assert_called_once_with("My API", pre_validate=True)
    workspace.list_sources.assert_called_once_with(
        name="My API (Connector Builder test)"
    )
    assert workspace.deploy_destination.call_args.kwargs["name"] == (
        connector_builder.CONNECTOR_BUILDER_TEST_DESTINATION_NAME
    )
    workspace.deploy_connection.assert_called_once_with(
        "My API (Connector Builder full sync)",
        source="src",
        destination="new-dest",
        selected_streams=["users", "orders"],
    )
    assert result.model_dump() == {
        "source_id": "src",
        "destination_id": "new-dest",
        "connection_id": "conn",
        "destination_created": True,
        "connection_created": True,
        "job_id": 7,
        "job_url": "https://job",
    }


def test_run_full_sync_reuses_destination_and_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = MagicMock(connection_id="old", source_id="src", destination_id="dest")
    existing.run_sync.return_value = SimpleNamespace(job_id=8, job_url="https://job8")
    project, workspace = _full_sync_project(
        monkeypatch,
        destinations=[SimpleNamespace(connector_id="dest")],
        connections=[existing],
    )

    result = project.run_full_sync(test_source_id="src")

    workspace.deploy_destination.assert_not_called()
    workspace.deploy_connection.assert_not_called()
    existing.set_selected_streams.assert_called_once_with(["users", "orders"])
    existing.run_sync.assert_called_once_with(wait=False)
    workspace.get_source.assert_called_once_with("src")
    assert (result.connection_id, result.connection_created, result.job_id) == (
        "old",
        False,
        8,
    )


def test_run_full_sync_without_test_source_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, _ = _full_sync_project(monkeypatch, destinations=[], connections=[])

    with pytest.raises(AirbyteLibInputError, match="No test source"):
        project.run_full_sync()


@pytest.mark.parametrize(
    ("stream_names", "record_count", "expect_sync"),
    [
        pytest.param([], 0, True, id="no-streams"),
        pytest.param(["users"], 1, True, id="all-passed"),
        pytest.param(["users"], 0, False, id="failed-skips-sync"),
    ],
)
def test_readiness_full_sync_gating(
    monkeypatch: pytest.MonkeyPatch,
    stream_names: list[str],
    record_count: int,
    expect_sync: bool,
) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")
    full_sync = connector_builder.ConnectorBuilderFullSync(
        source_id="src",
        destination_id="dest",
        connection_id="conn",
        destination_created=False,
        connection_created=True,
        job_id=1,
        job_url="u",
    )
    run_full_sync = MagicMock(return_value=full_sync)
    monkeypatch.setattr(project, "run_full_sync", run_full_sync)
    monkeypatch.setattr(
        project,
        "_test_read_stream",
        lambda _m, name, **_: _stream_read(name, record_count=record_count),
    )

    report = project.run_readiness_checks(
        stream_names, run_full_sync=True, test_source_id="src"
    )

    assert (report.full_sync is not None) is expect_sync
    if expect_sync:
        run_full_sync.assert_called_once_with(
            test_source_id="src", test_destination_id=None
        )


def test_readiness_requires_streams_without_full_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(monkeypatch, draft=MANIFEST)
    project = CloudConnectorBuilderProject(_fake_workspace(), "bp")

    with pytest.raises(AirbyteLibInputError, match="run_full_sync"):
        project.run_readiness_checks([])


def test_run_full_sync_resolves_draft_before_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, _ = _full_sync_project(
        monkeypatch,
        destinations=[SimpleNamespace(connector_id="dest")],
        connections=[],
    )
    calls: list[str] = []
    project.resolve_manifest.side_effect = lambda **kwargs: (
        calls.append("resolve"),
        SimpleNamespace(stream_names=["users"], manifest={"streams": []}),
    )[1]
    project.publish.side_effect = lambda *_, **__: calls.append("publish")

    project.run_full_sync(test_source_id="src")

    assert calls == ["resolve", "publish"]
    assert project.resolve_manifest.call_args.kwargs["manifest"] == MANIFEST


def test_run_full_sync_without_draft_syncs_published_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, workspace = _full_sync_project(
        monkeypatch,
        destinations=[SimpleNamespace(connector_id="dest")],
        connections=[],
        draft=None,
    )

    project.run_full_sync(test_source_id="src")

    project.publish.assert_not_called()
    assert project.resolve_manifest.call_args.kwargs["manifest"] == MANIFEST
    workspace.deploy_connection.assert_called_once()


def test_run_full_sync_rejects_source_of_other_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, workspace = _full_sync_project(
        monkeypatch, destinations=[], connections=[]
    )
    workspace.get_source.return_value = SimpleNamespace(definition_id="other-def")

    with pytest.raises(AirbyteLibInputError, match="published definition"):
        project.run_full_sync(test_source_id="src")

    workspace.deploy_connection.assert_not_called()


def test_run_full_sync_rejects_capped_dynamic_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limit = connector_builder._FULL_SYNC_STREAM_LIMIT
    project, workspace = _full_sync_project(
        monkeypatch,
        destinations=[],
        connections=[],
        resolved_streams=[
            {"name": "users", "dynamic_stream_name": None},
            *({"name": f"t{i}", "dynamic_stream_name": "tables"} for i in range(limit)),
        ],
    )

    with pytest.raises(AirbyteLibInputError, match="Dynamic streams") as excinfo:
        project.run_full_sync(test_source_id="src")

    assert excinfo.value.context == {"dynamic_streams": ["tables"]}
    project.publish.assert_not_called()
    workspace.deploy_connection.assert_not_called()


def test_create_test_source_after_publish_cleared_draft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_project_store(
        monkeypatch,
        draft=None,
        source_definition_id="def",
        testing_values={"api_key": "**********", "region": "us"},
    )
    workspace = _fake_workspace()
    workspace.deploy_source = MagicMock()
    project = CloudConnectorBuilderProject(workspace, "bp")
    monkeypatch.setattr(project, "_published_manifest", lambda _id: _secret_manifest())

    project.create_test_source()

    assert workspace.deploy_source.call_args.kwargs["defer_credentials"] is True


def test_run_readiness_checks_full_sync_only_without_draft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, workspace = _full_sync_project(
        monkeypatch,
        destinations=[SimpleNamespace(connector_id="dest")],
        connections=[],
        draft=None,
    )

    report = project.run_readiness_checks([], run_full_sync=True, test_source_id="src")

    assert report.full_sync is not None
    assert report.full_sync.destination_created is False
    project.publish.assert_not_called()
    workspace.deploy_connection.assert_called_once()
    draft_step = next(
        item for item in report.checklist if item.step == "Save a draft manifest"
    )
    assert draft_step.done is False
