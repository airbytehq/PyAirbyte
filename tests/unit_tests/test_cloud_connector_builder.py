# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Connector Builder projects in Airbyte Cloud."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from airbyte._util import api_util
from airbyte.cloud import connector_builder
from airbyte.cloud.connector_builder import (
    CloudConnectorBuilderProject,
    ConnectorBuilderResolvedManifest,
    ConnectorBuilderStreamReadResult,
    apply_manifest_text_edit,
    render_manifest_text,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteLibInputError
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
