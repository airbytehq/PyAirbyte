# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for custom connector definition publishing."""

import pytest
from airbyte.cloud.workspaces import CloudWorkspace

TEST_YAML_MANIFEST = {
    "version": "0.1.0",
    "type": "DeclarativeSource",
    "check": {
        "type": "CheckStream",
        "stream_names": ["test_stream"],
    },
    "definitions": {
        "base_requester": {
            "type": "HttpRequester",
            "url_base": "https://httpbin.org",
        },
    },
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "test_stream",
            "primary_key": ["id"],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {"$ref": "#/definitions/base_requester", "path": "/get"},
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
            },
        }
    ],
    "spec": {
        "type": "Spec",
        "connection_specification": {
            "type": "object",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "properties": {},
        },
    },
}


@pytest.mark.requires_creds
def test_publish_custom_yaml_source(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test publishing a custom YAML source definition."""
    from airbyte._util import text_util

    name = f"test-yaml-source-{text_util.generate_random_suffix()}"

    result = cloud_workspace.publish_custom_source_definition(
        name=name,
        manifest_yaml=TEST_YAML_MANIFEST,
        unique=True,
        pre_validate=True,
    )

    assert result.definition_id
    assert result.name == name
    assert result.manifest is not None
    assert result.version is not None
    assert result.definition_type == "yaml"
    assert result.connector_type == "source"

    definition_id = result.definition_id

    try:
        all_definitions = cloud_workspace.list_custom_source_definitions(
            definition_type="yaml",
        )
        definitions = [d for d in all_definitions if d.name == name]
        assert len(definitions) == 1
        assert definitions[0].definition_id == definition_id

        fetched = cloud_workspace.get_custom_source_definition(
            definition_id,
            definition_type="yaml",
        )
        assert fetched.definition_id == definition_id
        assert fetched.name == name
        assert fetched.definition_type == "yaml"
        assert fetched.connector_type == "source"

        updated_manifest = TEST_YAML_MANIFEST.copy()
        updated_manifest["version"] = "0.2.0"
        updated = fetched.update_definition(
            manifest_yaml=updated_manifest,
        )
        assert updated.manifest["version"] == "0.2.0"

    finally:
        cloud_workspace.permanently_delete_custom_source_definition(
            definition_id,
            definition_type="yaml",
        )


@pytest.mark.requires_creds
def test_yaml_validation_error(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test that validation catches invalid manifests."""
    from airbyte._util import text_util
    from airbyte.exceptions import PyAirbyteInputError

    name = f"test-invalid-{text_util.generate_random_suffix()}"
    invalid_manifest = {"version": "0.1.0"}

    with pytest.raises(PyAirbyteInputError) as exc_info:
        cloud_workspace.publish_custom_source_definition(
            name=name,
            manifest_yaml=invalid_manifest,
            pre_validate=True,
        )

    assert "type" in str(exc_info.value).lower()


@pytest.mark.requires_creds
def test_safety_mode_prevents_deletion(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test that safety_mode prevents deletion of connectors without proper naming."""
    from airbyte._util import text_util
    from airbyte.exceptions import PyAirbyteInputError

    name = f"test-yaml-source-{text_util.generate_random_suffix()}"

    result = cloud_workspace.publish_custom_source_definition(
        name=name,
        manifest_yaml=TEST_YAML_MANIFEST,
        unique=True,
        pre_validate=True,
    )

    definition_id = result.definition_id

    try:
        with pytest.raises(PyAirbyteInputError) as exc_info:
            cloud_workspace.permanently_delete_custom_source_definition(
                definition_id,
                definition_type="yaml",
                safety_mode=True,
            )

        error_message = str(exc_info.value).lower()
        assert "safety_mode" in error_message
        assert "delete:" in error_message or "delete-me" in error_message

    finally:
        cloud_workspace.permanently_delete_custom_source_definition(
            definition_id,
            definition_type="yaml",
            safety_mode=False,
        )


@pytest.mark.requires_creds
def test_safety_mode_allows_deletion_with_delete_prefix(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test that safety_mode allows deletion when name starts with 'delete:'."""
    from airbyte._util import text_util

    name = f"delete:test-yaml-source-{text_util.generate_random_suffix()}"

    result = cloud_workspace.publish_custom_source_definition(
        name=name,
        manifest_yaml=TEST_YAML_MANIFEST,
        unique=True,
        pre_validate=True,
    )

    definition_id = result.definition_id

    cloud_workspace.permanently_delete_custom_source_definition(
        definition_id,
        definition_type="yaml",
        safety_mode=True,
    )


@pytest.mark.requires_creds
def test_safety_mode_allows_deletion_with_delete_me(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test that safety_mode allows deletion when name contains 'delete-me'."""
    from airbyte._util import text_util

    name = f"test-delete-me-yaml-source-{text_util.generate_random_suffix()}"

    result = cloud_workspace.publish_custom_source_definition(
        name=name,
        manifest_yaml=TEST_YAML_MANIFEST,
        unique=True,
        pre_validate=True,
    )

    definition_id = result.definition_id

    cloud_workspace.permanently_delete_custom_source_definition(
        definition_id,
        definition_type="yaml",
        safety_mode=True,
    )
