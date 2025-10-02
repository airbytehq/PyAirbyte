# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for custom connector definition publishing."""

import pytest

from airbyte.cloud.workspaces import CloudWorkspace


TEST_YAML_MANIFEST = {
    "version": "0.1.0",
    "type": "DeclarativeSource",
    "check": {"type": "CheckStream", "stream_names": ["test"]},
    "streams": [
        {
            "type": "DeclarativeStream",
            "name": "test",
            "primary_key": [],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://httpbin.org",
                    "path": "/get",
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []},
                },
            },
        }
    ],
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
    assert result.connector_type == "yaml"

    definition_id = result.definition_id

    try:
        definitions = cloud_workspace.list_custom_source_definitions(
            name=name,
            custom_connector_type="yaml",
        )
        assert len(definitions) == 1
        assert definitions[0].definition_id == definition_id

        fetched = cloud_workspace.get_custom_source_definition(
            definition_id,
            custom_connector_type="yaml",
        )
        assert fetched.definition_id == definition_id
        assert fetched.name == name
        assert fetched.connector_type == "yaml"

        updated_manifest = TEST_YAML_MANIFEST.copy()
        updated_manifest["version"] = "0.2.0"
        updated = cloud_workspace.update_custom_source_definition(
            definition_id=definition_id,
            manifest_yaml=updated_manifest,
        )
        assert updated.manifest["version"] == "0.2.0"

    finally:
        cloud_workspace.permanently_delete_custom_source_definition(
            definition_id,
            custom_connector_type="yaml",
        )


@pytest.mark.requires_creds
def test_publish_custom_docker_source(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test publishing a custom Docker source definition."""
    from airbyte._util import text_util

    name = f"test-docker-source-{text_util.generate_random_suffix()}"

    result = cloud_workspace.publish_custom_source_definition(
        name=name,
        docker_image="airbyte/test-source",
        docker_tag="1.0.0",
        documentation_url="https://example.com/docs",
        unique=True,
    )

    assert result.definition_id
    assert result.name == name
    assert result.docker_repository == "airbyte/test-source"
    assert result.docker_image_tag == "1.0.0"
    assert result.connector_type == "docker"

    definition_id = result.definition_id

    try:
        definitions = cloud_workspace.list_custom_source_definitions(
            name=name,
            custom_connector_type="docker",
        )
        assert len(definitions) == 1
        assert definitions[0].definition_id == definition_id

        fetched = cloud_workspace.get_custom_source_definition(
            definition_id,
            custom_connector_type="docker",
        )
        assert fetched.definition_id == definition_id
        assert fetched.name == name
        assert fetched.connector_type == "docker"

        updated = cloud_workspace.update_custom_source_definition(
            definition_id=definition_id,
            docker_tag="2.0.0",
        )
        assert updated.docker_image_tag == "2.0.0"

    finally:
        cloud_workspace.permanently_delete_custom_source_definition(
            definition_id,
            custom_connector_type="docker",
        )


@pytest.mark.requires_creds
def test_publish_custom_docker_destination(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test publishing a custom Docker destination definition."""
    from airbyte._util import text_util

    name = f"test-docker-dest-{text_util.generate_random_suffix()}"

    result = cloud_workspace.publish_custom_destination_definition(
        name=name,
        docker_image="airbyte/test-destination",
        docker_tag="1.0.0",
        unique=True,
    )

    assert result.definition_id
    assert result.name == name
    assert result.docker_repository == "airbyte/test-destination"
    assert result.docker_image_tag == "1.0.0"

    definition_id = result.definition_id

    try:
        definitions = cloud_workspace.list_custom_destination_definitions(name=name)
        assert len(definitions) == 1
        assert definitions[0].definition_id == definition_id

        fetched = cloud_workspace.get_custom_destination_definition(definition_id)
        assert fetched.definition_id == definition_id
        assert fetched.name == name

        updated = cloud_workspace.update_custom_destination_definition(
            definition_id=definition_id,
            name=name,
            docker_tag="2.0.0",
        )
        assert updated.docker_image_tag == "2.0.0"

    finally:
        cloud_workspace.permanently_delete_custom_destination_definition(definition_id)


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
