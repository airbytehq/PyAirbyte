# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for the local MCP wrappers."""

from __future__ import annotations

import pytest

from airbyte.mcp.local import _resolve_docker_image_and_name


@pytest.mark.parametrize(
    ("connector_name", "docker_image", "expected_name", "expected_image"),
    [
        pytest.param(
            "source-mssql",
            None,
            "source-mssql",
            None,
            id="registry_name_passthrough",
        ),
        pytest.param(
            "source-mssql",
            "airbyte/source-mssql:dev",
            "source-mssql",
            "airbyte/source-mssql:dev",
            id="registry_name_with_explicit_image_override",
        ),
        pytest.param(
            "airbyte/source-mssql",
            None,
            "source-mssql",
            "airbyte/source-mssql",
            id="image_id_no_tag",
        ),
        pytest.param(
            "airbyte/source-mssql:4.4.2",
            None,
            "source-mssql",
            "airbyte/source-mssql:4.4.2",
            id="image_id_with_pinned_tag",
        ),
        pytest.param(
            "airbyte/source-mssql:dev",
            None,
            "source-mssql",
            "airbyte/source-mssql:dev",
            id="image_id_with_dev_tag",
        ),
        pytest.param(
            "ghcr.io/airbytehq/source-mssql:4.4.2",
            None,
            "source-mssql",
            "ghcr.io/airbytehq/source-mssql:4.4.2",
            id="multi_segment_image_path",
        ),
        pytest.param(
            "airbyte/source-mssql:4.4.2",
            "airbyte/source-mssql:dev",
            "source-mssql",
            "airbyte/source-mssql:dev",
            id="image_id_plus_explicit_image_override_wins",
        ),
        pytest.param(
            "ghcr.io/airbytehq/source-mssql@sha256:abc123",
            None,
            "source-mssql",
            "ghcr.io/airbytehq/source-mssql@sha256:abc123",
            id="image_id_with_digest",
        ),
        pytest.param(
            "localhost:5000/airbyte/source-mssql",
            None,
            "source-mssql",
            "localhost:5000/airbyte/source-mssql",
            id="image_id_with_registry_port_no_tag",
        ),
        pytest.param(
            "localhost:5000/airbyte/source-mssql:4.4.2",
            None,
            "source-mssql",
            "localhost:5000/airbyte/source-mssql:4.4.2",
            id="image_id_with_registry_port_and_tag",
        ),
    ],
)
def test_resolve_docker_image_and_name(
    connector_name: str,
    docker_image: str | None,
    expected_name: str,
    expected_image: str | None,
) -> None:
    name, image = _resolve_docker_image_and_name(
        connector_name=connector_name,
        docker_image=docker_image,
    )
    assert name == expected_name
    assert image == expected_image
