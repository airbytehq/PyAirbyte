# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Catalog sync modes must respect the stream's advertised capabilities."""

from unittest.mock import Mock, patch

import pytest
from airbyte.sources.base import Source
from airbyte_protocol.models import AirbyteCatalog, AirbyteStream, SyncMode


@pytest.mark.parametrize(
    "supported_modes,force_full_refresh,expected_mode",
    [
        pytest.param(
            [SyncMode.full_refresh], False, SyncMode.full_refresh, id="full-only"
        ),
        pytest.param(
            [SyncMode.incremental], False, SyncMode.incremental, id="incremental-only"
        ),
        pytest.param(
            [SyncMode.full_refresh, SyncMode.incremental],
            False,
            SyncMode.incremental,
            id="prefer-incremental",
        ),
        pytest.param(
            [SyncMode.full_refresh], True, SyncMode.full_refresh, id="force-full-only"
        ),
        pytest.param(
            [SyncMode.incremental],
            True,
            SyncMode.incremental,
            id="force-incremental-only",
        ),
        pytest.param(
            [SyncMode.full_refresh, SyncMode.incremental],
            True,
            SyncMode.full_refresh,
            id="force-prefer-full",
        ),
    ],
)
def test_configured_catalog_respects_supported_sync_modes(
    supported_modes: list[SyncMode],
    force_full_refresh: bool,
    expected_mode: SyncMode,
) -> None:
    """A full-refresh-only stream must never be configured as incremental."""
    discovered_catalog = AirbyteCatalog(
        streams=[
            AirbyteStream(
                name="records",
                json_schema={"type": "object", "properties": {}},
                supported_sync_modes=supported_modes,
            )
        ]
    )
    source = Source(executor=Mock(), name="source-test")

    with patch.object(Source, "_discover", return_value=discovered_catalog):
        configured_catalog = source.get_configured_catalog(
            force_full_refresh=force_full_refresh
        )

    assert configured_catalog.streams[0].sync_mode == expected_mode
