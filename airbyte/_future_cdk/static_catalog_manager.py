# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Catalog manager implementation that uses a static catalog input."""

from __future__ import annotations

from typing import TYPE_CHECKING

from overrides import overrides

from airbyte._future_cdk.catalog_manager import CatalogManagerBase


if TYPE_CHECKING:
    from airbyte_protocol.models import ConfiguredAirbyteCatalog


class StaticCatalogManager(CatalogManagerBase):
    """A catalog manager that uses a static catalog input."""

    def __init__(self, catalog: ConfiguredAirbyteCatalog) -> None:
        self._catalog = catalog

    @property
    @overrides
    def stream_names(self) -> list[str]:
        return [stream.name for stream in self.configured_catalog.streams]

    @property
    @overrides
    def configured_catalog(self) -> ConfiguredAirbyteCatalog:
        return self._catalog
