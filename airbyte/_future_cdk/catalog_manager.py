# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A SQL Cache implementation."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, final

from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        ConfiguredAirbyteCatalog,
        ConfiguredAirbyteStream,
    )


STREAMS_TABLE_NAME = "_airbyte_streams"
STATE_TABLE_NAME = "_airbyte_state"

GLOBAL_STATE_STREAM_NAMES = ["_GLOBAL", "_LEGACY"]

Base = declarative_base()


class CachedStream(Base):  # type: ignore[valid-type,misc]
    __tablename__ = STREAMS_TABLE_NAME

    stream_name = Column(String)
    source_name = Column(String)
    table_name = Column(String, primary_key=True)
    catalog_metadata = Column(String)


class CatalogManagerBase(abc.ABC):
    """
    A class to manage the stream catalog of data synced to a cache:
    * What streams exist and to what tables they map
    * The JSON schema for each stream
    * The state of each stream if available
    """

    @property
    @abc.abstractmethod
    def stream_names(self) -> list[str]:
        """Return the names of all streams in the cache."""
        ...

    @property
    @abc.abstractmethod
    def source_catalog(self) -> ConfiguredAirbyteCatalog:
        """Return the source catalog with all known streams.

        Raises:
            PyAirbyteInternalError: If the source catalog is not set.
        """
        ...

    def register_source(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        incoming_stream_names: set[str],
    ) -> None:
        """Register a source and its streams in the cache."""
        self._update_catalog(
            incoming_source_catalog=incoming_source_catalog,
            incoming_stream_names=incoming_stream_names,
        )
        self._save_catalog_info(
            source_name=source_name,
            incoming_source_catalog=incoming_source_catalog,
            incoming_stream_names=incoming_stream_names,
        )

    @abc.abstractmethod
    def _update_catalog(
        self,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        incoming_stream_names: set[str],
    ) -> None:
        """Update the catalog with the incoming stream names."""
        ...

    @abc.abstractmethod
    def _save_catalog_info(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        incoming_stream_names: set[str],
    ) -> None:
        """Save the catalog information to the cache."""
        ...

    @abc.abstractmethod
    def get_configured_catalog_info(
        self,
        stream_name: str,
    ) -> ConfiguredAirbyteStream:
        """Return the column definitions for the given stream."""
        ...

    @final
    def get_stream_json_schema(
        self,
        stream_name: str,
    ) -> dict[str, Any]:
        """Return the column definitions for the given stream."""
        return self.get_configured_catalog_info(stream_name).stream.json_schema

    @abc.abstractmethod
    def _load_catalog_info(self) -> None:
        """Load the catalog information from the cache."""
        ...
