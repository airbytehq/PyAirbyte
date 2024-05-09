# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Catalog manager implementation."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, final

from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

from airbyte import exceptions as exc


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
    """

    # Abstract implementations

    @property
    @abc.abstractmethod
    def configured_catalog(self) -> ConfiguredAirbyteCatalog:
        """Return the source catalog with all known streams.

        Raises:
            PyAirbyteInternalError: If the source catalog is not set.
        """
        ...

    @abc.abstractmethod
    def _load_catalog_info(self) -> None:
        """Load the catalog information from the cache."""
        ...

    @abc.abstractmethod
    def _update_catalog(
        self,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        incoming_stream_names: set[str],
    ) -> None:
        """Update the catalog data with incoming catalog."""
        ...

    @abc.abstractmethod
    def _save_catalog_info(
        self,
        source_name: str,
        incoming_source_catalog: ConfiguredAirbyteCatalog,
        incoming_stream_names: set[str],
    ) -> None:
        """Serialize the incoming catalog information to storage.

        Raises:
            NotImplementedError: If the catalog is static or the catalog manager is read only.
        """
        ...

    # Generic implementations

    @property
    def stream_names(self) -> list[str]:
        return [stream.name for stream in self.configured_catalog.streams]

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

    def get_configured_stream_info(
        self,
        stream_name: str,
    ) -> ConfiguredAirbyteStream:
        """Return the column definitions for the given stream."""
        if not self.configured_catalog:
            raise exc.PyAirbyteInternalError(
                message="Cannot get stream JSON schema without a catalog.",
            )

        matching_streams: list[ConfiguredAirbyteStream] = [
            stream
            for stream in self.configured_catalog.streams
            if stream.stream.name == stream_name
        ]
        if not matching_streams:
            raise exc.AirbyteStreamNotFoundError(
                stream_name=stream_name,
                context={
                    "available_streams": [
                        stream.stream.name for stream in self.configured_catalog.streams
                    ],
                },
            )

        if len(matching_streams) > 1:
            raise exc.PyAirbyteInternalError(
                message="Multiple streams found with same name.",
                context={
                    "stream_name": stream_name,
                },
            )

        return matching_streams[0]

    @final
    def get_stream_json_schema(
        self,
        stream_name: str,
    ) -> dict[str, Any]:
        """Return the column definitions for the given stream."""
        return self.get_configured_stream_info(stream_name).stream.json_schema
