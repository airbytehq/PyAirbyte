# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud Connectors."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from airbyte import exceptions as exc
from airbyte.cloud import _api_util
from airbyte.cloud._resources import ICloudResource, requires_fetch
from airbyte.cloud.constants import ConnectorTypeEnum
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte._util.api_imports import DestinationResponse, SourceResponse
    from airbyte.cloud.workspaces import CloudWorkspace


@dataclass
class CloudConnector(ICloudResource):
    """A connector is a source or destination in Airbyte Cloud.

    You can use a connector object to retrieve information about the connector and manage the
    connector.
    """

    workspace: CloudWorkspace
    """The workspace that the connector belongs to."""

    connector_id: str
    """The ID of the connector."""

    connector_type: ConnectorTypeEnum
    """The type of the connector."""

    _resource_info: SourceResponse | DestinationResponse | None = field(default=None, init=False)
    """The connector info for the connector. Internal use only."""

    @property
    @requires_fetch
    def name(self) -> str:
        """Return the name of the connector."""
        assert self._resource_info, "Decorator should have fetched the resource info."
        return self._resource_info.name

    def _fetch_resource_info(self) -> SourceResponse | DestinationResponse:
        """Populate the connector with data from the API."""
        if self._resource_info:
            return self._resource_info

        if self.is_source:
            self._resource_info = _api_util.fetch_source_info(
                source_id=self.source_id,
                api_root=self.workspace.api_root,
                api_key=self.workspace.api_key,
            )
            return self._resource_info

        if self.is_destination:
            self._resource_info = _api_util.fetch_destination_info(
                destination_id=self.destination_id,
                api_root=self.workspace.api_root,
                api_key=self.workspace.api_key,
            )
            return self._resource_info

        raise exc.PyAirbyteInternalError(
            message=f"Connector {self.name} is not a source or destination."
        )

    @property
    def is_source(self) -> bool:
        """Return true if the connector is a source."""
        return self.connector_type is ConnectorTypeEnum.SOURCE

    @property
    def is_destination(self) -> bool:
        """Return true if the connector is a destination."""
        return self.connector_type is ConnectorTypeEnum.DESTINATION

    def _assert_is_source(self) -> None:
        """Raise an error if the connector is not a source."""
        if not self.is_source:
            raise PyAirbyteInputError(message=f"Connector {self.name} is not a source.")

    def _assert_is_destination(self) -> None:
        """Raise an error if the connector is not a destination."""
        if not self.is_destination:
            raise PyAirbyteInputError(message=f"Connector {self.name} is not a destination.")

    @property
    def source_id(self) -> str:
        """Return the ID of the source if this is a source. Otherwise, raise an error.

        Raises:
            PyAirbyteInputError: If the connector is not a source.
        """
        self._assert_is_source()

        return self.connector_id

    @property
    def destination_id(self) -> str:
        """Return the ID of the destination if this is a destination. Otherwise, raise an error.

        Raises:
            PyAirbyteInputError: If the connector is not a destination.
        """
        self._assert_is_destination()

        return self.connector_id

    @classmethod
    def from_connector_id(
        cls,
        workspace: CloudWorkspace,
        connector_id: str,
        connector_type: ConnectorTypeEnum,
    ) -> CloudConnector:
        """Create a connector object from a connection ID."""
        return cls(
            workspace=workspace,
            connector_id=connector_id,
            connector_type=connector_type,
        )

    @property
    def configuration(self) -> dict:
        """Return the configuration of the connector."""
        return self._fetch_resource_info().configuration
