# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud connectors module for working with Cloud sources and destinations."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:
    from airbyte.cloud.workspaces import CloudWorkspace


class CloudConnector(abc.ABC):
    """A cloud connector is a deployed source or destination on Airbyte Cloud.

    You can use a connector object to manage the connector.
    """

    @property
    @abc.abstractmethod
    def connector_type(self) -> Literal["source", "destination"]:
        """Get the type of the connector."""
        ...

    def __init__(
        self,
        workspace: CloudWorkspace,
        connector_id: str,
    ) -> None:
        """Initialize a cloud connector object."""
        self.workspace = workspace
        """The workspace that the connector belongs to."""
        self.connector_id = connector_id
        """The ID of the connector."""

    @property
    def connector_url(self) -> str:
        """Get the URL of the source connector."""
        return f"{self.workspace.workspace_url}/{self.connector_type}s/{self.connector_id}"

    def permanently_delete(self) -> None:
        """Permanently delete the connector."""
        if self.connector_type == "source":
            self.workspace.permanently_delete_source(self.connector_id)
        else:
            self.workspace.permanently_delete_destination(self.connector_id)


class CloudSource(CloudConnector):
    """A cloud source is a source that is deployed on Airbyte Cloud."""

    @property
    def source_id(self) -> str:
        """Get the ID of the source.

        This is an alias for `connector_id`.
        """
        return self.connector_id

    @property
    def connector_type(self) -> Literal["source", "destination"]:
        """Get the type of the connector."""
        return "source"


class CloudDestination(CloudConnector):
    """A cloud destination is a destination that is deployed on Airbyte Cloud."""

    @property
    def destination_id(self) -> str:
        """Get the ID of the destination.

        This is an alias for `connector_id`.
        """
        return self.connector_id

    @property
    def connector_type(self) -> Literal["source", "destination"]:
        """Get the type of the connector."""
        return "destination"
