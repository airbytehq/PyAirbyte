# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud connectors module for working with Cloud sources and destinations.

This module provides classes for working with Cloud sources and destinations. Rather
than creating `CloudConnector` objects directly, it is recommended to use the
`airbyte.cloud.workspaces` module to create and manage cloud connector objects.

Classes:
  - `CloudConnector`: A cloud connector object.
  - `CloudSource`: A cloud source object.
  - `CloudDestination`: A cloud destination object.

## Usage Examples

Obtain a cloud source object and run a `check` on it:

```python
from airbyte.cloud import CloudWorkspace

workspace = CloudWorkspace(
    workspace_id="...",
    client_id="...",
    client_secret="...",
)

# Get the cloud source object
cloud_source = workspace.get_source("...")

# Check the source configuration and credentials
check_result = cloud_source.check()
if check_result:
    # Truthy if the check was successful
    print("Check successful")
else:
    # Stringify the check result to get the error message
    print(f"Check failed: {check_result}")
```
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Literal

from airbyte._util import api_util


if TYPE_CHECKING:
    from airbyte.cloud.workspaces import CloudWorkspace


@dataclass
class CheckResult:
    """A cloud check result object."""

    success: bool
    """Whether the check result is valid."""

    error_message: str | None = None
    """None if the check was successful. Otherwise the failure message from the check result."""

    internal_error: str | None = None
    """None if the check was able to be run. Otherwise, this will describe the internal failure."""

    def __bool__(self) -> bool:
        """Truthy when check was successful."""
        return self.success

    def __str__(self) -> str:
        """Get a string representation of the check result."""
        return "Success" if self.success else f"Failed: {self.error_message}"

    def __repr__(self) -> str:
        """Get a string representation of the check result."""
        return (
            f"CheckResult(success={self.success}, "
            f"error_message={self.error_message or self.internal_error})"
        )


class CloudConnector(abc.ABC):
    """A cloud connector is a deployed source or destination on Airbyte Cloud.

    You can use a connector object to manage the connector.
    """

    connector_type: ClassVar[Literal["source", "destination"]]
    """The type of the connector."""

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

    def check(
        self,
        *,
        raise_on_error: bool = True,
    ) -> CheckResult:
        """Check the connector.

        Returns:
            A `CheckResult` object containing the result. The object is truthy if the check was
            successful and falsy otherwise. The error message is available in the `error_message`
            or by converting the object to a string.
        """
        result = api_util.check_connector(
            workspace_id=self.workspace.workspace_id,
            connector_type=self.connector_type,
            actor_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
        )
        check_result = CheckResult(
            success=result[0],
            error_message=result[1],
        )
        if raise_on_error and not check_result:
            raise ValueError(f"Check failed: {check_result}")

        return check_result


class CloudSource(CloudConnector):
    """A cloud source is a source that is deployed on Airbyte Cloud."""

    connector_type: ClassVar[Literal["source", "destination"]] = "source"
    """The type of the connector."""

    @property
    def source_id(self) -> str:
        """Get the ID of the source.

        This is an alias for `connector_id`.
        """
        return self.connector_id


class CloudDestination(CloudConnector):
    """A cloud destination is a destination that is deployed on Airbyte Cloud."""

    connector_type: ClassVar[Literal["source", "destination"]] = "destination"
    """The type of the connector."""

    @property
    def destination_id(self) -> str:
        """Get the ID of the destination.

        This is an alias for `connector_id`.
        """
        return self.connector_id
