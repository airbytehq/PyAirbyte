# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

By overriding `api_root`, you can use this module to interact with self-managed Airbyte instances,
both OSS and Enterprise.
"""

from __future__ import annotations

import warnings
from contextlib import suppress
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from airbyte import exceptions as exc
from airbyte._util import iter as iter_util
from airbyte.cloud import _api_util
from airbyte.cloud._destination_util import get_destination_config_from_cache
from airbyte.cloud._resources import ICloudResource
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import CloudConnector
from airbyte.cloud.constants import ConnectorTypeEnum
from airbyte.cloud.sync_results import SyncResult


if TYPE_CHECKING:
    from airbyte._util.api_imports import DestinationResponse
    from airbyte.caches.base import CacheBase
    from airbyte.sources.base import Source


# Decorator for resolving connection objects
def resolve_connection(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(
        self: CloudWorkspace,
        *args: Any,  # noqa: ANN401
        connection: str | CloudConnection | None = None,
        connection_id: str | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        if connection_id is not None:
            warnings.warn(
                message="The `connection_id` parameter is deprecated. Use `connection` instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            connection = connection_id

        if isinstance(connection, str):
            connection = CloudConnection(
                workspace=self,
                connection_id=connection,
            )

        return func(self, *args, connection=connection, **kwargs)

    return wrapper


# Decorator for resolving source objects
def resolve_source(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(
        self: CloudWorkspace,
        *args: Any,  # noqa: ANN401
        source: str | CloudConnector | None = None,
        source_id: str | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        if source_id is not None:
            warnings.warn(
                "The `source_id` parameter is deprecated. Use the `source` parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            source = source_id

        if isinstance(source, str):
            source = CloudConnector(
                workspace=self,
                connector_id=source,
                connector_type=ConnectorTypeEnum.SOURCE,
            )

        return func(self, *args, source=source, **kwargs)

    return wrapper


# Decorator for resolving source IDs from objects
def resolve_source_id(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(
        self: CloudWorkspace,
        *args: Any,  # noqa: ANN401
        source: str | CloudConnector | None = None,
        source_id: str | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        if not isinstance(source, (str, CloudConnector)):
            raise ValueError(f"Invalid source type: {type(source)}")  # noqa: TRY004, TRY003

        if source_id is not None:
            warnings.warn(
                "The `source_id` parameter is deprecated. Use the `source` parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            source = source_id

        if isinstance(source, CloudConnector):
            source = source.source_id

        return func(self, *args, source=source, **kwargs)

    return wrapper


# Decorator for resolving destination objects
def resolve_destination(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(
        self: CloudWorkspace,
        *args: Any,  # noqa: ANN401
        destination: str | CloudConnector | None = None,
        destination_id: str | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        if destination_id is not None:
            warnings.warn(
                "The `destination_id` parameter is deprecated. Use `destination` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            destination = destination_id

        if isinstance(destination, str):
            destination = CloudConnector(
                workspace=self,
                connector_id=destination,
                connector_type=ConnectorTypeEnum.DESTINATION,
            )

        return func(self, *args, destination=destination, **kwargs)

    return wrapper


# Decorator for resolving destination IDs from objects
def resolve_destination_id(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(
        self: CloudWorkspace,
        *args: Any,  # noqa: ANN401
        destination: str | CloudConnector | None = None,
        destination_id: str | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> Any:  # noqa: ANN401
        if destination is None and destination_id is None:
            raise exc.PyAirbyteInputError(
                message="No destination or destination ID provided.",
            )

        if destination_id is not None:
            warnings.warn(
                "The `destination_id` parameter is deprecated. Use `destination` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            destination = destination_id

        if isinstance(destination, CloudConnector):
            destination = destination.connector_id

        return func(self, *args, destination=destination, **kwargs)

    return wrapper


@dataclass
class CloudWorkspace(ICloudResource):
    """A remote workspace on the Airbyte Cloud.

    By overriding `api_root`, you can use this class to interact with self-managed Airbyte
    instances, both OSS and Enterprise.
    """

    workspace_id: str
    api_key: str
    api_root: str = _api_util.CLOUD_API_ROOT

    @property
    def workspace_url(self) -> str | None:
        return f"{self.api_root}/workspaces/{self.workspace_id}"

    # Test connection and creds

    def connect(self) -> None:
        """Check that the workspace is reachable and raise an exception otherwise.

        Note: It is not necessary to call this method before calling other operations. It
              serves primarily as a simple check to ensure that the workspace is reachable
              and credentials are correct.
        """
        _ = _api_util.fetch_workspace_info(
            workspace_id=self.workspace_id,
            api_root=self.api_root,
            api_key=self.api_key,
        )

        print("Successfully connected to workspace.")

    # Deploy and delete sources

    def deploy_source(
        self,
        source: Source,
        name: str,
    ) -> CloudConnector:
        """Deploy a source to the workspace.

        This method will deploy a source to the workspace and return the `CloudConnector` object of
        the deployed source.

        Args:
            name (str): The key to use for the source name. This is used to provide
                idempotency when deploying the same source multiple times. If `None`, then
                `source_id` is required. If a matching source source is found and `update_existing`
                is `False`, then a `AirbyteResourceAlreadyExists` exception will be raised.
            source (Source): The source to deploy.
        """
        source_configuration: dict[str, Any] = source.get_config().copy()
        source_configuration["sourceType"] = source.name.replace("source-", "")

        iter_util.no_existing_resources(
            _api_util.list_sources(
                workspace_id=self.workspace_id,
                name_filter=name,
                api_root=self.api_root,
                api_key=self.api_key,
                limit=1,
            ),
        )

        deployed_source = _api_util.create_source(
            name=name,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            config=source_configuration,
        )

        # Set the deployment IDs on the source object
        source._deployed_api_root = self.api_root  # noqa: SLF001  # Accessing nn-public API
        source._deployed_workspace_id = self.workspace_id  # noqa: SLF001  # Accessing nn-public API
        source._deployed_source_id = deployed_source.source_id  # noqa: SLF001  # Accessing nn-public API

        return CloudConnector(
            workspace=self,
            connector_id=deployed_source.source_id,
            connector_type=ConnectorTypeEnum.SOURCE,
        )

    def get_source(
        self,
        source_id: str,
    ) -> CloudConnector:
        """Get a source by ID.

        This method does not fetch data from the API. It returns a `CloudConnector` object, which
        will be loaded lazily as needed.
        """
        result = CloudConnector(
            workspace=self,
            connector_id=source_id,
            connector_type=ConnectorTypeEnum.SOURCE,
        )
        if result.connector_type != "source":
            raise exc.PyAirbyteInputError(message="Connector is not a source.")

        return result

    @resolve_source_id
    def permanently_delete_source(
        self,
        source: str | CloudConnector,
    ) -> None:
        """Delete a source from the workspace.

        You can pass either the source ID `str` or a `CloudConnector` object.
        """
        assert isinstance(source, str), "Decorator should resolve source ID."

        _api_util.delete_source(
            source_id=source,
            api_root=self.api_root,
            api_key=self.api_key,
        )

    # Deploy and delete destinations

    def deploy_cache_as_destination(
        self,
        cache: CacheBase,
        *,
        name: str,
    ) -> str:
        """Deploy a cache to the workspace as a new destination.

        Returns the newly deployed destination ID.
        """
        deployed_destination: DestinationResponse = _api_util.create_destination(
            name=name,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            config=get_destination_config_from_cache(cache),
        )

        # Set the deployment Ids on the source object
        cache._deployed_api_root = self.api_root  # noqa: SLF001  # Accessing nn-public API
        cache._deployed_workspace_id = self.workspace_id  # noqa: SLF001  # Accessing nn-public API
        cache._deployed_destination_id = deployed_destination.destination_id  # noqa: SLF001  # Accessing nn-public API

        return deployed_destination.destination_id

    def get_destination(
        self,
        destination_id: str,
    ) -> CloudConnector:
        """Get a destination by ID.

        This method does not fetch data from the API. It returns a `CloudConnector` object, which
        will be loaded lazily as needed.
        """
        return CloudConnector(
            workspace=self,
            connector_id=destination_id,
            connector_type=ConnectorTypeEnum.DESTINATION,
        )

    @resolve_destination_id
    def permanently_delete_destination(
        self,
        *,
        destination: str | CloudConnector | None = None,
    ) -> None:
        """Delete a deployed destination from the workspace.

        You can pass either the `Cache` class or the deployed destination ID as a `str`.
        """
        assert isinstance(destination, str), "Decorator should resolve destination ID."
        _api_util.delete_destination(
            destination_id=destination,
            api_root=self.api_root,
            api_key=self.api_key,
        )

    # Deploy and delete connections

    @resolve_source_id
    @resolve_destination_id
    def deploy_connection(
        self,
        name: str,
        *,
        source: CloudConnector | str,
        destination: CloudConnector | str,
        table_prefix: str | None = None,
        selected_streams: list[str] | None = None,
    ) -> CloudConnection:
        """Deploy a source and cache to the workspace as a new connection.

        Returns the newly deployed connection ID as a `str`.

        Args:
            source: The source to deploy. You can pass either an already deployed
                source ID `str` or a PyAirbyte `Source` object. If you pass a `Source` object,
                it will be deployed automatically.
            destination (str, optional): The cache, destination, or destination ID to use.
        """
        assert isinstance(source, str), "Decorator should resolve source ID."
        assert isinstance(destination, str), "Decorator should resolve destination ID."
        existing_connection: CloudConnection | None = None
        with suppress(exc.AirbyteMissingResourceError):
            existing_connection = _api_util.fetch_connection_by_name(
                connection_name=name,
                workspace_id=self.workspace_id,
                api_root=self.api_root,
                api_key=self.api_key,
            )
        if existing_connection:
            raise exc.AirbyteResourceAlreadyExistsError(
                message="Connection with matching name key already exists.",
                context={
                    "name_key": name,
                    "connection_id": existing_connection.connection_id,
                },
            )

        deployed_connection = _api_util.create_connection(
            name=name,
            source_id=source,
            destination_id=destination,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            selected_stream_names=selected_streams,
            prefix=table_prefix or "",
        )

        return CloudConnection(
            workspace=self,
            connection_id=deployed_connection.connection_id,
            _source_id=deployed_connection.source_id,
            _destination_id=deployed_connection.destination_id,
        )

    def get_connection(
        self,
        *,
        connection_id: str | None = None,
        name: str | None = None,
    ) -> CloudConnection:
        """Get a connection by ID.

        This method does not fetch data from the API. It returns a `CloudConnection` object,
        which will be loaded lazily as needed.
        """
        if connection_id is None and name is None:
            raise exc.PyAirbyteInputError(message="No connection ID or name key provided.")
        if connection_id and name:
            raise exc.PyAirbyteInputError(
                message="You can provide either a connection ID or a name key, but not both."
            )

        if connection_id:
            return CloudConnection(
                workspace=self,
                connection_id=connection_id,
            )

        assert isinstance(name, str), "Name should be a string, per above validation."

        # Else derive connection ID from name key
        connection = _api_util.fetch_connection_by_name(
            connection_name=name,
            workspace_id=self.workspace_id,
            api_root=self.api_root,
            api_key=self.api_key,
        )

        return CloudConnection(
            workspace=self,
            connection_id=connection.connection_id,
            _source_id=connection.source_id,
            _destination_id=connection.destination_id,
        )

    def permanently_delete_connection(
        self,
        connection: str | CloudConnection,
        *,
        delete_source: bool = False,
        delete_destination: bool = False,
    ) -> None:
        """Delete a deployed connection from the workspace."""
        if connection is None:
            raise ValueError("No connection ID provided.")  # noqa: TRY003

        if isinstance(connection, str):
            connection = CloudConnection(
                workspace=self,
                connection_id=connection,
            )

        _api_util.delete_connection(
            connection_id=connection.connection_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )
        if delete_source:
            self.permanently_delete_source(source=connection.source_id)

        if delete_destination:
            self.permanently_delete_destination(destination=connection.destination_id)

    # Run syncs

    @resolve_connection
    def run_sync(
        self,
        connection: str | CloudConnection,
        *,
        wait: bool = True,
        wait_timeout: int = 300,
        connection_id: str | None = None,
    ) -> SyncResult:
        """Run a sync on a deployed connection.

        Note: The `connection_id` parameter is deprecated. Use the `connection` parameter instead.
        """
        _ = connection_id  # Deprecated
        assert isinstance(connection, CloudConnection), "Decorate should have resolved this."
        return connection.run_sync(wait=wait, wait_timeout=wait_timeout)

    # Get sync results and previous sync logs

    def get_sync_result(
        self,
        connection_id: str,
        job_id: str | None = None,
    ) -> SyncResult | None:
        """Get the sync result for a connection job.

        If `job_id` is not provided, the most recent sync job will be used.

        Returns `None` if job_id is omitted and no previous jobs are found.
        """
        connection = CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )
        if job_id is None:
            results = self.get_previous_sync_logs(
                connection_id=connection_id,
                limit=1,
            )
            if results:
                return results[0]

            return None
        connection = CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )
        return SyncResult(
            workspace=self,
            connection=connection,
            job_id=job_id,
            table_name_prefix=connection.table_prefix,
        )

    def get_previous_sync_logs(
        self,
        connection_id: str,
        *,
        limit: int = 10,
    ) -> list[SyncResult]:
        """Get the previous sync logs for a connection."""
        connection = CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )
        return connection.get_previous_sync_logs(
            limit=limit,
        )
