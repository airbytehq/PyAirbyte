# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

By overriding `api_root`, you can use this module to interact with self-managed Airbyte instances,
both OSS and Enterprise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from airbyte._util import api_util
from airbyte._util.api_util import (
    CLOUD_API_ROOT,
    create_connection,
    create_destination,
    create_source,
    delete_connection,
    delete_destination,
    delete_source,
    get_connection,
    get_workspace,
)
from airbyte.cloud._destinations import get_destination_config_from_cache
from airbyte.cloud._sync_results import SyncResult
from airbyte.sources.base import Source


if TYPE_CHECKING:
    from airbyte_api.models.shared.connectionresponse import ConnectionResponse
    from airbyte_api.models.shared.destinationresponse import DestinationResponse

    from airbyte.caches.base import CacheBase


@dataclass
class CloudWorkspace:
    """A remote workspace on the Airbyte Cloud.

    By overriding `api_root`, you can use this class to interact with self-managed Airbyte
    instances, both OSS and Enterprise.
    """

    workspace_id: str
    api_key: str
    api_root: str = CLOUD_API_ROOT

    def connect(self) -> None:
        """Check that the workspace is reachable and raise an exception otherwise."""
        _ = get_workspace(
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )

    def deploy_source(
        self,
        source: Source,
    ) -> str:
        """Deploy a source to the workspace.

        Returns the newly deployed source ID.
        """
        source_configuration = source.get_config().copy()
        source_configuration["sourceType"] = source.name.replace("source-", "")

        deployed_source = create_source(
            name=f"{source.name.replace('-', ' ').title()} (Deployed by PyAirbyte)",
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            config=source_configuration,
        )

        # Set the deployment Ids on the source object
        source._deployed_api_root = self.api_root  # noqa: SLF001  # Accessing nn-public API
        source._deployed_workspace_id = self.workspace_id  # noqa: SLF001  # Accessing nn-public API
        source._deployed_source_id = deployed_source.source_id  # noqa: SLF001  # Accessing nn-public API

        return deployed_source.source_id

    def delete_source(
        self,
        source: str | Source,
    ) -> None:
        """Delete a source from the workspace.

        You can pass either the source ID `str` or a deployed `Source` object.
        """
        if not isinstance(source, (str, Source)):
            raise ValueError(f"Invalid source type: {type(source)}")  # noqa: TRY004, TRY003

        if isinstance(source, Source):
            if not source._deployed_source_id:  # noqa: SLF001
                raise ValueError("Source has not been deployed.")  # noqa: TRY003

            source_id = source._deployed_source_id  # noqa: SLF001

        elif isinstance(source, str):
            source_id = source

        delete_source(
            source_id=source_id,
            api_root=self.api_root,
            api_key=self.api_key,
        )

    def deploy_cache_as_destination(
        self,
        cache: CacheBase,
    ) -> str:
        """Deploy a cache to the workspace as a new destination.

        Returns the newly deployed destination ID.
        """
        cache_type_name = cache.__class__.__name__.replace("Cache", "")

        deployed_destination: DestinationResponse = create_destination(
            name=f"Destination {cache_type_name} (Deployed by PyAirbyte)",
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

    def delete_destination(
        self,
        *,
        destination_id: str | None = None,
        cache: CacheBase | None = None,
    ) -> None:
        """Delete a deployed destination from the workspace.

        You can pass either the `Cache` class or the deployed destination ID as a `str`.
        """
        if destination_id is None and cache is None:
            raise ValueError("You must provide either a destination ID or a cache object.")  # noqa: TRY003
        if destination_id is not None and cache is not None:
            raise ValueError(  # noqa: TRY003
                "You must provide either a destination ID or a cache object, not both."
            )

        if cache:
            if not cache._deployed_destination_id:  # noqa: SLF001
                raise ValueError("Cache has not been deployed.")  # noqa: TRY003

            destination_id = cache._deployed_destination_id  # noqa: SLF001

        if destination_id is None:
            raise ValueError("No destination ID provided.")  # noqa: TRY003

        delete_destination(
            destination_id=destination_id,
            api_root=self.api_root,
            api_key=self.api_key,
        )

    def deploy_connection(
        self,
        source: Source,
        cache: CacheBase,
    ) -> str:
        """Deploy a source and cache to the workspace as a new connection.

        Returns the newly deployed connection ID as a `str`.
        """
        self.deploy_source(source)
        self.deploy_cache_as_destination(cache)

        assert source._deployed_source_id is not None  # noqa: SLF001  # Accessing nn-public API
        assert cache._deployed_destination_id is not None  # noqa: SLF001  # Accessing nn-public API

        deployed_connection = create_connection(
            name=f"Connection {source.name.replace('-', ' ').title()} (Deployed by PyAirbyte)",
            source_id=source._deployed_source_id,  # noqa: SLF001  # Accessing nn-public API
            destination_id=cache._deployed_destination_id,  # noqa: SLF001  # Accessing nn-public API
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )

        source._deployed_connection_id = deployed_connection.connection_id  # noqa: SLF001
        cache._deployed_connection_id = deployed_connection.connection_id  # noqa: SLF001

        return deployed_connection.connection_id

    def delete_connection(
        self,
        connection_id: str | None,
        *,
        delete_source: bool = False,
        delete_destination: bool = False,
    ) -> None:
        """Delete a deployed connection from the workspace."""
        if connection_id is None:
            raise ValueError("No connection ID provided.")  # noqa: TRY003

        connection: ConnectionResponse = get_connection(
            connection_id=connection_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )
        delete_connection(
            connection_id=connection_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )
        if delete_source:
            self.delete_source(source=connection.source_id)

        if delete_destination:
            self.delete_destination(destination_id=connection.destination_id)

    def run_sync(
        self,
        connection_id: str,
    ) -> SyncResult:
        """Run a sync on a deployed connection."""
        connection_response = api_util.run_connection(
            connection_id=connection_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            wait_for_job=True,
            raise_on_failure=True,
        )
        return SyncResult(
            workspace=self,
            connection_id=connection_response.connection_id,
            job_id=connection_response.job_id,
        )
