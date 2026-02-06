# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud Connections."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airbyte._util import api_util
from airbyte.cloud.connection_state import (
    ConnectionStateResponse,
    _get_stream_list,
    _match_stream,
)
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.sync_results import SyncResult
from airbyte.exceptions import AirbyteWorkspaceMismatchError, PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte_api.models import ConnectionResponse, JobResponse, JobTypeEnum

    from airbyte.cloud.workspaces import CloudWorkspace


class CloudConnection:  # noqa: PLR0904  # Too many public methods
    """A connection is an extract-load (EL) pairing of a source and destination in Airbyte Cloud.

    You can use a connection object to run sync jobs, retrieve logs, and manage the connection.
    """

    def __init__(
        self,
        workspace: CloudWorkspace,
        connection_id: str,
        source: str | None = None,
        destination: str | None = None,
    ) -> None:
        """It is not recommended to create a `CloudConnection` object directly.

        Instead, use `CloudWorkspace.get_connection()` to create a connection object.
        """
        self.connection_id = connection_id
        """The ID of the connection."""

        self.workspace = workspace
        """The workspace that the connection belongs to."""

        self._source_id = source
        """The ID of the source."""

        self._destination_id = destination
        """The ID of the destination."""

        self._connection_info: ConnectionResponse | None = None
        """The connection info object. (Cached.)"""

        self._cloud_source_object: CloudSource | None = None
        """The source object. (Cached.)"""

        self._cloud_destination_object: CloudDestination | None = None
        """The destination object. (Cached.)"""

    def _fetch_connection_info(
        self,
        *,
        force_refresh: bool = False,
        verify: bool = True,
    ) -> ConnectionResponse:
        """Fetch and cache connection info from the API.

        By default, this method will only fetch from the API if connection info is not
        already cached. It also verifies that the connection belongs to the expected
        workspace unless verification is explicitly disabled.

        Args:
            force_refresh: If True, always fetch from the API even if cached.
                If False (default), only fetch if not already cached.
            verify: If True (default), verify that the connection is valid (e.g., that
                the workspace_id matches this object's workspace). Raises an error if
                validation fails.

        Returns:
            The ConnectionResponse from the API.

        Raises:
            AirbyteWorkspaceMismatchError: If verify is True and the connection's
                workspace_id doesn't match the expected workspace.
            AirbyteMissingResourceError: If the connection doesn't exist.
        """
        if not force_refresh and self._connection_info is not None:
            # Use cached info, but still verify if requested
            if verify:
                self._verify_workspace_match(self._connection_info)
            return self._connection_info

        # Fetch from API
        connection_info = api_util.get_connection(
            workspace_id=self.workspace.workspace_id,
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )

        # Cache the result first (before verification may raise)
        self._connection_info = connection_info

        # Verify if requested
        if verify:
            self._verify_workspace_match(connection_info)

        return connection_info

    def _verify_workspace_match(self, connection_info: ConnectionResponse) -> None:
        """Verify that the connection belongs to the expected workspace.

        Raises:
            AirbyteWorkspaceMismatchError: If the workspace IDs don't match.
        """
        if connection_info.workspace_id != self.workspace.workspace_id:
            raise AirbyteWorkspaceMismatchError(
                resource_type="connection",
                resource_id=self.connection_id,
                workspace=self.workspace,
                expected_workspace_id=self.workspace.workspace_id,
                actual_workspace_id=connection_info.workspace_id,
                message=(
                    f"Connection '{self.connection_id}' belongs to workspace "
                    f"'{connection_info.workspace_id}', not '{self.workspace.workspace_id}'."
                ),
            )

    def check_is_valid(self) -> bool:
        """Check if this connection exists and belongs to the expected workspace.

        This method fetches connection info from the API (if not already cached) and
        verifies that the connection's workspace_id matches the workspace associated
        with this CloudConnection object.

        Returns:
            True if the connection exists and belongs to the expected workspace.

        Raises:
            AirbyteWorkspaceMismatchError: If the connection belongs to a different workspace.
            AirbyteMissingResourceError: If the connection doesn't exist.
        """
        self._fetch_connection_info(force_refresh=False, verify=True)
        return True

    @classmethod
    def _from_connection_response(
        cls,
        workspace: CloudWorkspace,
        connection_response: ConnectionResponse,
    ) -> CloudConnection:
        """Create a CloudConnection from a ConnectionResponse."""
        result = cls(
            workspace=workspace,
            connection_id=connection_response.connection_id,
            source=connection_response.source_id,
            destination=connection_response.destination_id,
        )
        result._connection_info = connection_response  # noqa: SLF001 # Accessing Non-Public API
        return result

    # Properties

    @property
    def name(self) -> str | None:
        """Get the display name of the connection, if available.

        E.g. "My Postgres to Snowflake", not the connection ID.
        """
        if not self._connection_info:
            self._connection_info = self._fetch_connection_info()

        return self._connection_info.name

    @property
    def source_id(self) -> str:
        """The ID of the source."""
        if not self._source_id:
            if not self._connection_info:
                self._connection_info = self._fetch_connection_info()

            self._source_id = self._connection_info.source_id

        return self._source_id

    @property
    def source(self) -> CloudSource:
        """Get the source object."""
        if self._cloud_source_object:
            return self._cloud_source_object

        self._cloud_source_object = CloudSource(
            workspace=self.workspace,
            connector_id=self.source_id,
        )
        return self._cloud_source_object

    @property
    def destination_id(self) -> str:
        """The ID of the destination."""
        if not self._destination_id:
            if not self._connection_info:
                self._connection_info = self._fetch_connection_info()

            self._destination_id = self._connection_info.destination_id

        return self._destination_id

    @property
    def destination(self) -> CloudDestination:
        """Get the destination object."""
        if self._cloud_destination_object:
            return self._cloud_destination_object

        self._cloud_destination_object = CloudDestination(
            workspace=self.workspace,
            connector_id=self.destination_id,
        )
        return self._cloud_destination_object

    @property
    def stream_names(self) -> list[str]:
        """The stream names."""
        if not self._connection_info:
            self._connection_info = self._fetch_connection_info()

        return [stream.name for stream in self._connection_info.configurations.streams or []]

    @property
    def table_prefix(self) -> str:
        """The table prefix."""
        if not self._connection_info:
            self._connection_info = self._fetch_connection_info()

        return self._connection_info.prefix or ""

    @property
    def connection_url(self) -> str | None:
        """The web URL to the connection."""
        return f"{self.workspace.workspace_url}/connections/{self.connection_id}"

    @property
    def job_history_url(self) -> str | None:
        """The URL to the job history for the connection."""
        return f"{self.connection_url}/timeline"

    # Run Sync

    def run_sync(
        self,
        *,
        wait: bool = True,
        wait_timeout: int = 300,
    ) -> SyncResult:
        """Run a sync."""
        connection_response = api_util.run_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            workspace_id=self.workspace.workspace_id,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        sync_result = SyncResult(
            workspace=self.workspace,
            connection=self,
            job_id=connection_response.job_id,
        )

        if wait:
            sync_result.wait_for_completion(
                wait_timeout=wait_timeout,
                raise_failure=True,
                raise_timeout=True,
            )

        return sync_result

    def __repr__(self) -> str:
        """String representation of the connection."""
        return (
            f"CloudConnection(connection_id={self.connection_id}, source_id={self.source_id}, "
            f"destination_id={self.destination_id}, connection_url={self.connection_url})"
        )

    # Logs

    def get_previous_sync_logs(
        self,
        *,
        limit: int = 20,
        offset: int | None = None,
        from_tail: bool = True,
        job_type: JobTypeEnum | None = None,
    ) -> list[SyncResult]:
        """Get previous sync jobs for a connection with pagination support.

        Returns SyncResult objects containing job metadata (job_id, status, bytes_synced,
        rows_synced, start_time). Full log text can be fetched lazily via
        `SyncResult.get_full_log_text()`.

        Args:
            limit: Maximum number of jobs to return. Defaults to 20.
            offset: Number of jobs to skip from the beginning. Defaults to None (0).
            from_tail: If True, returns jobs ordered newest-first (createdAt DESC).
                If False, returns jobs ordered oldest-first (createdAt ASC).
                Defaults to True.
            job_type: Filter by job type (e.g., JobTypeEnum.SYNC, JobTypeEnum.REFRESH).
                If not specified, defaults to sync and reset jobs only (API default behavior).

        Returns:
            A list of SyncResult objects representing the sync jobs.
        """
        order_by = (
            api_util.JOB_ORDER_BY_CREATED_AT_DESC
            if from_tail
            else api_util.JOB_ORDER_BY_CREATED_AT_ASC
        )
        sync_logs: list[JobResponse] = api_util.get_job_logs(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            workspace_id=self.workspace.workspace_id,
            limit=limit,
            offset=offset,
            order_by=order_by,
            job_type=job_type,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return [
            SyncResult(
                workspace=self.workspace,
                connection=self,
                job_id=sync_log.job_id,
                _latest_job_info=sync_log,
            )
            for sync_log in sync_logs
        ]

    def get_sync_result(
        self,
        job_id: int | None = None,
    ) -> SyncResult | None:
        """Get the sync result for the connection.

        If `job_id` is not provided, the most recent sync job will be used.

        Returns `None` if job_id is omitted and no previous jobs are found.
        """
        if job_id is None:
            # Get the most recent sync job
            results = self.get_previous_sync_logs(
                limit=1,
            )
            if results:
                return results[0]

            return None

        # Get the sync job by ID (lazy loaded)
        return SyncResult(
            workspace=self.workspace,
            connection=self,
            job_id=job_id,
        )

    # Artifacts

    def get_state_artifacts(self) -> list[dict[str, Any]] | None:
        """Get the connection state artifacts.

        Returns the persisted state for this connection, which can be used
        when debugging incremental syncs.

        Uses the Config API endpoint: POST /v1/state/get

        Returns:
            List of state objects for each stream, or None if no state is set.
        """
        state_response = api_util.get_connection_state(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        if state_response.get("stateType") == "not_set":
            return None
        return state_response.get("streamState", [])

    def get_state(
        self,
        *,
        stream_name: str | None = None,
        stream_namespace: str | None = None,
    ) -> ConnectionStateResponse:
        """Get the current state for this connection as a typed model.

        Returns the connection's sync state, which tracks progress for incremental syncs.
        The state can be one of: stream (per-stream), global, legacy, or not_set.

        When stream_name is provided, filters the response to include only the
        matching stream's state. Raises an error if the stream is not found.

        Args:
            stream_name: Optional stream name to filter state for a single stream.
            stream_namespace: Optional stream namespace to narrow the stream filter.
                Only used when stream_name is also provided.

        Returns:
            A ConnectionStateResponse model with the connection's state.
        """
        state_data = api_util.get_connection_state(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        result = ConnectionStateResponse(**state_data)

        if stream_name is None:
            return result

        streams = _get_stream_list(result)
        matching = [s for s in streams if _match_stream(s, stream_name, stream_namespace)]

        if not matching:
            available = [s.stream_descriptor.name for s in streams]
            raise PyAirbyteInputError(
                message=f"Stream '{stream_name}' not found in connection state.",
                context={
                    "connection_id": self.connection_id,
                    "stream_name": stream_name,
                    "stream_namespace": stream_namespace,
                    "available_streams": available,
                },
            )

        if result.state_type == "stream":
            result.stream_state = matching
        elif result.state_type == "global" and result.global_state:
            result.global_state.stream_states = matching

        return result

    def set_state(
        self,
        connection_state: dict[str, Any],
    ) -> ConnectionStateResponse:
        """Set (create or update) the full state for this connection.

        Uses the safe variant that prevents updates while a sync is running (HTTP 423).

        Args:
            connection_state: The full ConnectionState object to set. Must include:
                - stateType: "global", "stream", or "legacy"
                - connectionId: Must match this connection's ID
                - One of: state (legacy), streamState (stream), globalState (global)

        Returns:
            A ConnectionStateResponse model with the updated state.
        """
        updated_data = api_util.create_or_update_connection_state_safe(
            connection_id=self.connection_id,
            connection_state=connection_state,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return ConnectionStateResponse(**updated_data)

    def get_stream_state(
        self,
        stream_name: str,
        stream_namespace: str | None = None,
    ) -> ConnectionStateResponse:
        """Get the state for a single stream within this connection.

        This is a convenience wrapper around `get_state()` with stream filtering.

        Args:
            stream_name: The name of the stream to get state for.
            stream_namespace: Optional stream namespace to narrow the filter.

        Returns:
            A ConnectionStateResponse filtered to include only the matching stream.

        Raises:
            PyAirbyteInputError: If the stream is not found in the connection state.
        """
        return self.get_state(
            stream_name=stream_name,
            stream_namespace=stream_namespace,
        )

    def set_stream_state(
        self,
        stream_name: str,
        stream_state: dict[str, Any],
        stream_namespace: str | None = None,
    ) -> ConnectionStateResponse:
        """Set the state for a single stream within this connection.

        Fetches the current full state, replaces only the specified stream's state,
        then sends the full updated state back to the API. If the stream does not
        exist in the current state, it is appended.

        Uses the safe variant that prevents updates while a sync is running (HTTP 423).

        Args:
            stream_name: The name of the stream to update state for.
            stream_state: The state blob for this stream (e.g., {"cursor": "2024-01-01"}).
            stream_namespace: Optional stream namespace to identify the stream.

        Returns:
            A ConnectionStateResponse model with the updated state.

        Raises:
            PyAirbyteInputError: If the connection has no existing state or uses legacy state.
        """
        current = self.get_state()

        if current.state_type == "not_set":
            raise PyAirbyteInputError(
                message="Cannot set stream state: connection has no existing state.",
                context={"connection_id": self.connection_id},
            )

        if current.state_type == "legacy":
            raise PyAirbyteInputError(
                message="Cannot set stream state on a legacy-type connection state.",
                context={"connection_id": self.connection_id},
            )

        new_stream_entry = {
            "streamDescriptor": {
                "name": stream_name,
                **(
                    {
                        "namespace": stream_namespace,
                    }
                    if stream_namespace
                    else {}
                ),
            },
            "streamState": stream_state,
        }

        streams = _get_stream_list(current)
        found = False
        updated_streams_raw: list[dict[str, Any]] = []
        for s in streams:
            if _match_stream(s, stream_name, stream_namespace):
                updated_streams_raw.append(new_stream_entry)
                found = True
            else:
                updated_streams_raw.append(s.model_dump(by_alias=True))

        if not found:
            updated_streams_raw.append(new_stream_entry)

        full_state: dict[str, Any] = {
            "stateType": current.state_type,
            "connectionId": self.connection_id,
        }

        if current.state_type == "stream":
            full_state["streamState"] = updated_streams_raw
        elif current.state_type == "global" and current.global_state:
            full_state["globalState"] = {
                "sharedState": current.global_state.shared_state,
                "streamStates": updated_streams_raw,
            }

        return self.set_state(full_state)

    def get_catalog_artifact(self) -> dict[str, Any] | None:
        """Get the configured catalog for this connection.

        Returns the full configured catalog (syncCatalog) for this connection,
        including stream schemas, sync modes, cursor fields, and primary keys.

        Uses the Config API endpoint: POST /v1/web_backend/connections/get

        Returns:
            Dictionary containing the configured catalog, or `None` if not found.
        """
        connection_response = api_util.get_connection_catalog(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return connection_response.get("syncCatalog")

    def rename(self, name: str) -> CloudConnection:
        """Rename the connection.

        Args:
            name: New name for the connection

        Returns:
            Updated CloudConnection object with refreshed info
        """
        updated_response = api_util.patch_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            name=name,
        )
        self._connection_info = updated_response
        return self

    def set_table_prefix(self, prefix: str) -> CloudConnection:
        """Set the table prefix for the connection.

        Args:
            prefix: New table prefix to use when syncing to the destination

        Returns:
            Updated CloudConnection object with refreshed info
        """
        updated_response = api_util.patch_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            prefix=prefix,
        )
        self._connection_info = updated_response
        return self

    def set_selected_streams(self, stream_names: list[str]) -> CloudConnection:
        """Set the selected streams for the connection.

        This is a destructive operation that can break existing connections if the
        stream selection is changed incorrectly. Use with caution.

        Args:
            stream_names: List of stream names to sync

        Returns:
            Updated CloudConnection object with refreshed info
        """
        configurations = api_util.build_stream_configurations(stream_names)

        updated_response = api_util.patch_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            configurations=configurations,
        )
        self._connection_info = updated_response
        return self

    # Enable/Disable

    @property
    def enabled(self) -> bool:
        """Get the current enabled status of the connection.

        This property always fetches fresh data from the API to ensure accuracy,
        as another process or user may have toggled the setting.

        Returns:
            True if the connection status is 'active', False otherwise.
        """
        connection_info = self._fetch_connection_info(force_refresh=True)
        return connection_info.status == api_util.models.ConnectionStatusEnum.ACTIVE

    @enabled.setter
    def enabled(self, value: bool) -> None:
        """Set the enabled status of the connection.

        Args:
            value: True to enable (set status to 'active'), False to disable
                (set status to 'inactive').
        """
        self.set_enabled(enabled=value)

    def set_enabled(
        self,
        *,
        enabled: bool,
        ignore_noop: bool = True,
    ) -> None:
        """Set the enabled status of the connection.

        Args:
            enabled: True to enable (set status to 'active'), False to disable
                (set status to 'inactive').
            ignore_noop: If True (default), silently return if the connection is already
                in the requested state. If False, raise ValueError when the requested
                state matches the current state.

        Raises:
            ValueError: If ignore_noop is False and the connection is already in the
                requested state.
        """
        # Always fetch fresh data to check current status
        connection_info = self._fetch_connection_info(force_refresh=True)
        current_status = connection_info.status
        desired_status = (
            api_util.models.ConnectionStatusEnum.ACTIVE
            if enabled
            else api_util.models.ConnectionStatusEnum.INACTIVE
        )

        if current_status == desired_status:
            if ignore_noop:
                return
            raise ValueError(
                f"Connection is already {'enabled' if enabled else 'disabled'}. "
                f"Current status: {current_status}"
            )

        updated_response = api_util.patch_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            status=desired_status,
        )
        self._connection_info = updated_response

    # Scheduling

    def set_schedule(
        self,
        cron_expression: str,
    ) -> None:
        """Set a cron schedule for the connection.

        Args:
            cron_expression: A cron expression defining when syncs should run.

        Examples:
                - "0 0 * * *" - Daily at midnight UTC
                - "0 */6 * * *" - Every 6 hours
                - "0 0 * * 0" - Weekly on Sunday at midnight UTC
        """
        schedule = api_util.models.AirbyteAPIConnectionSchedule(
            schedule_type=api_util.models.ScheduleTypeEnum.CRON,
            cron_expression=cron_expression,
        )
        updated_response = api_util.patch_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            schedule=schedule,
        )
        self._connection_info = updated_response

    def set_manual_schedule(self) -> None:
        """Set the connection to manual scheduling.

        Disables automatic syncs. Syncs will only run when manually triggered.
        """
        schedule = api_util.models.AirbyteAPIConnectionSchedule(
            schedule_type=api_util.models.ScheduleTypeEnum.MANUAL,
        )
        updated_response = api_util.patch_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            schedule=schedule,
        )
        self._connection_info = updated_response

    # Deletions

    def permanently_delete(
        self,
        *,
        cascade_delete_source: bool = False,
        cascade_delete_destination: bool = False,
    ) -> None:
        """Delete the connection.

        Args:
            cascade_delete_source: Whether to also delete the source.
            cascade_delete_destination: Whether to also delete the destination.
        """
        self.workspace.permanently_delete_connection(self)

        if cascade_delete_source:
            self.workspace.permanently_delete_source(self.source_id)

        if cascade_delete_destination:
            self.workspace.permanently_delete_destination(self.destination_id)
