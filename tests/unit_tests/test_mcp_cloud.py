# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud MCP tools."""

from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Callable, cast
from unittest.mock import MagicMock

import pytest
import requests
from airbyte import Destination, Source
from airbyte._direct_connectors import connector_docs
from airbyte._direct_connectors.models import (
    CloudConnectorConnectionInfo,
    DirectAccessGuidance,
    DirectAccessGuidanceIndexEntry,
    DirectAccessGuidanceSection,
    ExternalApiExecuteResult,
    ExternalApiWriteAction,
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
)
from airbyte.cloud.connectors import (
    CheckResult,
    CloudSource,
    ConnectorFeature,
    ConnectorType,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.mcp._error_handling import (
    MCP_TOOL_USER_FACING_ERRORS,
    format_user_facing_error,
)
from airbyte.cloud.models import (
    CloudDefaultContextInfo,
    CloudOrganizationInfo,
    CloudWorkspaceInfo,
    JobStatusEnum,
)
from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp.cloud import (
    CloudConnectionResult,
    CloudConnectorDetailsResult,
    CloudConnectorResult,
    SyncJobResult,
)
from airbyte.exceptions import (
    AirbyteCloudApiError,
    AirbyteError,
    PyAirbyteInputError,
)
from fastmcp import Context


@dataclass
class _SyncResultLike:
    """Subset of `SyncResult` used by connection status tests."""

    job_id: int
    status: JobStatusEnum
    start_time: datetime
    bytes_synced: int = 0
    records_synced: int = 0
    job_url: str = "https://cloud.airbyte.com/jobs"

    def get_job_status(self) -> JobStatusEnum:
        """Return the configured job status."""
        return self.status

    def is_job_complete(self) -> bool:
        """Return whether the test sync job is complete."""
        return True


@dataclass
class _CloudConnectorLike:
    """Subset of `CloudConnector` used by tested MCP list tools."""

    connector_id: str
    connector_type: ConnectorType
    name: str
    connector_url: str
    enabled_features: frozenset[ConnectorFeature] = frozenset()


@dataclass
class _CheckableConnectorLike:
    """Subset of `CloudConnector` used by connector check tests."""

    connector_id: str
    connector_type: str
    result: CheckResult
    received_raise_on_error: bool | None = None

    def check(self, *, raise_on_error: bool = True) -> CheckResult:
        """Capture the error handling option and return the configured result."""
        self.received_raise_on_error = raise_on_error
        return self.result


class _ConnectorCheckWorkspace:
    """Return checkable source and destination test doubles."""

    def __init__(self, result: CheckResult) -> None:
        """Create source and destination test doubles."""
        self.source = _CheckableConnectorLike(
            connector_id="source-id",
            connector_type="source",
            result=result,
        )
        self.destination = _CheckableConnectorLike(
            connector_id="destination-id",
            connector_type="destination",
            result=result,
        )

    def get_source(self, *, source_id: str) -> _CheckableConnectorLike:
        """Return the source test double."""
        assert source_id == self.source.connector_id
        return self.source

    def get_destination(self, *, destination_id: str) -> _CheckableConnectorLike:
        """Return the destination test double."""
        assert destination_id == self.destination.connector_id
        return self.destination

    def get_connector(self, *, connector_id: str) -> _CheckableConnectorLike:
        """Return the untyped connector test double matching the ID."""
        if connector_id == self.source.connector_id:
            return self.source
        assert connector_id == self.destination.connector_id
        return self.destination


@dataclass
class _CloudConnectionLike:
    """Subset of `CloudConnection` used by tested MCP list tools."""

    connection_id: str
    name: str
    connection_url: str
    source_id: str
    destination_id: str
    failed: bool = False

    def get_previous_sync_logs(self, *, limit: int = 20) -> list[_SyncResultLike]:
        """Return one completed sync result for connection status tests."""
        _ = limit
        status = JobStatusEnum.FAILED if self.failed else JobStatusEnum.SUCCEEDED
        return [
            _SyncResultLike(
                job_id=1,
                status=status,
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        ]


@dataclass
class _CancelableConnectionLike:
    """Subset of `CloudConnection` used by sync cancellation tests."""

    sync_result: _SyncResultLike
    received_job_id: int | None = None

    def cancel_sync(self, *, job_id: int | None = None) -> _SyncResultLike:
        """Capture the job ID and return the cancelled sync result."""
        self.received_job_id = job_id
        return self.sync_result


class _CancellationWorkspace:
    """Return a connection test double for sync cancellation tests."""

    def __init__(self, connection: _CancelableConnectionLike) -> None:
        """Create a workspace test double."""
        self.connection = connection

    def get_connection(self, *, connection_id: str) -> _CancelableConnectionLike:
        """Return the configured connection."""
        assert connection_id == "connection-id"
        return self.connection


class _CloudWorkspace:
    """Capture `limit` values passed from MCP list tools."""

    def __init__(self) -> None:
        """Create a workspace test double."""
        self.limits: dict[str, int | None] = {}

    def list_connectors(
        self,
        *,
        connector_type: ConnectorType | None = None,
        feature_filter: ConnectorFeature | None = None,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> list[_CloudConnectorLike]:
        """Capture the list limit and mimic core filtering on connector test data."""
        assert connector_type is not None
        assert feature_filter is None
        self.limits[f"{connector_type.value}s"] = limit
        items = [
            _CloudConnectorLike(
                connector_id=f"{connector_type.value}-{index}",
                connector_type=connector_type,
                name="target" if index == 2 else "miss",
                connector_url=f"https://cloud.airbyte.com/{connector_type.value}-{index}",
            )
            for index in range(1, 3)
        ]
        if name_contains:
            items = [item for item in items if name_contains in item.name]
        return items if limit is None else items[:limit]

    def list_connections(
        self, *, limit: int | None = None
    ) -> list[_CloudConnectionLike]:
        """Capture connection list limit and return connection test data."""
        self.limits["connections"] = limit
        items = [
            _CloudConnectionLike(
                connection_id=f"connection-{index}",
                name="target" if index == 2 else "miss",
                connection_url=f"https://cloud.airbyte.com/connection-{index}",
                source_id=f"source-connection-{index}",
                destination_id=f"destination-connection-{index}",
                failed=index == 2,
            )
            for index in range(1, 3)
        ]
        return items if limit is None else items[:limit]


@pytest.mark.parametrize(
    "tool,limit_key,extra_kwargs",
    [
        pytest.param(
            cloud_mcp.list_cloud_connectors,
            "sources",
            {"connector_type": ConnectorType.SOURCE},
            id="sources",
        ),
        pytest.param(
            cloud_mcp.list_cloud_connectors,
            "destinations",
            {"connector_type": ConnectorType.DESTINATION},
            id="destinations",
        ),
        pytest.param(
            cloud_mcp.list_cloud_connections,
            "connections",
            {"with_connection_status": False, "failing_connections_only": False},
            id="connections",
        ),
    ],
)
def test_mcp_cloud_list_tools_pass_limit_to_workspace(
    monkeypatch: pytest.MonkeyPatch,
    tool: Callable[..., list[object]],
    limit_key: str,
    extra_kwargs: dict[str, object],
) -> None:
    """Verify Cloud MCP list tools forward `limit` to workspace list operations."""
    workspace = _CloudWorkspace()
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    results = tool(
        ctx=object(),
        workspace_id="workspace-id",
        name_contains=None,
        limit=1,
        **extra_kwargs,
    )

    assert workspace.limits[limit_key] == 1
    assert len(results) == 1


@pytest.mark.parametrize(
    "tool,limit_key,forwarded_limit,extra_kwargs",
    [
        pytest.param(
            cloud_mcp.list_cloud_connectors,
            "sources",
            1,
            {"connector_type": ConnectorType.SOURCE},
            id="sources",
        ),
        pytest.param(
            cloud_mcp.list_cloud_connectors,
            "destinations",
            1,
            {"connector_type": ConnectorType.DESTINATION},
            id="destinations",
        ),
        pytest.param(
            cloud_mcp.list_cloud_connections,
            "connections",
            None,
            {"with_connection_status": False, "failing_connections_only": False},
            id="connections",
        ),
    ],
)
def test_mcp_cloud_list_tools_apply_limit_after_name_filter(
    monkeypatch: pytest.MonkeyPatch,
    tool: Callable[
        ...,
        list[CloudConnectorResult] | list[CloudConnectionResult],
    ],
    limit_key: str,
    forwarded_limit: int | None,
    extra_kwargs: dict[str, object],
) -> None:
    """Verify Cloud MCP list tools cap results after name filtering.

    Connector tools delegate both filters to `CloudWorkspace.list_connectors()`, so the
    limit is forwarded as-is; the connections tool still filters locally.
    """
    workspace = _CloudWorkspace()
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    results = tool(
        ctx=object(),
        workspace_id="workspace-id",
        name_contains="target",
        limit=1,
        **extra_kwargs,
    )

    assert workspace.limits[limit_key] == forwarded_limit
    assert len(results) == 1
    assert results[0].name == "target"


def test_mcp_cloud_connections_apply_limit_after_status_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify connection list caps results after local status filtering."""
    workspace = _CloudWorkspace()
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    results = cloud_mcp.list_cloud_connections(
        ctx=cast(Context, object()),
        workspace_id="workspace-id",
        name_contains=None,
        limit=1,
        with_connection_status=False,
        failing_connections_only=True,
    )

    assert workspace.limits["connections"] is None
    assert len(results) == 1
    assert results[0].id == "connection-2"


@pytest.mark.parametrize(
    ("connector_id", "connector_type", "explicit_type"),
    [
        pytest.param("source-id", "source", ConnectorType.SOURCE, id="source-typed"),
        pytest.param("source-id", "source", None, id="source-untyped"),
        pytest.param(
            "destination-id",
            "destination",
            ConnectorType.DESTINATION,
            id="destination-typed",
        ),
        pytest.param("destination-id", "destination", None, id="destination-untyped"),
    ],
)
@pytest.mark.parametrize(
    ("check_result", "expected_success", "expected_message"),
    [
        pytest.param(
            CheckResult(success=True),
            True,
            None,
            id="success",
        ),
        pytest.param(
            CheckResult(success=False, error_message="Invalid credentials"),
            False,
            "Invalid credentials",
            id="error-message",
        ),
        pytest.param(
            CheckResult(success=False, internal_error="Check service unavailable"),
            False,
            "Check service unavailable",
            id="internal-error",
        ),
        pytest.param(
            CheckResult(success=False),
            False,
            "Connector check failed without a failure message.",
            id="missing-message",
        ),
    ],
)
def test_mcp_cloud_connector_checks_map_results(
    monkeypatch: pytest.MonkeyPatch,
    connector_id: str,
    connector_type: str,
    explicit_type: ConnectorType | None,
    check_result: CheckResult,
    expected_success: bool,
    expected_message: str | None,
) -> None:
    """Verify Cloud MCP connector checks map result fields and disable raising."""
    workspace = _ConnectorCheckWorkspace(check_result)
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    result = cloud_mcp.check_cloud_connector(
        ctx=cast(Context, object()),
        connector_id=connector_id,
        connector_type=explicit_type,
        workspace_id="workspace-id",
    )

    connector = (
        workspace.source if connector_type == "source" else workspace.destination
    )
    assert result.connector_id == connector_id
    assert result.connector_type == connector_type
    assert result.succeeded is expected_success
    assert result.message == expected_message
    assert connector.received_raise_on_error is False


def test_cancel_cloud_sync_returns_cancelled_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the Cloud MCP cancellation tool maps the cancelled sync result."""
    sync_result = _SyncResultLike(
        job_id=42,
        status=JobStatusEnum.CANCELLED,
        bytes_synced=123,
        records_synced=456,
        start_time=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        job_url="https://cloud.airbyte.com/jobs/42",
    )
    connection = _CancelableConnectionLike(sync_result=sync_result)
    workspace = _CancellationWorkspace(connection)
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    result = cast(
        SyncJobResult,
        cloud_mcp.cancel_cloud_sync(
            ctx=cast(Context, object()),
            connection_id="connection-id",
            job_id=42,
            workspace_id="workspace-id",
        ),
    )

    assert connection.received_job_id == 42
    assert result.job_id == 42
    assert result.status == "cancelled"
    assert result.bytes_synced == 123
    assert result.records_synced == 456
    assert result.start_time == "2026-01-02T03:04:05+00:00"
    assert result.job_url == "https://cloud.airbyte.com/jobs/42"


def test_cancel_cloud_sync_forwards_missing_job_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the Cloud MCP cancellation tool forwards a missing job ID."""
    connection = _CancelableConnectionLike(
        sync_result=_SyncResultLike(
            job_id=42,
            status=JobStatusEnum.CANCELLED,
            start_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
    )
    workspace = _CancellationWorkspace(connection)
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    cloud_mcp.cancel_cloud_sync(
        ctx=cast(Context, object()),
        connection_id="connection-id",
        job_id=None,
        workspace_id="workspace-id",
    )

    assert connection.received_job_id is None


def test_get_default_cloud_context_returns_context_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = CloudDefaultContextInfo(
        user_id="user-id",
        user_name="User",
        user_email="user@example.com",
        default_workspace_id="workspace-id",
        default_workspace_name="Workspace",
        default_workspace_verified=True,
        unvalidated_workspace_count=0,
        default_organization_id="organization-id",
        default_organization_name="Organization",
        configured_workspace_id=None,
        configured_organization_id=None,
        member_organizations=[
            CloudOrganizationInfo(
                organization_id="organization-id",
                organization_name="Organization",
            )
        ],
        member_workspaces=[
            CloudWorkspaceInfo(
                workspace_id="workspace-id",
                name="Workspace",
                organization_id="organization-id",
                organization_name="Organization",
                notifications={"webhook": {"enabled": True}},
            )
        ],
        member_organizations_truncated=True,
        member_workspaces_truncated=True,
        discovery_hints=[],
    )

    class ContextClient:
        def get_default_context_for_user(self) -> CloudDefaultContextInfo:
            return context

    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _: ContextClient())

    result = cloud_mcp.get_default_cloud_context(cast(Context, object()))

    assert result.user_id == "user-id"
    assert result.member_organizations[0].organization_id == "organization-id"
    assert result.member_workspaces[0].workspace_id == "workspace-id"
    assert result.member_workspaces[0].workspace_name == "Workspace"
    assert result.member_workspaces[0].organization_id == "organization-id"
    assert result.member_workspaces[0].organization_name == "Organization"
    assert "notifications" not in result.model_dump(mode="json")["member_workspaces"][0]
    assert result.message.startswith(
        "Resolved default workspace Workspace (workspace-id) "
        "in organization Organization (organization-id). "
    )
    assert "membership-based, not access-based" in result.message
    assert (
        "Only the first 1 organization memberships and 1 workspace memberships are shown"
        in result.message
    )


def test_get_default_cloud_context_flags_unverified_default_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = CloudDefaultContextInfo(
        user_id="user-id",
        user_name="User",
        user_email="user@example.com",
        default_workspace_id="deleted-workspace",
        default_workspace_name=None,
        default_workspace_verified=False,
        unvalidated_workspace_count=0,
        default_organization_id=None,
        default_organization_name=None,
        configured_workspace_id="deleted-workspace",
        configured_organization_id=None,
        member_organizations=[],
        member_workspaces=[],
        member_organizations_truncated=False,
        member_workspaces_truncated=False,
        discovery_hints=[],
    )

    class ContextClient:
        def get_default_context_for_user(self) -> CloudDefaultContextInfo:
            return context

    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _: ContextClient())

    result = cloud_mcp.get_default_cloud_context(cast(Context, object()))

    assert result.message.startswith(
        "Default workspace ID deleted-workspace could not be verified "
        "(it may have been deleted or is not accessible with these credentials). These"
    )


def test_describe_cloud_workspace_includes_parent_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organization = SimpleNamespace(
        organization_id="org-id", organization_name="Organization"
    )
    workspace = SimpleNamespace(
        workspace_id="workspace-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=None,
        workspace_url="https://cloud.airbyte.com/workspaces/workspace-id",
        get_organization=lambda **_: organization,
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda *_: workspace)
    monkeypatch.setattr(
        cloud_mcp.api_util,
        "get_workspace",
        lambda **_: SimpleNamespace(workspace_id="workspace-id", name="Workspace"),
    )
    result = cloud_mcp.describe_cloud_workspace(
        cast(Context, object()), workspace_id=None
    )
    assert result.organization_id == "org-id"
    assert result.organization_name == "Organization"


def test_describe_cloud_workspace_allows_missing_parent_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = SimpleNamespace(
        workspace_id="workspace-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=None,
        workspace_url=None,
        get_organization=lambda **_: None,
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda *_: workspace)
    monkeypatch.setattr(
        cloud_mcp.api_util,
        "get_workspace",
        lambda **_: SimpleNamespace(workspace_id="workspace-id", name="Workspace"),
    )
    result = cloud_mcp.describe_cloud_workspace(
        cast(Context, object()), workspace_id=None
    )
    assert result.organization_id is None
    assert result.organization_name is None


def test_get_cloud_organization_billing_status_returns_billing_info(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name="Organization",
        get_billing_status=lambda: SimpleNamespace(
            payment_status="okay",
            subscription_status="subscribed",
            is_account_locked=False,
        ),
    )
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_client",
        lambda _: SimpleNamespace(get_organization=lambda **_: organization),
    )
    result = cloud_mcp.get_cloud_organization_billing_status(
        cast(Context, object()), organization_id=None, organization_name=None
    )
    assert result.billing_info_available is True
    assert result.payment_status == "okay"
    assert result.subscription_status == "subscribed"
    assert result.is_account_locked is False


def test_get_cloud_organization_billing_status_handles_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def get_billing_status() -> None:
        raise cloud_mcp.AirbyteError(message="not allowed")

    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name="Organization",
        get_billing_status=get_billing_status,
    )
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_client",
        lambda _: SimpleNamespace(get_organization=lambda **_: organization),
    )
    result = cloud_mcp.get_cloud_organization_billing_status(
        cast(Context, object()), organization_id=None, organization_name=None
    )
    assert result.billing_info_available is False
    assert result.payment_status is None
    assert result.is_account_locked is False
    assert result.message == "Billing information could not be retrieved: not allowed"


def test_describe_cloud_organization_excludes_billing_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name="Organization",
        email="org@example.com",
        enabled_features=[],
    )
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_client",
        lambda _: SimpleNamespace(get_organization=lambda **_: organization),
    )
    result = cloud_mcp.describe_cloud_organization(
        cast(Context, object()), organization_id=None, organization_name=None
    )
    assert result.id == "org-id"
    assert not hasattr(result, "payment_status")
    assert not hasattr(result, "subscription_status")
    assert not hasattr(result, "is_account_locked")


def test_set_default_cloud_workspace_returns_update_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the tool maps the client result and states the durable impact."""
    update = cloud_mcp.CloudDefaultWorkspaceUpdateInfo(
        user_id="user-id",
        user_email="user@example.com",
        previous_default_workspace_id="old-workspace",
        default_workspace_id="workspace-id",
        default_workspace_name="Workspace",
        organization_id="organization-id",
        organization_name="Organization",
        membership_basis="workspace",
    )

    class ContextClient:
        def set_default_workspace_for_user(
            self,
            *,
            user_email: str,
            workspace_id: str,
        ) -> cloud_mcp.CloudDefaultWorkspaceUpdateInfo:
            assert user_email == "user@example.com"
            assert workspace_id == "workspace-id"
            return update

    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _: ContextClient())

    result = cloud_mcp.set_default_cloud_workspace(
        cast(Context, object()),
        user_email="user@example.com",
        workspace_id="workspace-id",
    )

    assert result.user_id == "user-id"
    assert result.default_workspace_id == "workspace-id"
    assert result.previous_default_workspace_id == "old-workspace"
    assert result.membership_basis == "workspace"
    assert result.message == (
        "Default workspace durably set to Workspace (workspace-id) for "
        "user@example.com. This applies to future MCP sessions and the Airbyte "
        "Cloud web app."
    )


@pytest.mark.parametrize(
    "tool,id_kwarg,getter,deleter",
    [
        pytest.param(
            functools.partial(
                cloud_mcp.permanently_delete_cloud_connector,
                connector_type=ConnectorType.SOURCE,
            ),
            "connector_id",
            "get_source",
            "permanently_delete_source",
            id="source",
        ),
        pytest.param(
            functools.partial(
                cloud_mcp.permanently_delete_cloud_connector,
                connector_type=ConnectorType.DESTINATION,
            ),
            "connector_id",
            "get_destination",
            "permanently_delete_destination",
            id="destination",
        ),
        pytest.param(
            cloud_mcp.permanently_delete_cloud_connection,
            "connection_id",
            "get_connection",
            "permanently_delete_connection",
            id="connection",
        ),
    ],
)
def test_permanently_delete_cloud_tools_pass_workspace_id(
    monkeypatch: pytest.MonkeyPatch,
    tool: Callable[..., str],
    id_kwarg: str,
    getter: str,
    deleter: str,
) -> None:
    """Verify Cloud MCP delete tools forward an explicit `workspace_id`."""
    seen_workspace_ids: list[str | None] = []
    resource = SimpleNamespace(
        name="delete-me-resource",
        connector_type=ConnectorType(getter.removeprefix("get_"))
        if getter != "get_connection"
        else None,
    )
    workspace = SimpleNamespace(**{
        getter: lambda **_: resource,
        deleter: lambda **_: None,
    })

    def fake_get_cloud_workspace(
        ctx: object,
        workspace_id: str | None = None,
    ) -> SimpleNamespace:
        seen_workspace_ids.append(workspace_id)
        return workspace

    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", fake_get_cloud_workspace)
    monkeypatch.setattr(cloud_mcp, "check_guid_created_in_session", lambda _: None)

    result = tool(
        cast(Context, object()),
        **{id_kwarg: "resource-id"},
        name="delete-me-resource",
        workspace_id="explicit-workspace-id",
    )

    assert seen_workspace_ids == ["explicit-workspace-id"]
    assert "resource-id" in result


@pytest.mark.parametrize(
    "explicit_type",
    [
        pytest.param(True, id="explicit-type"),
        pytest.param(False, id="inferred-type"),
    ],
)
@pytest.mark.parametrize(
    "connector_type,connector_name,registry_getter",
    [
        pytest.param(ConnectorType.SOURCE, "source-faker", "get_source", id="source"),
        pytest.param(
            ConnectorType.DESTINATION,
            "destination-duckdb",
            "get_destination",
            id="destination",
        ),
    ],
)
def test_deploy_connector_to_cloud_routes_by_type(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: ConnectorType,
    connector_name: str,
    registry_getter: str,
    explicit_type: bool,
) -> None:
    """Verify `deploy_connector_to_cloud` dispatches to the matching getter and deploy path."""
    connector_cls = Source if connector_type == ConnectorType.SOURCE else Destination
    connector = MagicMock(spec=connector_cls)
    connector.config_spec = {"type": "object"}
    getter_calls: list[str] = []
    deploy_calls: list[dict[str, object]] = []

    def fake_getter(name: str, *, no_executor: bool) -> MagicMock:
        assert no_executor is True
        getter_calls.append(name)
        return connector

    def fake_deploy(**kwargs: object) -> SimpleNamespace:
        deploy_calls.append(kwargs)
        return SimpleNamespace(
            connector_id="deployed-id",
            connector_url="https://cloud.airbyte.com/deployed-id",
        )

    workspace = SimpleNamespace(
        deploy_source=fake_deploy,
        deploy_destination=fake_deploy,
    )
    monkeypatch.setattr(cloud_mcp, registry_getter, fake_getter)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda *_: workspace)
    monkeypatch.setattr(
        cloud_mcp, "resolve_connector_config", lambda **_: {"resolved": True}
    )
    registered: list[str] = []
    monkeypatch.setattr(
        cloud_mcp, "register_guid_created_in_session", registered.append
    )

    result = cloud_mcp.deploy_connector_to_cloud(
        cast(Context, object()),
        name="My Connector",
        connector_name=connector_name,
        connector_type=connector_type if explicit_type else None,
        workspace_id=None,
        config={"key": "value"},
        config_secret_name=None,
        unique=True,
    )

    assert getter_calls == [connector_name]
    connector.set_config.assert_called_once_with({"resolved": True}, validate=True)
    assert len(deploy_calls) == 1
    assert deploy_calls[0]["name"] == "My Connector"
    assert deploy_calls[0]["unique"] is True
    assert deploy_calls[0][connector_type.value] is connector
    assert registered == ["deployed-id"]
    assert f"deployed {connector_type.value} 'My Connector'" in result
    assert "deployed-id" in result


def test_deploy_connector_to_cloud_rejects_unknown_prefix() -> None:
    """Verify a non-canonical name without an explicit type raises `PyAirbyteInputError`."""
    with pytest.raises(PyAirbyteInputError, match="Cannot infer connector type"):
        cloud_mcp.deploy_connector_to_cloud(
            cast(Context, object()),
            name="My Connector",
            connector_name="faker",
            connector_type=None,
            workspace_id=None,
            config=None,
            config_secret_name=None,
            unique=True,
        )


class _CombinedListingWorkspace:
    """Fake `CloudWorkspace` returning one source and one destination."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def list_connectors(
        self,
        *,
        connector_type: ConnectorType | None = None,
        feature_filter: ConnectorFeature | None = None,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> list[_CloudConnectorLike]:
        """Capture filters and mimic the core `list_connectors` filtering."""
        self.calls.append({
            "connector_type": connector_type,
            "feature_filter": feature_filter,
            "name_contains": name_contains,
            "limit": limit,
        })
        items = [
            _CloudConnectorLike(
                connector_id="source-1",
                connector_type=ConnectorType.SOURCE,
                name="GitHub",
                connector_url="https://cloud.airbyte.com/source-1",
                enabled_features=frozenset({
                    ConnectorFeature.DIRECT_ACCESS,
                    ConnectorFeature.DIRECT_API_QUERY,
                }),
            ),
            _CloudConnectorLike(
                connector_id="destination-1",
                connector_type=ConnectorType.DESTINATION,
                name="Snowflake",
                connector_url="https://cloud.airbyte.com/destination-1",
                enabled_features=frozenset({
                    ConnectorFeature.DIRECT_ACCESS,
                    ConnectorFeature.DIRECT_SQL_QUERY,
                }),
            ),
        ]
        if connector_type is not None:
            items = [item for item in items if item.connector_type == connector_type]
        if feature_filter is not None:
            items = [item for item in items if feature_filter in item.enabled_features]
        if name_contains:
            items = [item for item in items if name_contains in item.name]
        return items if limit is None else items[:limit]


def _patch_combined_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> _CombinedListingWorkspace:
    workspace = _CombinedListingWorkspace()
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda _ctx, _id: workspace)
    return workspace


def test_list_cloud_connectors_returns_both_kinds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The combined tool maps each connector's type and enabled features."""
    _patch_combined_listing(monkeypatch)

    results = cloud_mcp.list_cloud_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=None,
        feature_filter=None,
    )
    assert [(r.id, r.connector_type) for r in results] == [
        ("source-1", "source"),
        ("destination-1", "destination"),
    ]
    assert results[0].enabled_features == cloud_mcp.FEATURES_NOT_CHECKED
    assert results[1].enabled_features == cloud_mcp.FEATURES_NOT_CHECKED

    resolved = cloud_mcp.list_cloud_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=None,
        feature_filter=ConnectorFeature.DIRECT_ACCESS,
    )
    assert resolved[0].enabled_features == [
        ConnectorFeature.DIRECT_ACCESS,
        ConnectorFeature.DIRECT_API_QUERY,
    ]
    assert resolved[1].enabled_features == [
        ConnectorFeature.DIRECT_ACCESS,
        ConnectorFeature.DIRECT_SQL_QUERY,
    ]


def test_list_cloud_connectors_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`connector_type` and `feature_filter` narrow the combined listing."""
    workspace = _patch_combined_listing(monkeypatch)

    results = cloud_mcp.list_cloud_connectors(
        None,
        workspace_id=None,
        connector_type=ConnectorType.DESTINATION,
        name_contains=None,
        limit=None,
        feature_filter=ConnectorFeature.DIRECT_SQL_QUERY,
    )

    assert workspace.calls[0]["connector_type"] == ConnectorType.DESTINATION
    assert workspace.calls[0]["feature_filter"] is None
    assert [(r.id, r.connector_type) for r in results] == [
        ("destination-1", "destination")
    ]


def test_get_agent_skill_docs_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tool forwards `docs_skill_id`/`section` and renders the docs result."""
    guidance = DirectAccessGuidance(
        metadata=DirectAccessGuidanceIndexEntry(
            id="connector-source:source-1", title="GitHub", warnings=["partial"]
        ),
        outline=[
            DirectAccessGuidanceSection(id="setup", title="Setup"),
        ],
        content=[{"type": "paragraph", "text": "Hello"}],
        section_id="setup",
    )
    calls: list[dict[str, object]] = []
    workspace = SimpleNamespace(
        get_agent_skill_docs=lambda skill_id, *, connector_id=None, section=None: (
            calls.append({
                "skill_id": skill_id,
                "connector_id": connector_id,
                "section": section,
            }),
            guidance,
        )[1]
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda _ctx, _id: workspace)

    result = cloud_mcp.get_agent_skill_docs(
        None,
        docs_skill_id="connector-source:source-1",
        section="setup",
        workspace_id=None,
    )

    assert isinstance(result, cloud_mcp.AgentSkillDocsResult)
    assert calls == [
        {
            "skill_id": "connector-source:source-1",
            "connector_id": None,
            "section": "setup",
        }
    ]
    assert result.skill_id == "connector-source:source-1"
    assert result.title == "GitHub"
    assert result.section_id == "setup"
    assert result.warnings == ["partial"]
    assert "Hello" in result.content


def test_get_agent_skill_docs_tool_connector_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tool forwards `connector_id` to the workspace method."""
    guidance = DirectAccessGuidance(
        metadata=DirectAccessGuidanceIndexEntry(id="connector-source:source-1"),
        content=[],
    )
    calls: list[dict[str, object]] = []
    workspace = SimpleNamespace(
        get_agent_skill_docs=lambda skill_id, *, connector_id=None, section=None: (
            calls.append({
                "skill_id": skill_id,
                "connector_id": connector_id,
                "section": section,
            }),
            guidance,
        )[1]
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda _ctx, _id: workspace)

    result = cloud_mcp.get_agent_skill_docs(
        None,
        connector_id="source-1",
        workspace_id=None,
    )

    assert isinstance(result, cloud_mcp.AgentSkillDocsResult)
    assert calls == [{"skill_id": None, "connector_id": "source-1", "section": None}]
    assert result.skill_id == "connector-source:source-1"


class _RecordingExecuteConnector:
    """Fake connector recording `execute_api_*`/`execute_sql_query` calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.result = ExternalApiExecuteResult(status="success", result={"ok": True})

    def execute_api_query(
        self, entity_type: str, action: object, api_args: object, **kwargs: object
    ) -> object:
        self.calls.append((
            "query",
            {
                "entity_type": entity_type,
                "action": action,
                "api_args": api_args,
                **kwargs,
            },
        ))
        return self.result

    def execute_api_action(
        self, entity_type: str, action: object, api_args: object, **kwargs: object
    ) -> object:
        self.calls.append((
            "action",
            {
                "entity_type": entity_type,
                "action": action,
                "api_args": api_args,
                **kwargs,
            },
        ))
        return self.result

    def execute_sql_query(self, sql: str, **kwargs: object) -> object:
        self.calls.append(("sql", {"sql": sql, **kwargs}))
        return self.result


def _execute_workspace(
    monkeypatch: pytest.MonkeyPatch, connector: _RecordingExecuteConnector
) -> SimpleNamespace:
    """Patch `_get_cloud_workspace` to return a workspace serving `connector`."""
    get_connector = MagicMock(return_value=connector)
    workspace = SimpleNamespace(get_connector=get_connector)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda _ctx, _id: workspace)
    return workspace


def test_execute_external_api_query_forwards_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tool forwards every kwarg, parsing JSON `api_args` and CSV field lists."""
    connector = _RecordingExecuteConnector()
    workspace = _execute_workspace(monkeypatch, connector)

    result = cloud_mcp.execute_external_api_query(
        None,
        connector_id="source-1",
        entity_type="issues",
        action="list",
        api_args='{"repository": "airbytehq/PyAirbyte"}',
        select_fields="id,title",
        exclude_fields=["body"],
        page_size=5,
        cursor="cursor-1",
        skip_truncation=False,
        intent="test",
        workspace_id=None,
    )

    workspace.get_connector.assert_called_once_with("source-1")
    (kind, kwargs) = connector.calls[0]
    assert kind == "query"
    assert kwargs == {
        "entity_type": "issues",
        "action": "list",
        "api_args": {"repository": "airbytehq/PyAirbyte"},
        "select_fields": ["id", "title"],
        "exclude_fields": ["body"],
        "page_size": 5,
        "cursor": "cursor-1",
        "skip_truncation": False,
        "intent": "test",
    }
    assert result.status == "success"


def test_execute_external_api_query_rejects_non_object_api_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-object `api_args` string raises `PyAirbyteInputError`."""
    connector = _RecordingExecuteConnector()
    _execute_workspace(monkeypatch, connector)

    with pytest.raises(PyAirbyteInputError, match="JSON object"):
        cloud_mcp.execute_external_api_query(
            None,
            connector_id="source-1",
            entity_type="issues",
            api_args='["not", "an", "object"]',
            workspace_id=None,
        )
    assert connector.calls == []


def test_execute_external_api_action_uses_write_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The write tool calls `execute_api_action`, not `execute_api_query`."""
    connector = _RecordingExecuteConnector()
    _execute_workspace(monkeypatch, connector)

    cloud_mcp.execute_external_api_action(
        None,
        connector_id="source-1",
        entity_type="issues",
        action=ExternalApiWriteAction.CREATE,
        api_args={"title": "Bug"},
        workspace_id=None,
    )

    (kind, kwargs) = connector.calls[0]
    assert kind == "action"
    assert kwargs["entity_type"] == "issues"
    assert kwargs["action"] is ExternalApiWriteAction.CREATE
    assert kwargs["api_args"] == {"title": "Bug"}


def test_execute_external_sql_query_forwards_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SQL tool forwards `sql`/`sql_dialect`/`page_size`/`cursor`."""
    connector = _RecordingExecuteConnector()
    _execute_workspace(monkeypatch, connector)

    cloud_mcp.execute_external_sql_query(
        None,
        connector_id="destination-1",
        sql="SHOW TABLES",
        sql_dialect="snowflake",
        page_size=10,
        cursor="cursor-2",
        workspace_id=None,
    )
    cloud_mcp.execute_external_sql_query(
        None,
        connector_id="destination-1",
        sql="SELECT * FROM users LIMIT 1",
        dry_run=True,
        workspace_id=None,
    )

    (kind, kwargs) = connector.calls[0]
    assert kind == "sql"
    assert kwargs == {
        "sql": "SHOW TABLES",
        "sql_dialect": "snowflake",
        "page_size": 10,
        "cursor": "cursor-2",
        "dry_run": False,
    }
    (kind, kwargs) = connector.calls[1]
    assert kind == "sql"
    assert kwargs["dry_run"] is True


def _describe_details() -> CloudConnectorDetailsResult:
    """Return a minimal `CloudConnectorDetailsResult` for describe-tool forwarding tests."""
    return CloudConnectorDetailsResult(
        connector_id="connector-1",
        connector_type="source",
        connector_name="GitHub",
        connector_url="",
        connector_definition_id="definition-id",
    )


def test_describe_cloud_connector_forwards_id_and_toggles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`describe_cloud_connector` forwards the ID and all `with_*` toggles."""
    details = _describe_details()
    describe = MagicMock(return_value=details)
    monkeypatch.setattr(cloud_mcp, "_describe_cloud_connector", describe)
    connector = object()
    get_connector = MagicMock(return_value=connector)
    workspace = SimpleNamespace(get_connector=get_connector)
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda *args, **kwargs: workspace
    )

    result = cloud_mcp.describe_cloud_connector(
        ctx=cast(Context, object()),
        connector_id="connector-1",
        workspace_id="workspace-1",
        with_config=True,
        with_replication_details=True,
        with_direct_access_guidance=True,
        with_data_replication_docs=True,
    )

    get_connector.assert_called_once_with(connector_id="connector-1")
    describe.assert_called_once_with(
        connector,
        with_config=True,
        with_replication_details=True,
        with_direct_access_guidance=True,
        with_data_replication_docs=True,
    )
    assert result is details


@pytest.mark.parametrize(
    ("connector_type", "getter", "id_kwarg"),
    [
        pytest.param(ConnectorType.SOURCE, "get_source", "source_id", id="source"),
        pytest.param(
            ConnectorType.DESTINATION,
            "get_destination",
            "destination_id",
            id="destination",
        ),
    ],
)
def test_describe_cloud_connector_explicit_type_skips_lookup(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: ConnectorType,
    getter: str,
    id_kwarg: str,
) -> None:
    """An explicit `connector_type` routes straight to the typed workspace getter."""
    details = _describe_details()
    describe = MagicMock(return_value=details)
    monkeypatch.setattr(cloud_mcp, "_describe_cloud_connector", describe)
    connector = object()
    typed_getter = MagicMock(return_value=connector)
    get_connector = MagicMock()
    workspace = SimpleNamespace(**{
        getter: typed_getter,
        "get_connector": get_connector,
    })
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda *args, **kwargs: workspace
    )

    result = cloud_mcp.describe_cloud_connector(
        ctx=cast(Context, object()),
        connector_id="connector-1",
        connector_type=connector_type,
        workspace_id="workspace-1",
        with_config=True,
        with_replication_details=True,
        with_direct_access_guidance=True,
        with_data_replication_docs=True,
    )

    typed_getter.assert_called_once_with(**{id_kwarg: "connector-1"})
    get_connector.assert_not_called()
    describe.assert_called_once_with(
        connector,
        with_config=True,
        with_replication_details=True,
        with_direct_access_guidance=True,
        with_data_replication_docs=True,
    )
    assert result is details


@dataclass
class _DescribedConnector:
    """Subset of `CloudConnector` exercised by `_describe_cloud_connector` tests."""

    connector_id: str = "connector-1"
    connector_type: ConnectorType = ConnectorType.SOURCE
    name: str | None = "GitHub"
    connector_url: str = ""
    definition_id: str = "definition-id"
    enabled_features: frozenset[ConnectorFeature] = frozenset()

    def __post_init__(self) -> None:
        self._integration_name: str | AirbyteError = "GitHub"
        self.workspace = SimpleNamespace(_has_context_layer_api=lambda: False)
        self.inspect_result: object | None = None
        self.inspect_error: AirbyteError | None = None
        self.config: dict[str, object] | AirbyteError = {"key": "value"}
        self.guidance: DirectAccessGuidance | Exception | None = None
        self.replication_docs: list[object] | Exception = []

    @property
    def integration_name(self) -> str:
        """The integration title, raising the stored error when set."""
        if isinstance(self._integration_name, AirbyteError):
            raise self._integration_name
        return self._integration_name

    def get_enabled_features(
        self, *, warnings: list[str]
    ) -> frozenset[ConnectorFeature]:
        if self.workspace._has_context_layer_api():
            details = self._context_layer_inspect(warnings=warnings)
            if details is not None:
                warnings.extend(details.warnings)
        return self.enabled_features

    def is_feature_enabled(self, feature: ConnectorFeature) -> bool:
        return feature in self.enabled_features

    def _context_layer_inspect(
        self, *, warnings: list[str], **_kwargs: object
    ) -> object:
        if self.inspect_error is not None:
            warnings.append(f"Connector inspect failed: {self.inspect_error}")
            return None
        return self.inspect_result

    def as_cloud_source(self) -> "_DescribedConnector":
        return self

    def as_cloud_destination(self) -> "_DescribedConnector":
        return self

    @property
    def configuration(self) -> dict[str, object]:
        if isinstance(self.config, AirbyteError):
            raise self.config
        return self.config

    def get_direct_access_guidance(self, **_kwargs: object) -> DirectAccessGuidance:
        if isinstance(self.guidance, Exception):
            raise self.guidance
        assert self.guidance is not None
        return self.guidance

    def get_data_replication_docs(self, **_kwargs: object) -> list[object]:
        if isinstance(self.replication_docs, AirbyteError):
            raise self.replication_docs
        return self.replication_docs


def _describe(
    connector: _DescribedConnector, **overrides: object
) -> CloudConnectorDetailsResult:
    kwargs = {
        "with_config": False,
        "with_replication_details": False,
        "with_direct_access_guidance": False,
        "with_data_replication_docs": False,
    }
    kwargs.update(overrides)
    return cloud_mcp._describe_cloud_connector(connector, **kwargs)  # noqa: SLF001


def test_describe_helper_reports_identity_and_enabled_features() -> None:
    """Identity fields populate always; `enabled_features` is sorted by value."""
    connector = _DescribedConnector(
        enabled_features=frozenset({
            ConnectorFeature.DIRECT_API_QUERY,
            ConnectorFeature.DIRECT_ACCESS,
        })
    )

    result = _describe(connector)

    assert result.connector_id == "connector-1"
    assert result.connector_type == "source"
    assert result.connector_name == "GitHub"
    assert result.integration_name == "GitHub"
    assert result.enabled_features == [
        ConnectorFeature.DIRECT_ACCESS,
        ConnectorFeature.DIRECT_API_QUERY,
    ]
    assert result.warnings == []


def test_describe_helper_destination_with_no_features() -> None:
    """A connector with no enabled features reports an empty list."""
    connector = _DescribedConnector(connector_type=ConnectorType.DESTINATION)

    result = _describe(connector)

    assert result.connector_type == "destination"
    assert result.enabled_features == []


def test_describe_helper_integration_name_failure_warns() -> None:
    """A definition lookup failure warns and leaves `integration_name` unset."""
    connector = _DescribedConnector()
    connector._integration_name = AirbyteError(message="lookup boom")  # noqa: SLF001

    result = _describe(connector)

    assert result.integration_name is None
    assert any("Integration name lookup failed" in w for w in result.warnings)


def test_describe_helper_collects_inspect_warnings() -> None:
    """Context Layer `inspect` failures and warnings land in `warnings`."""
    connector = _DescribedConnector(
        enabled_features=frozenset({ConnectorFeature.DIRECT_ACCESS})
    )
    connector.workspace = SimpleNamespace(_has_context_layer_api=lambda: True)
    connector.inspect_error = AirbyteError(message="inspect boom")

    result = _describe(connector)

    assert any("Connector inspect failed" in w for w in result.warnings)


def test_describe_helper_extends_context_layer_warnings() -> None:
    """Warnings reported by a successful `inspect` are surfaced too."""
    connector = _DescribedConnector(
        enabled_features=frozenset({ConnectorFeature.DIRECT_ACCESS})
    )
    connector.workspace = SimpleNamespace(_has_context_layer_api=lambda: True)
    connector.inspect_result = SimpleNamespace(warnings=["Partial runtime metadata."])

    result = _describe(connector)

    assert "Partial runtime metadata." in result.warnings


def test_describe_helper_with_config_reads_destination_config() -> None:
    """`with_config` on a destination returns the redacted configuration."""
    connector = _DescribedConnector(connector_type=ConnectorType.DESTINATION)
    connector.config = {"database": "analytics"}

    result = _describe(connector, with_config=True)

    assert result.config == {"database": "analytics"}


def test_describe_helper_with_config_source_has_no_config() -> None:
    """Sources never expose configuration."""
    connector = _DescribedConnector()

    result = _describe(connector, with_config=True)

    assert result.config is None


def test_describe_helper_with_config_failure_warns() -> None:
    """A config fetch failure under `with_config` warns instead of raising."""
    connector = _DescribedConnector(connector_type=ConnectorType.DESTINATION)
    connector.config = AirbyteError(message="config boom")

    result = _describe(connector, with_config=True)

    assert result.config is None
    assert any("Connector configuration lookup failed" in w for w in result.warnings)


def test_describe_helper_with_replication_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`with_replication_details` returns the connections touching the connector."""
    info = CloudConnectorConnectionInfo(
        connection_id="conn-1",
        name="sync",
        source_id="source-1",
        source_name="GitHub",
        destination_id="dest-1",
        destination_name="Warehouse",
        schedule="manual",
        stream_names=["issues"],
        table_prefix="raw_",
        destination_database="analytics",
        destination_schema="raw",
    )
    monkeypatch.setattr(
        cloud_mcp.connector_docs,
        "build_connection_details",
        lambda _connector: [info],
    )
    connector = _DescribedConnector(connector_type=ConnectorType.DESTINATION)

    result = _describe(connector, with_replication_details=True)

    assert result.replication_details == [info]


def test_describe_helper_replication_details_failure_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connection listing failure warns instead of raising."""

    def fail(_connector: object) -> list[object]:
        raise AirbyteError(message="listing boom")

    monkeypatch.setattr(cloud_mcp.connector_docs, "build_connection_details", fail)
    connector = _DescribedConnector(connector_type=ConnectorType.DESTINATION)

    result = _describe(connector, with_replication_details=True)

    assert result.replication_details is None
    assert any("Connection listing failed" in w for w in result.warnings)


def test_describe_helper_with_direct_access_guidance() -> None:
    """`with_direct_access_guidance` renders the connector's docs as Markdown."""
    connector = _DescribedConnector()
    connector.guidance = DirectAccessGuidance(
        metadata=DirectAccessGuidanceIndexEntry(id="connector:github", title="GitHub"),
        content=[{"type": "paragraph", "text": "Use it."}],
    )

    result = _describe(connector, with_direct_access_guidance=True)

    assert result.direct_access_guidance is not None
    assert result.direct_access_guidance.skill_id == "connector:github"
    assert result.direct_access_guidance.title == "GitHub"
    assert "Use it." in result.direct_access_guidance.content


def test_describe_helper_fallback_guidance_names_no_tools() -> None:
    """The structure-only fallback docs never mention Airbyte SQL tool calls."""
    snowflake_definition_id = next(
        definition_id
        for definition_id, dialect in _SQL_PASSTHROUGH_DESTINATION_DIALECTS.items()
        if dialect == "snowflake"
    )
    destination = SimpleNamespace(
        connector_id="dest-1",
        name="Warehouse",
        definition_id=snowflake_definition_id,
        configuration={"database": "DATABASE", "schema": "SCHEMA"},
        workspace=SimpleNamespace(list_connections=lambda: []),
    )
    connector = _DescribedConnector(connector_type=ConnectorType.DESTINATION)
    connector.guidance = connector_docs.build_direct_access_sql_guidance(
        destination,
        sql_passthrough_notice=connector_docs.SQL_PASSTHROUGH_NOT_ENABLED_NOTICE,
    )

    result = _describe(connector, with_direct_access_guidance=True)

    assert result.direct_access_guidance is not None
    assert "execute_external_sql_query" not in result.direct_access_guidance.content
    assert "SHOW TABLES" not in result.direct_access_guidance.content
    assert "sql_select" not in result.direct_access_guidance.content


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(PyAirbyteInputError(message="bad docs"), id="input_error"),
        pytest.param(requests.Timeout("docs timed out"), id="transport_error"),
    ],
)
def test_describe_helper_direct_access_guidance_failure_warns(error: Exception) -> None:
    """Docs failures under `with_direct_access_guidance` append a warning."""
    connector = _DescribedConnector()
    connector.guidance = error

    result = _describe(connector, with_direct_access_guidance=True)

    assert result.direct_access_guidance is None
    assert any("Direct access docs are unavailable" in w for w in result.warnings)


def test_describe_helper_data_replication_docs_failure_warns() -> None:
    """A registry miss under `with_data_replication_docs` appends a warning."""
    connector = _DescribedConnector()
    connector.replication_docs = AirbyteError(message="unregistered")

    result = _describe(connector, with_data_replication_docs=True)

    assert result.data_replication_docs is None
    assert any("Data replication docs are unavailable" in w for w in result.warnings)


class _ProbingConnector:
    """Connector double whose `enabled_features` lookup can fail, with call counting."""

    def __init__(
        self,
        connector_id: str,
        probe_calls: list[str],
        *,
        features: frozenset[ConnectorFeature] = frozenset(),
        error: Exception | None = None,
    ) -> None:
        self.connector_id = connector_id
        self.connector_type = ConnectorType.SOURCE
        self.name = connector_id
        self.connector_url = f"https://cloud.airbyte.com/{connector_id}"
        self._features = features
        self._error = error
        self._probe_calls = probe_calls

    @property
    def enabled_features(self) -> frozenset[ConnectorFeature]:
        self._probe_calls.append(self.connector_id)
        if self._error is not None:
            raise self._error
        return self._features


class _ProbeListingWorkspace:
    """Fake `CloudWorkspace` returning a fixed connector list for feature-filter tests."""

    def __init__(self, connectors: list[_ProbingConnector]) -> None:
        self._connectors = connectors

    def list_connectors(
        self,
        *,
        connector_type: ConnectorType | None = None,
        feature_filter: ConnectorFeature | None = None,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> list[_ProbingConnector]:
        assert feature_filter is None
        items = self._connectors
        if connector_type is not None:
            items = [item for item in items if item.connector_type == connector_type]
        return items if limit is None else items[:limit]


def _patch_probe_listing(
    monkeypatch: pytest.MonkeyPatch, connectors: list[_ProbingConnector]
) -> None:
    workspace = _ProbeListingWorkspace(connectors)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda _ctx, _id: workspace)


def test_list_cloud_connectors_feature_filter_probe_failure_marks_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-'not enabled' probe failure marks remaining connectors `"unknown"`."""
    probe_calls: list[str] = []
    _patch_probe_listing(
        monkeypatch,
        [
            _ProbingConnector(
                "source-1",
                probe_calls,
                features=frozenset({
                    ConnectorFeature.DIRECT_ACCESS,
                    ConnectorFeature.DIRECT_API_QUERY,
                }),
            ),
            _ProbingConnector(
                "source-2",
                probe_calls,
                error=AirbyteCloudApiError(status_code=502),
            ),
            _ProbingConnector(
                "source-3",
                probe_calls,
                features=frozenset({
                    ConnectorFeature.DIRECT_ACCESS,
                    ConnectorFeature.DIRECT_API_QUERY,
                }),
            ),
        ],
    )

    results = cloud_mcp.list_cloud_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=None,
        feature_filter=ConnectorFeature.DIRECT_ACCESS,
    )

    assert len(results) == 3
    assert results[0].enabled_features == [
        ConnectorFeature.DIRECT_ACCESS,
        ConnectorFeature.DIRECT_API_QUERY,
    ]
    for result in results[1:]:
        assert result.enabled_features == cloud_mcp.FEATURES_UNKNOWN
        assert any("enabled features are unknown" in w for w in result.warnings)
    assert probe_calls == ["source-1", "source-2"]


def test_list_cloud_connectors_feature_filter_connector_failure_keeps_probing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connector-specific probe failure marks only that connector `"unknown"`."""
    probe_calls: list[str] = []
    _patch_probe_listing(
        monkeypatch,
        [
            _ProbingConnector(
                "source-1",
                probe_calls,
                error=AirbyteCloudApiError(status_code=422),
            ),
            _ProbingConnector(
                "source-2",
                probe_calls,
                features=frozenset(),
            ),
            _ProbingConnector(
                "source-3",
                probe_calls,
                features=frozenset({ConnectorFeature.DIRECT_ACCESS}),
            ),
        ],
    )

    results = cloud_mcp.list_cloud_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=None,
        feature_filter=ConnectorFeature.DIRECT_ACCESS,
    )

    assert [result.id for result in results] == ["source-1", "source-3"]
    assert results[0].enabled_features == cloud_mcp.FEATURES_UNKNOWN
    assert any("enabled features are unknown" in w for w in results[0].warnings)
    assert results[1].enabled_features == [ConnectorFeature.DIRECT_ACCESS]
    assert probe_calls == ["source-1", "source-2", "source-3"]


def test_list_cloud_connectors_limit_must_be_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`limit=0` raises `PyAirbyteInputError` before listing connectors."""
    with pytest.raises(PyAirbyteInputError, match="`limit` must be greater than 0"):
        cloud_mcp.list_cloud_connectors(
            None,
            workspace_id=None,
            name_contains=None,
            limit=0,
            feature_filter=ConnectorFeature.DIRECT_ACCESS,
        )


def test_list_cloud_connectors_feature_filter_not_enabled_still_excluded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 403/404 probe failure still means 'not enabled': excluded, not `"unknown"`."""
    probe_calls: list[str] = []
    _patch_probe_listing(
        monkeypatch,
        [
            _ProbingConnector(
                "source-1",
                probe_calls,
                features=frozenset(),
            ),
            _ProbingConnector(
                "source-2",
                probe_calls,
                features=frozenset({ConnectorFeature.DIRECT_ACCESS}),
            ),
        ],
    )

    results = cloud_mcp.list_cloud_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=None,
        feature_filter=ConnectorFeature.DIRECT_ACCESS,
    )

    assert [result.id for result in results] == ["source-2"]
    assert probe_calls == ["source-1", "source-2"]


def test_list_cloud_connectors_feature_filter_limit_counts_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`"unknown"` results count toward `limit`."""
    probe_calls: list[str] = []
    _patch_probe_listing(
        monkeypatch,
        [
            _ProbingConnector(
                "source-1",
                probe_calls,
                error=AirbyteCloudApiError(status_code=502),
            ),
            _ProbingConnector("source-2", probe_calls),
        ],
    )

    results = cloud_mcp.list_cloud_connectors(
        None,
        workspace_id=None,
        name_contains=None,
        limit=1,
        feature_filter=ConnectorFeature.DIRECT_ACCESS,
    )

    assert len(results) == 1
    assert results[0].enabled_features == cloud_mcp.FEATURES_UNKNOWN
    assert probe_calls == ["source-1"]


def test_describe_cloud_connector_probe_failure_marks_unknown() -> None:
    """A feature-probe failure marks `enabled_features` `"unknown"` with a warning."""

    class _FailingFeaturesConnector(_DescribedConnector):
        @property
        def enabled_features(self) -> frozenset[ConnectorFeature]:
            raise AirbyteCloudApiError(status_code=504)

        @enabled_features.setter
        def enabled_features(self, value: frozenset[ConnectorFeature]) -> None:
            pass

    result = _describe(_FailingFeaturesConnector())

    assert result.enabled_features == cloud_mcp.FEATURES_UNKNOWN
    assert any("enabled features are unknown" in w for w in result.warnings)


@pytest.mark.parametrize("action", list(ExternalApiWriteAction))
def test_cloud_write_tool_reports_unsupported_without_network(
    monkeypatch: pytest.MonkeyPatch, action: ExternalApiWriteAction
) -> None:
    workspace = CloudWorkspace(workspace_id="workspace-1", bearer_token="token")
    source = CloudSource(workspace=workspace, connector_id="source-1")
    monkeypatch.setattr(workspace, "get_connector", lambda _: source)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda *_: workspace)
    request = MagicMock(side_effect=AssertionError("unsupported write sent to server"))
    monkeypatch.setattr(requests, "request", request)

    with pytest.raises(PyAirbyteInputError) as raised:
        cloud_mcp.execute_external_api_action(
            None,
            connector_id="source-1",
            entity_type="issues",
            action=action,
            api_args={"title": "example"},
            workspace_id="workspace-1",
        )

    request.assert_not_called()
    assert isinstance(raised.value, MCP_TOOL_USER_FACING_ERRORS)
    assert format_user_facing_error(raised.value) == (
        "Cloud connector write actions are not supported yet. "
        "Use execute_api_query for read actions (list, get, search)."
    )
