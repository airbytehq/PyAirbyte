# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud MCP tools."""

from __future__ import annotations

import asyncio
import dataclasses
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
    ExternalSearchResult,
    ExternalSearchStatusResult,
    ExternalSearchType,
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
)
from airbyte._util import api_util
from airbyte.cloud import CloudConnection, CloudWorkspace
from airbyte.cloud.organizations import CloudOrganization
from airbyte.cloud.connectors import CheckResult, ConnectorFeature, ConnectorType
from airbyte.cloud.sync_results import (
    SyncAttempt,
    SyncAttemptFailure,
    SyncJobSnapshot,
    SyncResult,
)
from airbyte.cloud.models import (
    CloudDefaultContextInfo,
    CloudOrganizationInfo,
    CloudWorkspaceInfo,
    JobStatusEnum,
    JobTypeEnum,
)
from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp._arg_resolvers import resolve_list_of_dicts
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
from airbyte.mcp.server import MCP_SERVER_INSTRUCTIONS
from fastmcp import Context
from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701


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

    def execute_search_query(self, prompt: str, **kwargs: object) -> object:
        self.calls.append(("search", {"prompt": prompt, **kwargs}))
        return ExternalSearchResult(hits=[], metadata=[])

    def get_search_status(self) -> object:
        self.calls.append(("search_status", {}))
        return ExternalSearchStatusResult.model_validate({
            "sources": [
                {
                    "source_id": "source-1",
                    "streams": [{"name": "issues"}, {"name": "users"}],
                },
                {
                    "source_id": "source-2",
                    "streams": [{"name": "deals", "namespace": "crm"}],
                },
            ]
        })


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

    cloud_mcp._execute_external_api_action(
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


def test_execute_external_api_action_is_not_advertised() -> None:
    """The write tool stays hidden until the backend supports write actions."""
    from airbyte.mcp import server

    names = {tool.name for tool in asyncio.run(server.app.list_tools())}
    assert "execute_external_api_query" in names
    assert "execute_external_api_action" not in names


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


def test_execute_external_search_query_forwards_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The search tool forwards every arg, parsing a JSON `streams` string."""
    connector = _RecordingExecuteConnector()
    workspace = _execute_workspace(monkeypatch, connector)

    result = cloud_mcp.execute_external_search_query(
        None,
        connector_id="source-1",
        prompt="refund requests",
        search_type=ExternalSearchType.SEMANTIC,
        limit=5,
        streams='[{"stream_name": "issues", "fields": ["title"]}]',
        lookback_seconds=3600,
        max_context_chars=200,
        min_similarity=0.3,
        max_similarity_diff=0.1,
        destination_id="destination-1",
        workspace_id=None,
    )
    cloud_mcp.execute_external_search_query(
        None,
        connector_id="source-1",
        prompt="refund requests",
        streams=[{"stream_name": "users"}],
        workspace_id=None,
    )

    workspace.get_connector.assert_called_with(connector_id="source-1")
    (kind, kwargs) = connector.calls[0]
    assert kind == "search"
    assert kwargs == {
        "prompt": "refund requests",
        "search_type": ExternalSearchType.SEMANTIC,
        "limit": 5,
        "streams": [{"stream_name": "issues", "fields": ["title"]}],
        "lookback_seconds": 3600,
        "max_context_chars": 200,
        "min_similarity": 0.3,
        "max_similarity_diff": 0.1,
        "destination_id": "destination-1",
    }
    (kind, kwargs) = connector.calls[1]
    assert kind == "search"
    assert kwargs["search_type"] is ExternalSearchType.HYBRID
    assert kwargs["streams"] == [{"stream_name": "users"}]
    assert kwargs["limit"] is None
    assert isinstance(result, ExternalSearchResult)


@pytest.mark.parametrize(
    "streams",
    [
        pytest.param("not json", id="invalid_json"),
        pytest.param('{"stream_name": "issues"}', id="object_not_array"),
        pytest.param('["issues"]', id="array_of_strings"),
    ],
)
def test_execute_external_search_query_rejects_bad_streams(
    monkeypatch: pytest.MonkeyPatch,
    streams: str,
) -> None:
    """A `streams` string that is not a JSON array of objects raises `PyAirbyteInputError`."""
    connector = _RecordingExecuteConnector()
    _execute_workspace(monkeypatch, connector)

    with pytest.raises(PyAirbyteInputError, match="`streams`"):
        cloud_mcp.execute_external_search_query(
            None,
            connector_id="source-1",
            prompt="refund requests",
            streams=streams,
            workspace_id=None,
        )
    assert connector.calls == []


@pytest.mark.parametrize(
    ("stream_name", "expected"),
    [
        pytest.param(
            None,
            [("source-1", ["issues", "users"]), ("source-2", ["deals"])],
            id="no_filter",
        ),
        pytest.param("users", [("source-1", ["users"])], id="filters_streams"),
        pytest.param("missing", [], id="no_match"),
    ],
)
def test_get_cloud_search_status_filters_by_stream_name(
    monkeypatch: pytest.MonkeyPatch,
    stream_name: str | None,
    expected: list[tuple[str, list[str]]],
) -> None:
    """`stream_name` filters streams client-side and drops sources left without any."""
    connector = _RecordingExecuteConnector()
    workspace = _execute_workspace(monkeypatch, connector)

    result = cloud_mcp.get_cloud_search_status(
        None,
        connector_id="destination-1",
        stream_name=stream_name,
        workspace_id=None,
    )

    workspace.get_connector.assert_called_once_with(connector_id="destination-1")
    assert connector.calls == [("search_status", {})]
    assert [
        (source.source_id, [stream.name for stream in source.streams])
        for source in result.sources
    ] == expected


@pytest.mark.parametrize(
    ("stream_name", "namespace", "expected", "expected_warning"),
    [
        pytest.param(None, "crm", [("source-2", ["deals"])], None, id="namespace"),
        pytest.param(
            "deals", "crm", [("source-2", ["deals"])], None, id="name_and_namespace"
        ),
        pytest.param(
            "issues",
            "crm",
            [],
            "No indexed stream matches stream_name='issues', namespace='crm'. "
            "Indexed streams: crm.deals, issues, users.",
            id="namespace_miss",
        ),
        pytest.param(
            "missing",
            None,
            [],
            "No indexed stream matches stream_name='missing'. "
            "Indexed streams: crm.deals, issues, users.",
            id="stream_miss",
        ),
    ],
)
def test_get_cloud_search_status_namespace_filter_and_miss_warning(
    monkeypatch: pytest.MonkeyPatch,
    stream_name: str | None,
    namespace: str | None,
    expected: list[tuple[str, list[str]]],
    expected_warning: str | None,
) -> None:
    """A filter that matches nothing returns a warning listing the indexed streams."""
    connector = _RecordingExecuteConnector()
    _execute_workspace(monkeypatch, connector)

    result = cloud_mcp.get_cloud_search_status(
        None,
        connector_id="destination-1",
        stream_name=stream_name,
        namespace=namespace,
        workspace_id=None,
    )

    assert [
        (source.source_id, [stream.name for stream in source.streams])
        for source in result.sources
    ] == expected
    assert result.warnings == ([expected_warning] if expected_warning else [])


def test_get_cloud_search_status_miss_warning_caps_stream_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _RecordingExecuteConnector()
    status = ExternalSearchStatusResult.model_validate({
        "sources": [
            {
                "source_id": "source-1",
                "streams": [{"name": f"stream_{index:02d}"} for index in range(25)],
            }
        ]
    })
    monkeypatch.setattr(connector, "get_search_status", lambda: status)
    _execute_workspace(monkeypatch, connector)

    result = cloud_mcp.get_cloud_search_status(
        None,
        connector_id="source-1",
        stream_name="missing",
        workspace_id=None,
    )

    (warning,) = result.warnings
    assert "stream_19" in warning
    assert "stream_20" not in warning
    assert warning.endswith("(and 5 more).")


@pytest.mark.parametrize(
    ("connector_type", "getter", "getter_kwarg"),
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
def test_search_tools_use_connector_type_without_probe(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: ConnectorType,
    getter: str,
    getter_kwarg: str,
) -> None:
    """An explicit `connector_type` builds a typed connector, skipping the kind probe."""
    connector = _RecordingExecuteConnector()
    typed_getter = MagicMock(return_value=connector)
    workspace = SimpleNamespace(
        get_connector=MagicMock(side_effect=AssertionError("unexpected kind probe")),
        **{getter: typed_getter},
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lambda _ctx, _id: workspace)

    cloud_mcp.execute_external_search_query(
        None,
        connector_id="connector-1",
        connector_type=connector_type,
        prompt="refund requests",
        workspace_id=None,
    )
    cloud_mcp.get_cloud_search_status(
        None,
        connector_id="connector-1",
        connector_type=connector_type,
        workspace_id=None,
    )

    assert typed_getter.call_args_list == [
        ((), {getter_kwarg: "connector-1"}),
        ((), {getter_kwarg: "connector-1"}),
    ]
    workspace.get_connector.assert_not_called()
    assert [kind for kind, _ in connector.calls] == ["search", "search_status"]


def test_search_tools_are_advertised() -> None:
    """The search tools register like the other Cloud tools."""
    from airbyte.mcp import server

    names = {tool.name for tool in asyncio.run(server.app.list_tools())}
    assert {"execute_external_search_query", "get_cloud_search_status"} <= names


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, None, id="none"),
        pytest.param([], [], id="empty_list"),
        pytest.param([{"a": 1}], [{"a": 1}], id="list"),
        pytest.param('[{"a": 1}, {"b": 2}]', [{"a": 1}, {"b": 2}], id="json_string"),
    ],
)
def test_resolve_list_of_dicts(
    value: list[dict[str, object]] | str | None,
    expected: list[dict[str, object]] | None,
) -> None:
    assert resolve_list_of_dicts(value) == expected


@pytest.mark.parametrize(
    ("value", "match"),
    [
        pytest.param("[{", "not valid JSON", id="invalid_json"),
        pytest.param('{"a": 1}', "not a JSON array of objects", id="object"),
        pytest.param("[1, 2]", "not a JSON array of objects", id="array_of_ints"),
    ],
)
def test_resolve_list_of_dicts_rejects_invalid(value: str, match: str) -> None:
    with pytest.raises(PyAirbyteInputError, match=match) as exc_info:
        resolve_list_of_dicts(value, arg_name="streams")
    assert "`streams`" in exc_info.value.get_message()


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
        self._canonical_name: str | AirbyteError = "source-github"
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

    @property
    def canonical_name(self) -> str:
        """The canonical registry name, raising the stored error when set."""
        if isinstance(self._canonical_name, AirbyteError):
            raise self._canonical_name
        return self._canonical_name

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
    assert result.canonical_connector_name == "source-github"
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
    assert any("Connector definition lookup failed" in w for w in result.warnings)


def test_describe_helper_canonical_name_failure_warns() -> None:
    """A canonical-name lookup failure warns and leaves the field unset."""
    connector = _DescribedConnector()
    connector._canonical_name = AirbyteError(message="lookup boom")  # noqa: SLF001

    result = _describe(connector)

    assert result.canonical_connector_name is None
    assert result.integration_name == "GitHub"
    assert any("Connector definition lookup failed" in w for w in result.warnings)


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


@dataclass
class _TroubleshootConnector:
    """Subset of `CloudConnector` read by `troubleshoot_cloud_connection`."""

    connector_id: str
    connector_type: str
    canonical_name: str
    check_result: (
        CheckResult | AirbyteError | requests.RequestException | NotImplementedError
    ) = dataclasses.field(default_factory=lambda: CheckResult(success=True))
    name: str | None = "Connector"

    @property
    def connector_url(self) -> str:
        return f"https://cloud.example.com/{self.connector_type}s/{self.connector_id}"

    def check(self, *, raise_on_error: bool = True) -> CheckResult:
        assert raise_on_error is False
        if isinstance(
            self.check_result,
            (AirbyteError, requests.RequestException, NotImplementedError),
        ):
            raise self.check_result
        return self.check_result


@dataclass
class _TroubleshootAttempt:
    """Subset of `SyncAttempt` read by `troubleshoot_cloud_connection`."""

    attempt_number: int
    status: str
    log_text: str | AirbyteError | TypeError = ""
    failures: list[SyncAttemptFailure] = dataclasses.field(default_factory=list)
    created_at: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def get_full_log_text(self) -> str:
        if isinstance(self.log_text, (AirbyteError, TypeError)):
            raise self.log_text
        return self.log_text


@dataclass
class _TroubleshootSyncResult(_SyncResultLike):
    """`SyncResult` double that also returns attempts."""

    attempts: (
        list[_TroubleshootAttempt]
        | AirbyteError
        | requests.RequestException
        | NotImplementedError
        | TypeError
    ) = dataclasses.field(default_factory=list)
    raw_attempt_count: int | None = None

    def get_raw_attempt_count(self) -> int:
        return (
            self.raw_attempt_count
            if self.raw_attempt_count is not None
            else len(self.get_attempts())
        )

    def get_attempts(self) -> list[_TroubleshootAttempt]:
        if isinstance(
            self.attempts,
            (AirbyteError, requests.RequestException, NotImplementedError, TypeError),
        ):
            raise self.attempts
        return self.attempts

    def get_job_snapshot(self) -> SyncJobSnapshot:
        return SyncJobSnapshot(
            status=self.get_job_status(),
            bytes_synced=self.bytes_synced,
            records_synced=self.records_synced,
            start_time=self.start_time,
        )


@dataclass
class _TroubleshootConnection:
    """Subset of `CloudConnection` read by `troubleshoot_cloud_connection`."""

    jobs: list[_TroubleshootSyncResult] | AirbyteError = dataclasses.field(
        default_factory=list
    )
    status: str = "active"
    connection_id: str = "connection-id"
    name: str | None = "Postgres to Snowflake"
    connection_url: str = "https://cloud.example.com/connections/connection-id"
    source: _TroubleshootConnector = dataclasses.field(
        default_factory=lambda: _TroubleshootConnector(
            "source-id", "source", "source-postgres"
        )
    )
    destination: _TroubleshootConnector = dataclasses.field(
        default_factory=lambda: _TroubleshootConnector(
            "destination-id", "destination", "destination-snowflake"
        )
    )

    def get_previous_sync_logs(
        self, *, limit: int, from_tail: bool, job_type: JobTypeEnum
    ) -> list[_TroubleshootSyncResult]:
        assert limit == cloud_mcp.TROUBLESHOOT_RECENT_JOBS_LIMIT
        assert from_tail is True
        assert job_type == JobTypeEnum.SYNC
        if isinstance(self.jobs, AirbyteError):
            raise self.jobs
        return self.jobs


@dataclass
class _TroubleshootWorkspace:
    """Subset of `CloudWorkspace` read by `troubleshoot_cloud_connection`."""

    connection: _TroubleshootConnection | AirbyteError
    organization: object | AirbyteError | requests.RequestException | None = None

    def get_connection(self, *, connection_id: str) -> _TroubleshootConnection:
        assert connection_id == "connection-id"
        if isinstance(self.connection, AirbyteError):
            raise self.connection
        return self.connection

    def get_organization(self, *, raise_on_error: bool = True) -> object:
        assert raise_on_error is True
        if isinstance(self.organization, (AirbyteError, requests.RequestException)):
            raise self.organization
        if self.organization is None:
            raise AirbyteError(message="Organization info is incomplete.")
        return self.organization


def _healthy_organization() -> SimpleNamespace:
    return SimpleNamespace(
        organization_id="org-id",
        organization_name="Organization",
        get_billing_status=lambda: SimpleNamespace(
            payment_status="okay",
            subscription_status="subscribed",
            is_account_locked=False,
        ),
    )


def _job(
    job_id: int, status: JobStatusEnum, **kwargs: object
) -> _TroubleshootSyncResult:
    return _TroubleshootSyncResult(
        job_id=job_id,
        status=status,
        start_time=datetime(2026, 1, job_id, tzinfo=timezone.utc),
        **kwargs,
    )


def _troubleshoot(
    monkeypatch: pytest.MonkeyPatch,
    connection: _TroubleshootConnection | AirbyteError,
    *,
    organization: object | AirbyteError | requests.RequestException | None = None,
    max_log_lines: int = cloud_mcp.TROUBLESHOOT_DEFAULT_MAX_LOG_LINES,
) -> cloud_mcp.TroubleshootConnectionResult:
    workspace = _TroubleshootWorkspace(
        connection=connection,
        organization=organization
        if organization is not None
        else _healthy_organization(),
    )
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )
    return cloud_mcp.troubleshoot_cloud_connection(
        cast(Context, object()),
        connection_id="connection-id",
        workspace_id=None,
        max_log_lines=max_log_lines,
    )


_CONFIG_FAILURE = SyncAttemptFailure(
    failure_origin="source",
    failure_type="config_error",
    external_message="Invalid password.",
    retryable=False,
)


def test_troubleshoot_healthy_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """A healthy connection reports identity, passing checks, jobs and billing."""
    connection = _TroubleshootConnection(jobs=[_job(2, JobStatusEnum.SUCCEEDED)])

    result = _troubleshoot(monkeypatch, connection)

    assert result.connection.connection_id == "connection-id"
    assert result.connection.enabled is True
    assert result.connection.source.canonical_connector_name == "source-postgres"
    assert (
        result.connection.destination.canonical_connector_name
        == "destination-snowflake"
    )
    assert result.connection.source.connector_url.endswith("/sources/source-id")
    assert result.source_check.succeeded is True
    assert result.destination_check.succeeded is True
    assert [job.job_id for job in result.recent_jobs.jobs] == [2]
    assert result.latest_failed_job.job_id is None
    assert result.latest_failed_job.message is not None
    assert result.log_tail.log_text == ""
    assert result.billing.status is not None
    assert result.billing.status.billing_info_available is True
    assert result.billing.status.is_account_locked is False


def test_troubleshoot_failed_check_reports_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed source check is reported with its message."""
    connection = _TroubleshootConnection()
    connection.source.check_result = CheckResult(
        success=False, error_message="Authentication failed."
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.source_check.succeeded is False
    assert result.source_check.message == "Authentication failed."
    assert result.destination_check.succeeded is True


def test_troubleshoot_failed_job_reports_attempts_and_log_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The newest failed job's attempts, failures and failed-attempt log tail are included."""
    attempts = [
        _TroubleshootAttempt(0, "failed", "early-1\nearly-2", [_CONFIG_FAILURE]),
        _TroubleshootAttempt(1, "failed", "line-1\nline-2\nline-3", [_CONFIG_FAILURE]),
        _TroubleshootAttempt(2, "running", "latest-running"),
    ]
    connection = _TroubleshootConnection(
        jobs=[
            _job(5, JobStatusEnum.SUCCEEDED),
            _job(4, JobStatusEnum.FAILED, attempts=attempts),
            _job(3, JobStatusEnum.FAILED),
        ]
    )

    result = _troubleshoot(monkeypatch, connection, max_log_lines=2)

    assert [job.job_id for job in result.recent_jobs.jobs] == [5, 4, 3]
    assert result.latest_failed_job.job_id == 4
    assert result.latest_failed_job.status == "failed"
    assert [a.attempt_number for a in result.latest_failed_job.attempts] == [0, 1, 2]
    assert result.latest_failed_job.attempts[1].failures == [
        cloud_mcp.SyncAttemptFailureResult.from_failure(_CONFIG_FAILURE)
    ]
    assert result.log_tail.job_id == 4
    assert result.log_tail.attempt_number == 1
    assert result.log_tail.log_text == "line-2\nline-3"
    assert result.log_tail.log_text_line_count == 2
    assert result.log_tail.total_log_lines_available == 3


def test_troubleshoot_log_tail_falls_back_to_latest_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a failed attempt, the log tail comes from the highest-numbered attempt."""
    attempts = [
        _TroubleshootAttempt(0, "succeeded", "a"),
        _TroubleshootAttempt(1, "running", "b"),
    ]
    connection = _TroubleshootConnection(
        jobs=[_job(4, JobStatusEnum.INCOMPLETE, attempts=attempts)]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.latest_failed_job.status == "incomplete"
    assert result.log_tail.attempt_number == 1
    assert result.log_tail.log_text == "b"


def test_troubleshoot_skips_cancelled_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cancelled job is not treated as the latest failure."""
    connection = _TroubleshootConnection(
        jobs=[
            _job(3, JobStatusEnum.CANCELLED),
            _job(2, JobStatusEnum.INCOMPLETE),
            _job(1, JobStatusEnum.FAILED),
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.latest_failed_job.job_id == 2
    assert result.latest_failed_job.message == "Job has no attempts."


def test_troubleshoot_no_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    """A connection with no jobs still returns a full report."""
    result = _troubleshoot(monkeypatch, _TroubleshootConnection(jobs=[]))

    assert result.recent_jobs.jobs == []
    assert result.recent_jobs.message == "No sync jobs found for this connection."
    assert result.latest_failed_job.job_id is None
    assert result.recent_jobs.error is None
    assert result.source_check.succeeded is True


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        pytest.param(0, 1, id="below-minimum"),
        pytest.param(-5, 1, id="negative"),
        pytest.param(5000, cloud_mcp.TROUBLESHOOT_MAX_LOG_LINES_CAP, id="above-cap"),
    ],
)
def test_troubleshoot_clamps_max_log_lines(
    monkeypatch: pytest.MonkeyPatch, requested: int, expected: int
) -> None:
    """`max_log_lines` is clamped to 1..TROUBLESHOOT_MAX_LOG_LINES_CAP."""
    total_lines = cloud_mcp.TROUBLESHOOT_MAX_LOG_LINES_CAP + 50
    log_text = "\n".join(f"line-{i}" for i in range(total_lines))
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                1,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", log_text)],
            )
        ]
    )

    result = _troubleshoot(monkeypatch, connection, max_log_lines=requested)

    assert result.log_tail.log_text_line_count == expected
    assert result.log_tail.total_log_lines_available == total_lines
    assert result.log_tail.log_text.splitlines()[-1] == f"line-{total_lines - 1}"


def test_troubleshoot_isolates_section_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failures in checks and job history are reported per section."""
    connection = _TroubleshootConnection(jobs=AirbyteError(message="jobs boom"))
    connection.destination.check_result = AirbyteError(message="check boom")

    result = _troubleshoot(monkeypatch, connection)

    assert result.source_check.succeeded is True
    assert result.destination_check.succeeded is None
    assert result.destination_check.error is not None
    assert "check boom" in result.destination_check.error
    assert result.recent_jobs.error is not None
    assert "jobs boom" in result.recent_jobs.error
    assert result.latest_failed_job.error is not None
    assert result.log_tail.error is not None
    assert result.billing.status is not None
    assert result.connection.source.canonical_connector_name == "source-postgres"


def test_troubleshoot_isolates_attempt_and_log_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Attempt and log retrieval failures land in their own sections."""
    attempts_failed = _TroubleshootConnection(
        jobs=[
            _job(
                1, JobStatusEnum.FAILED, attempts=AirbyteError(message="attempts boom")
            )
        ]
    )
    result = _troubleshoot(monkeypatch, attempts_failed)
    assert result.latest_failed_job.job_id == 1
    assert result.latest_failed_job.error is not None
    assert "attempts boom" in result.latest_failed_job.error
    assert result.log_tail.error is not None

    logs_failed = _TroubleshootConnection(
        jobs=[
            _job(
                1,
                JobStatusEnum.FAILED,
                attempts=[
                    _TroubleshootAttempt(
                        0,
                        "failed",
                        AirbyteError(message="logs boom"),
                        [_CONFIG_FAILURE],
                    )
                ],
            )
        ]
    )
    result = _troubleshoot(monkeypatch, logs_failed)
    assert result.latest_failed_job.attempts[0].failures == [
        cloud_mcp.SyncAttemptFailureResult.from_failure(_CONFIG_FAILURE)
    ]
    assert result.log_tail.error is not None
    assert "logs boom" in result.log_tail.error


def test_troubleshoot_billing_without_permission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A billing permission failure is reported in the billing section only."""

    def get_billing_status() -> None:
        raise AirbyteError(message="not allowed")

    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name="Organization",
        get_billing_status=get_billing_status,
    )

    result = _troubleshoot(
        monkeypatch, _TroubleshootConnection(), organization=organization
    )

    assert result.billing.status is not None
    assert result.billing.status.billing_info_available is False
    assert (
        result.billing.status.message
        == "Billing information could not be retrieved: not allowed"
    )
    assert (
        result.billing.error
        == "Billing information could not be retrieved: not allowed"
    )
    assert result.source_check.succeeded is True


def test_troubleshoot_billing_without_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unresolvable organization is reported as a billing error."""
    workspace = _TroubleshootWorkspace(connection=_TroubleshootConnection())
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )

    result = cloud_mcp.troubleshoot_cloud_connection(
        cast(Context, object()), connection_id="connection-id", workspace_id=None
    )

    assert result.billing.status is None
    assert result.billing.error is not None


def test_troubleshoot_reports_disabled_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disabled connection is reported as such."""
    result = _troubleshoot(monkeypatch, _TroubleshootConnection(status="inactive"))

    assert result.connection.status == "inactive"
    assert result.connection.enabled is False


def test_troubleshoot_propagates_connection_lookup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a connection there is nothing to diagnose, so the lookup error propagates."""
    with pytest.raises(AirbyteError, match="no such connection"):
        _troubleshoot(monkeypatch, AirbyteError(message="no such connection"))


def test_troubleshoot_tool_annotations() -> None:
    """The tool runs connector checks, so it is not advertised as read-only or idempotent."""
    (annotations,) = [
        a for f, a in _REGISTERED_TOOLS if f is cloud_mcp.troubleshoot_cloud_connection
    ]
    assert annotations["readOnlyHint"] is False
    assert annotations["idempotentHint"] is False
    assert annotations["openWorldHint"] is True


def test_troubleshoot_check_error_exposes_only_public_failure_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A check that cannot complete reports its external message, not the raw response."""
    connection = _TroubleshootConnection()
    connection.source.check_result = AirbyteError(
        context={
            "failure_origin": "airbyte_platform",
            "external_message": "Failed to create pod",
        },
    )

    error = _troubleshoot(monkeypatch, connection).source_check.error

    assert (
        error
        == "Check did not complete (failure origin: airbyte_platform): Failed to create pod"
    )


def test_troubleshoot_log_tail_drops_stack_trace_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Java `stackTrace:` log lines are omitted from the log tail and its counts."""
    log_text = "line-0\nstackTrace: [Ljava.lang.StackTraceElement;@4b6306e3\nline-1"
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", log_text)],
            )
        ]
    )

    log_tail = _troubleshoot(monkeypatch, connection).log_tail

    assert log_tail.log_text == "line-0\nline-1"
    assert log_tail.total_log_lines_available == 3
    assert log_tail.filtered_line_count == 1


def test_troubleshoot_guidance_classifies_and_restricts_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The returned guidance lists every root cause and the action guardrails."""
    guidance = _troubleshoot(monkeypatch, _TroubleshootConnection()).guidance

    for category in [
        "credentials/auth",
        "network/allowlist",
        "permissions",
        "invalid config",
        "connector bug/regression",
        "schema/state issue",
        "rate limit/transient",
        "billing/account locked",
        "connection disabled",
        "platform/infrastructure",
    ]:
        assert f"- {category}:" in guidance
    for tool_name in [
        "run_cloud_sync",
        "cancel_cloud_sync",
        "get_connector_version_history",
    ]:
        assert tool_name in guidance
    assert "never call update_cloud_connector_config" in guidance
    assert "permanently_delete_*" in guidance
    assert "set_cloud_connection_selected_streams" in guidance
    assert "SafeModeError" in guidance
    assert "secrets" in guidance
    assert "not a specific job" in guidance
    assert "Only state facts this report supports" in guidance
    assert "Only re-run when\n   latest_failed_job.status is failed" in guidance
    flat_guidance = " ".join(guidance.split())
    assert (
        "A 401/403 in a section's error field instead means the Airbyte credentials"
        in (flat_guidance)
    )


def test_troubleshoot_is_discoverable() -> None:
    """The tool docstring and server instructions point agents at the tool."""
    hint = "list_cloud_connections(failing_connections_only=True)"
    docstring = cloud_mcp.troubleshoot_cloud_connection.__doc__ or ""
    assert hint in docstring
    instructions = " ".join(MCP_SERVER_INSTRUCTIONS.split())
    assert "call troubleshoot_cloud_connection" in instructions
    assert hint in instructions


def test_troubleshoot_check_transport_error_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A network failure during a connector check fills only that section's error."""
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", "log")],
            )
        ]
    )
    connection.destination.check_result = requests.ConnectionError("connection reset")

    result = _troubleshoot(monkeypatch, connection)

    assert result.destination_check.error == "Check request failed: connection reset"
    assert result.destination_check.succeeded is None
    assert result.source_check.succeeded is True
    assert result.latest_failed_job.job_id == 4


def test_troubleshoot_log_tail_drops_prefixed_stack_trace_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Event-formatted `[timestamp] LEVEL: stackTrace:` lines are also omitted."""
    log_text = (
        "[2026-09-28T11:54:32] INFO: line-0\n"
        "[2026-09-28T11:54:33] ERROR: stackTrace: [Ljava.lang.StackTraceElement;@4b6306e3\n"
        "[2026-09-28T11:54:34] INFO: line-1"
    )
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", log_text)],
            )
        ]
    )

    log_tail = _troubleshoot(monkeypatch, connection).log_tail

    assert log_tail.log_text == (
        "[2026-09-28T11:54:32] INFO: line-0\n[2026-09-28T11:54:34] INFO: line-1"
    )
    assert log_tail.total_log_lines_available == 3
    assert log_tail.filtered_line_count == 1


def test_troubleshoot_attempts_transport_error_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A network failure loading attempts fills latest_failed_job.error only."""
    connection = _TroubleshootConnection(
        jobs=[
            _job(4, JobStatusEnum.FAILED, attempts=requests.Timeout("read timed out"))
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.latest_failed_job.job_id == 4
    assert result.latest_failed_job.error == "read timed out"
    assert result.log_tail.error == "Attempts unavailable; see latest_failed_job.error."
    assert result.recent_jobs.error is None
    assert result.billing.status is not None


def test_troubleshoot_organization_transport_error_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A network failure resolving the organization fills billing.error only."""
    result = _troubleshoot(
        monkeypatch,
        _TroubleshootConnection(),
        organization=requests.ConnectionError("dns failure"),
    )

    assert result.billing.error == "Organization lookup failed: dns failure"
    assert result.billing.status is None
    assert result.source_check.succeeded is True


@dataclass
class _RefreshingSyncResult(_TroubleshootSyncResult):
    """Sync result whose status changes after the first lookup."""

    refreshed_status: JobStatusEnum = JobStatusEnum.SUCCEEDED
    _lookups: int = 0

    def get_job_status(self) -> JobStatusEnum:
        self._lookups += 1
        return self.status if self._lookups == 1 else self.refreshed_status


def test_troubleshoot_latest_failed_job_keeps_selection_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reported status is the one that qualified the job, not a later refresh."""
    job = _RefreshingSyncResult(
        job_id=4,
        status=JobStatusEnum.INCOMPLETE,
        start_time=datetime(2026, 1, 4, tzinfo=timezone.utc),
    )

    result = _troubleshoot(monkeypatch, _TroubleshootConnection(jobs=[job]))

    assert result.latest_failed_job.job_id == 4
    assert result.latest_failed_job.status == "incomplete"


class _DefinitionErrorConnector(_TroubleshootConnector):
    """Connector whose definition lookup fails."""

    def __getattribute__(self, name: str) -> object:
        if name == "canonical_name":
            raise AirbyteError(message="API error occurred: definition lookup failed")
        return super().__getattribute__(name)


def test_troubleshoot_connector_definition_error_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed definition lookup fills only that connector's error."""
    source = _DefinitionErrorConnector("source-id", "source", "unused")

    result = _troubleshoot(monkeypatch, _TroubleshootConnection(source=source))

    assert result.connection.source.error is not None
    assert "definition lookup failed" in result.connection.source.error
    assert result.connection.source.canonical_connector_name is None
    assert result.connection.destination.error is None
    assert result.source_check.succeeded is True
    assert result.billing.status is not None


def test_troubleshoot_log_tail_is_bounded_by_characters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A last line over the character cap is omitted whole, never cut mid-line."""
    huge_line = "x" * (cloud_mcp.TROUBLESHOOT_MAX_LOG_CHARS + 500)
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", f"first\n{huge_line}")],
            )
        ]
    )

    log_tail = _troubleshoot(monkeypatch, connection).log_tail

    assert log_tail.log_text == ""
    assert log_tail.log_text_truncated is True
    assert log_tail.log_text_line_count == 0
    assert log_tail.total_log_lines_available == 2


def test_troubleshoot_log_tail_under_character_cap_is_not_truncated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal-sized logs are returned whole with `log_text_truncated` false."""
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", "line-0\nline-1")],
            )
        ]
    )

    log_tail = _troubleshoot(monkeypatch, connection).log_tail

    assert log_tail.log_text == "line-0\nline-1"
    assert log_tail.log_text_truncated is False


_NO_CONFIG_API_ROOT = NotImplementedError(
    "Configuration API root not found for api_root='https://custom.example.com'."
)


def test_troubleshoot_check_config_api_root_error_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-inferable Config API root fills only the affected check's error."""
    connection = _TroubleshootConnection()
    connection.source.check_result = _NO_CONFIG_API_ROOT

    result = _troubleshoot(monkeypatch, connection)

    assert result.source_check.succeeded is None
    assert result.source_check.error is not None
    assert "Configuration API root not found" in result.source_check.error
    assert result.destination_check.succeeded is True
    assert result.billing.status is not None


def test_troubleshoot_attempts_config_api_root_error_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-inferable Config API root while loading attempts fills latest_failed_job.error."""
    connection = _TroubleshootConnection(
        jobs=[_job(4, JobStatusEnum.FAILED, attempts=_NO_CONFIG_API_ROOT)]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.latest_failed_job.job_id == 4
    assert result.latest_failed_job.error is not None
    assert "Configuration API root not found" in result.latest_failed_job.error
    assert result.log_tail.error == "Attempts unavailable; see latest_failed_job.error."
    assert result.recent_jobs.error is None


@dataclass
class _NoConfigApiStartTimeSyncResult(_TroubleshootSyncResult):
    """Sync result whose `start_time` fallback needs an unavailable Config API root."""

    @property
    def start_time(self) -> datetime:  # type: ignore[override]
        raise _NO_CONFIG_API_ROOT

    @start_time.setter
    def start_time(self, value: datetime) -> None:
        pass


def test_troubleshoot_recent_jobs_config_api_root_error_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Config API root failure while listing jobs fills recent_jobs.error only."""
    job = _NoConfigApiStartTimeSyncResult(
        job_id=4,
        status=JobStatusEnum.FAILED,
        start_time=datetime(2026, 1, 4, tzinfo=timezone.utc),
    )

    result = _troubleshoot(monkeypatch, _TroubleshootConnection(jobs=[job]))

    assert result.recent_jobs.error is not None
    assert "Configuration API root not found" in result.recent_jobs.error
    assert result.source_check.succeeded is True
    assert result.billing.status is not None


def test_troubleshoot_failure_messages_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Long failure messages are cut and flagged; short ones are untouched."""
    long_failure = dataclasses.replace(_CONFIG_FAILURE, external_message="x" * 50_000)
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[
                    _TroubleshootAttempt(
                        0, "failed", "", [long_failure, _CONFIG_FAILURE]
                    )
                ],
            )
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    long_result, short_result = result.latest_failed_job.attempts[0].failures
    assert long_result.external_message is not None
    assert len(long_result.external_message) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert long_result.external_message.endswith(cloud_mcp.TRUNCATION_MARKER)
    assert long_result.external_message_truncated is True
    assert short_result.external_message == "Invalid password."
    assert short_result.external_message_truncated is False


def test_troubleshoot_section_error_omits_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Section errors show only the message and status, never context or log text."""
    error = AirbyteError(
        message="API error occurred: forbidden",
        context={
            "status_code": 403,
            "body": "internalMessage: leaked-internal stackTrace: leaked-stack",
        },
        log_text="leaked-log",
    )

    result = _troubleshoot(monkeypatch, _TroubleshootConnection(jobs=error))

    assert result.recent_jobs.error == "API error occurred: forbidden (HTTP status 403)"
    assert "leaked" not in result.model_dump_json()


def test_troubleshoot_section_error_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Section errors are cut to the message cap."""
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=requests.RequestException("y" * 50_000),
            )
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.latest_failed_job.error is not None
    assert (
        len(result.latest_failed_job.error) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    )
    assert result.latest_failed_job.error.endswith(cloud_mcp.TRUNCATION_MARKER)


def test_troubleshoot_check_message_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long connector check message is cut to the message cap."""
    connection = _TroubleshootConnection()
    connection.source.check_result = CheckResult(
        success=False, error_message="z" * 50_000
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.source_check.message is not None
    assert len(result.source_check.message) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert result.source_check.message.endswith(cloud_mcp.TRUNCATION_MARKER)


def test_troubleshoot_names_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Long connection and connector names are cut to the message cap."""
    connection = _TroubleshootConnection(name="c" * 50_000)
    connection.source.name = "s" * 50_000

    result = _troubleshoot(monkeypatch, connection)

    assert (
        len(result.connection.connection_name)
        == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    )
    assert result.connection.source.connector_name is not None
    assert (
        len(result.connection.source.connector_name)
        == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    )


def test_troubleshoot_guidance_requires_failed_job_to_be_newest() -> None:
    """Guidance only allows a rerun when no newer job exists."""
    assert "newest entry in" in cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE
    assert "external_message_truncated" in cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE


def test_troubleshoot_log_tail_drops_java_and_python_stack_frames(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stack frames and internal-detail lines are dropped, with or without event prefixes."""
    log_text = "\n".join([
        "Starting sync",
        "\tat io.airbyte.Foo.bar(Foo.java:12)",
        "[2026-01-01 00:00:00] ERROR: at io.airbyte.Foo.baz(Foo.java:34)",
        "Caused by: java.io.IOException: boom",
        "[2026-01-01 00:00:00] ERROR: caused by: leaked-cause",
        "\t... 12 more",
        "Traceback (most recent call last):",
        '  File "/app/main.py", line 10, in run',
        "ValueError: leaked-exception",
        "[2026-01-01 00:00:00] INFO: stacktrace: leaked-stack",
        "internalMessage: leaked-internal",
        'failureReason: {"externalMessage": "x", "internalMessage": "leaked-json"}',
        '{"stacktrace": "leaked-json-stack"}',
        "at least one record was emitted",
        "Sync failed",
    ])
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", log_text)],
            )
        ]
    )

    log_tail = _troubleshoot(monkeypatch, connection).log_tail

    assert log_tail.log_text.splitlines() == [
        "Starting sync",
        "at least one record was emitted",
        "Sync failed",
    ]
    assert log_tail.total_log_lines_available == 15
    assert log_tail.filtered_line_count == 12


def test_troubleshoot_log_tail_char_cap_drops_partial_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The character cap keeps only whole lines and counts them accurately."""
    half = "y" * (cloud_mcp.TROUBLESHOOT_MAX_LOG_CHARS // 2)
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[
                    _TroubleshootAttempt(0, "failed", f"{half}a\n{half}b\n{half}c")
                ],
            )
        ]
    )

    log_tail = _troubleshoot(monkeypatch, connection).log_tail

    assert log_tail.log_text == f"{half}c"
    assert log_tail.log_text_truncated is True
    assert log_tail.log_text_line_count == 1
    assert log_tail.total_log_lines_available == 3
    assert log_tail.filtered_line_count == 0


class _CountingDefinitionConnector(_DescribedConnector):
    """Connector whose definition lookup fails with a transport error and counts calls."""

    def __post_init__(self) -> None:
        super().__post_init__()
        self.definition_lookups = 0

    @property
    def integration_name(self) -> str:
        self.definition_lookups += 1
        raise requests.ConnectionError("dns failure")

    @property
    def canonical_name(self) -> str:
        self.definition_lookups += 1
        raise requests.ConnectionError("dns failure")


def test_describe_helper_definition_failure_warns_once() -> None:
    """A failed definition lookup is attempted once and yields one warning."""
    connector = _CountingDefinitionConnector()

    result = _describe(connector)

    assert connector.definition_lookups == 1
    assert result.warnings == ["Connector definition lookup failed: dns failure"]
    assert result.integration_name is None
    assert result.canonical_connector_name is None


def test_describe_helper_definition_warning_omits_error_context() -> None:
    """Definition lookup warnings never include error context or log text."""
    connector = _DescribedConnector()
    connector._integration_name = AirbyteError(  # noqa: SLF001
        message="API error occurred: boom",
        context={"body": "leaked-body"},
        log_text="leaked-log",
    )

    result = _describe(connector)

    assert result.warnings == [
        "Connector definition lookup failed: API error occurred: boom"
    ]


def test_troubleshoot_check_connector_failure_reason_is_failed_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connector-origin failure reason is a failed check with a message, not an error."""
    connection = _TroubleshootConnection()
    connection.source.check_result = AirbyteError(
        context={
            "failure_origin": "source",
            "external_message": "403 Forbidden: invalid API key",
        },
    )

    section = _troubleshoot(monkeypatch, connection).source_check

    assert section.succeeded is False
    assert section.message == "403 Forbidden: invalid API key"
    assert section.error is None


@dataclass
class _SnapshotErrorSyncResult(_TroubleshootSyncResult):
    """Sync result whose job info lookup fails."""

    def get_job_snapshot(self) -> SyncJobSnapshot:
        raise AirbyteError(message="API error occurred: job lookup failed")


def test_troubleshoot_recent_jobs_isolates_per_job_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One job's lookup error omits only that job; an older failed job is still diagnosed."""
    broken = _SnapshotErrorSyncResult(
        job_id=5,
        status=JobStatusEnum.RUNNING,
        start_time=datetime(2026, 1, 5, tzinfo=timezone.utc),
    )
    connection = _TroubleshootConnection(
        jobs=[
            broken,
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", "boom")],
            ),
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert [job.job_id for job in result.recent_jobs.jobs] == [4]
    assert result.recent_jobs.error is not None
    assert "Job 5: API error occurred: job lookup failed" in result.recent_jobs.error
    assert result.latest_failed_job.job_id == 4
    assert result.latest_failed_job.error is None
    assert result.log_tail.log_text == "boom"


def test_troubleshoot_guidance_forbids_configuration_changes() -> None:
    """Guidance forbids every configuration-mutating tool and flags unfiltered logs."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    for tool_name in [
        "update_cloud_connector_config",
        "update_cloud_connection",
        "set_cloud_connection_table_prefix",
        "rename_cloud_*",
        "set_cloud_connection_selected_streams",
        "permanently_delete_*",
    ]:
        assert tool_name in guidance
    assert "Never change connector, connection or definition configuration" in guidance
    assert "do not enable it yourself" in guidance
    assert "Its output is unfiltered" in guidance
    assert "log_text_line_count lower than total_log_lines_available" in guidance


def test_troubleshoot_guidance_checks_non_sync_jobs_before_rerun() -> None:
    """Guidance says recent_jobs is sync-only and to check other jobs before re-running."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    assert "recent_jobs lists sync jobs only" in guidance
    assert "call list_cloud_sync_jobs three times" in guidance
    assert callable(cloud_mcp.list_cloud_sync_jobs)


_LEAKY_CONTEXT_ERROR = AirbyteError(
    message="Lookup failed.",
    context={"response": '{"internalMessage": "leaked-internal-detail"}'},
)

_LOG_PREFIXES = [
    pytest.param("", id="no-prefix"),
    pytest.param("[2026-09-28T11:54:32] ERROR: ", id="event-prefix"),
    pytest.param("2026-09-28 11:54:32 source > ", id="legacy-prefix"),
    pytest.param(
        "\x1b[32m2026-09-28 11:54:32\x1b[0m \x1b[36mreplication-orchestrator\x1b[0m > ",
        id="legacy-ansi-prefix",
    ),
]

_STACK_TRACE_BLOCKS = [
    pytest.param(
        [
            "Traceback (most recent call last):",
            '  File "/app/main.py", line 10, in run',
            "    raise ValueError(secret)",
            "          ^^^^^^^^^^^^^^^^^^",
            "ValueError: leaked-python-exception",
        ],
        id="python-traceback",
    ),
    pytest.param(
        [
            "\tat io.airbyte.Foo.bar(Foo.java:12)",
            "\tat io.airbyte.Foo.baz(Native Method)",
            "Caused by: java.io.IOException: leaked-cause",
            "\tat io.airbyte.Bar.run(Bar.java:3)",
            "\t... 4 more",
        ],
        id="java-frames-caused-by",
    ),
    pytest.param(
        [
            "\tSuppressed: java.lang.IllegalStateException: leaked-suppressed",
            "\t\tat io.airbyte.Baz.close(Baz.java:7)",
        ],
        id="java-suppressed",
    ),
    pytest.param(
        [
            "    at Object.<anonymous> (/app/index.js:10:5)",
            "    at /app/lib/leaked.js:1:2",
        ],
        id="node-frames",
    ),
    pytest.param(
        [
            "failureReason=FailureReason{internalMessage=leaked-internal, retryable=false}"
        ],
        id="internal-message-equals",
    ),
    pytest.param(
        ['payload={\\"internalMessage\\": \\"leaked-internal\\"}'],
        id="internal-message-escaped-json",
    ),
    pytest.param(['{"stack_trace": "leaked-stack"}'], id="stack-trace-json"),
    pytest.param(
        [
            "java.lang.IllegalStateException: leaked-header",
            "\tat io.airbyte.Foo.bar(Foo.java:12)",
        ],
        id="java-header-frames",
    ),
    pytest.param(
        [
            'Exception in thread "main" java.lang.RuntimeException: leaked-thread',
            "\tat io.airbyte.Main.main(Main.java:5)",
        ],
        id="java-thread-header-frames",
    ),
    pytest.param(
        ['Exception in thread "main" java.lang.RuntimeException: leaked-thread'],
        id="java-thread-header-alone",
    ),
    pytest.param(
        [
            "i.a.w.Worker(run):42 - java.sql.SQLException: leaked-boom",
            "\tat org.postgresql.Driver.connect(Driver.java:1)",
        ],
        id="java-logger-header-frames",
    ),
    pytest.param(
        [
            "Error: leaked-node-error",
            "    at Object.<anonymous> (/app/index.js:10:5)",
        ],
        id="node-header-frames",
    ),
    pytest.param(
        [
            "java.lang.IllegalStateException: leaked-log4j-header",
            "\tat io.airbyte.Foo.bar(Foo.java:12) ~[io.airbyte-foo.jar:?]",
            "\tat java.base/java.lang.Thread.run(Thread.java:1583) [?:?]",
        ],
        id="java-log4j-suffix-frames",
    ),
    pytest.param(
        [
            "io.airbyte.workers.LeakedFailure: leaked-dotted-header",
            "\tat io.airbyte.Foo.bar(Foo.java:12) ~[io.airbyte-foo.jar:?]",
        ],
        id="dotted-header-with-message",
    ),
    pytest.param(
        [
            "io.airbyte.workers.LeakedFailure",
            "\tat io.airbyte.Foo.bar(Foo.java:12)",
        ],
        id="dotted-header-alone",
    ),
    pytest.param(
        [
            "  + Exception Group Traceback (most recent call last):",
            '  |   File "/app/main.py", line 3, in <module>',
            '  |     raise ExceptionGroup("leaked-group", [ValueError("leaked-a")])',
            "  | ExceptionGroup: leaked-group (1 sub-exception)",
            "  +-+---------------- 1 ----------------",
            "    | ValueError: leaked-a",
            "    +------------------------------------",
        ],
        id="python-exception-group",
    ),
]


def test_filter_log_lines_keeps_standalone_dotted_names() -> None:
    """A dotted name not followed by a stack block is ordinary log output."""
    lines = ["io.airbyte.workers.Worker: sync started", "records: 10"]

    assert cloud_mcp._filter_log_lines(lines) == lines  # noqa: SLF001


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        pytest.param("Invalid credentials.", "Invalid credentials.", id="plain"),
        pytest.param(
            "Invalid credentials.\njava.lang.IllegalStateException: leaked\n"
            "\tat io.airbyte.Foo.bar(Foo.java:12) ~[io.airbyte-foo.jar:?]",
            "Invalid credentials. [stack trace removed]",
            id="java-trace",
        ),
        pytest.param(
            "Traceback (most recent call last):\n"
            '  File "/app/main.py", line 1, in run\nValueError: leaked',
            " [stack trace removed]",
            id="only-trace",
        ),
    ],
)
def test_failure_result_filters_stack_traces(message: str, expected: str) -> None:
    """Structured failure messages drop stack-trace lines and mark the removal."""
    result = cloud_mcp.SyncAttemptFailureResult.from_failure(
        SyncAttemptFailure(
            failure_origin="source",
            failure_type=None,
            external_message=message,
            retryable=None,
        )
    )

    assert result.external_message == expected
    assert result.external_message_truncated is False


def test_failure_result_filters_then_caps() -> None:
    """Filtering happens before the cap, and the removal marker survives the cap."""
    message = "x" * 5_000 + "\n\tat io.airbyte.Foo.bar(Foo.java:12)"

    result = cloud_mcp.SyncAttemptFailureResult.from_failure(
        SyncAttemptFailure(
            failure_origin="source",
            failure_type=None,
            external_message=message,
            retryable=None,
        )
    )

    assert result.external_message is not None
    assert result.external_message.endswith(
        cloud_mcp.TRUNCATION_MARKER + cloud_mcp.STACK_TRACE_REMOVED_MARKER
    )
    assert len(result.external_message) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert "Foo.java" not in result.external_message
    assert result.external_message_truncated is True
    assert '" [stack trace removed]"' in cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE


_TRACE_MESSAGE = (
    "Invalid API key.\njava.lang.IllegalStateException: leaked-check\n"
    "\tat io.airbyte.Foo.bar(Foo.java:12) ~[io.airbyte-foo.jar:?]"
)


def test_troubleshoot_check_message_filters_stack_traces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check messages, including the failure-reason fallback, drop stack traces."""
    connection = _TroubleshootConnection()
    connection.source.check_result = CheckResult(
        success=False, error_message=_TRACE_MESSAGE
    )
    connection.destination.check_result = AirbyteError(
        context={
            "failure_origin": "destination",
            "external_message": _TRACE_MESSAGE,
        },
    )

    result = _troubleshoot(monkeypatch, connection)

    for section in (result.source_check, result.destination_check):
        assert section.message == "Invalid API key. [stack trace removed]"
        assert section.error is None


def test_troubleshoot_platform_check_error_filters_stack_traces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A platform-origin check failure's `error` also drops stack traces."""
    connection = _TroubleshootConnection()
    connection.source.check_result = AirbyteError(
        context={
            "failure_origin": "airbyte_platform",
            "external_message": _TRACE_MESSAGE,
        },
    )

    section = _troubleshoot(monkeypatch, connection).source_check

    assert section.error is not None
    assert section.error.endswith("Invalid API key. [stack trace removed]")
    assert "leaked-check" not in section.error


@pytest.mark.parametrize(
    ("readable", "raw_count", "expected_error"),
    [
        pytest.param(1, 1, None, id="all-readable"),
        pytest.param(
            1, 3, "2 attempts returned by the API could not be read.", id="some-dropped"
        ),
        pytest.param(
            0, 2, "2 attempts returned by the API could not be read.", id="all-dropped"
        ),
    ],
)
def test_troubleshoot_reports_dropped_attempts(
    monkeypatch: pytest.MonkeyPatch,
    readable: int,
    raw_count: int,
    expected_error: str | None,
) -> None:
    """Attempts the API returned but `get_attempts` skipped are reported, not hidden."""
    failed_job = _job(
        4,
        JobStatusEnum.FAILED,
        attempts=[_TroubleshootAttempt(0, "failed", "boom")][:readable],
    )
    failed_job.raw_attempt_count = raw_count
    connection = _TroubleshootConnection(jobs=[failed_job])

    result = _troubleshoot(monkeypatch, connection)

    assert result.latest_failed_job.error == expected_error
    if readable:
        assert result.log_tail.log_text == "boom"
        assert result.log_tail.error is None
    else:
        assert result.latest_failed_job.attempts == []
        assert result.log_tail.error is not None
        assert "no attempts" not in result.log_tail.error.lower()
        assert result.log_tail.message is None or (
            "no attempts" not in result.log_tail.message.lower()
        )


@pytest.mark.parametrize("prefix", _LOG_PREFIXES)
@pytest.mark.parametrize("block", _STACK_TRACE_BLOCKS)
def test_filter_log_lines_drops_stack_trace_blocks(
    prefix: str, block: list[str]
) -> None:
    """Known stack-trace blocks and internal-detail keys are dropped under either prefix."""
    lines = [
        f"{prefix}before",
        *(f"{prefix}{line}" for line in block),
        f"{prefix}after",
    ]

    kept = cloud_mcp._filter_log_lines(lines)  # noqa: SLF001

    assert kept == [f"{prefix}before", f"{prefix}after"]


def test_describe_warnings_do_not_leak_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every describe warning renders only the error message, never its context."""

    def _raise(_connector: object) -> object:
        raise _LEAKY_CONTEXT_ERROR

    monkeypatch.setattr(connector_docs, "build_connection_details", _raise)
    connector = _DescribedConnector(connector_type=ConnectorType.DESTINATION)
    connector.config = _LEAKY_CONTEXT_ERROR
    connector.guidance = _LEAKY_CONTEXT_ERROR
    connector.replication_docs = _LEAKY_CONTEXT_ERROR

    result = _describe(
        connector,
        with_config=True,
        with_replication_details=True,
        with_direct_access_guidance=True,
        with_data_replication_docs=True,
    )

    assert len(result.warnings) == 4
    assert all("Lookup failed." in warning for warning in result.warnings)
    assert not any("leaked-internal-detail" in warning for warning in result.warnings)
    fallback = cloud_mcp._feature_lookup_warning(_LEAKY_CONTEXT_ERROR)  # noqa: SLF001
    assert "leaked-internal-detail" not in fallback
    assert fallback.endswith("Lookup failed.")


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(
            AirbyteCloudApiError(message="Forbidden.", status_code=403),
            "Organization lookup was denied for this workspace; this requires "
            "ORGANIZATION_READER permission.",
            id="forbidden",
        ),
        pytest.param(
            AirbyteError(
                message="Organization info is incomplete.",
                context=_LEAKY_CONTEXT_ERROR.context,
            ),
            "Organization lookup failed: Organization info is incomplete.",
            id="airbyte-error",
        ),
        pytest.param(
            requests.ConnectionError("dns failure"),
            "Organization lookup failed: dns failure",
            id="transport",
        ),
    ],
)
def test_troubleshoot_billing_organization_errors(
    monkeypatch: pytest.MonkeyPatch, error: Exception, expected: str
) -> None:
    """Only access denials get the ORGANIZATION_READER hint; other errors are sanitized."""
    result = _troubleshoot(monkeypatch, _TroubleshootConnection(), organization=error)

    assert result.billing.error == expected
    assert result.source_check.succeeded is True


def test_troubleshoot_billing_config_api_root_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing Config API root fills billing.error without the permission hint."""

    def _raise(**_: object) -> object:
        raise NotImplementedError("Config API root unknown.")

    workspace = _TroubleshootWorkspace(connection=_TroubleshootConnection())
    monkeypatch.setattr(workspace, "get_organization", _raise)
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )

    result = cloud_mcp.troubleshoot_cloud_connection(
        cast(Context, object()), connection_id="connection-id", workspace_id=None
    )

    assert (
        result.billing.error == "Organization lookup failed: Config API root unknown."
    )


def test_troubleshoot_isolates_attempt_parsing_type_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed attempts payload fills latest_failed_job.error only."""
    connection = _TroubleshootConnection(
        jobs=[_job(4, JobStatusEnum.FAILED, attempts=TypeError("malformed attempts"))]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.latest_failed_job.job_id == 4
    assert result.latest_failed_job.error == "malformed attempts"
    assert result.billing.error is None


def test_troubleshoot_reports_deleted_connection_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deleted connection reports its raw `deprecated` status and guidance never says enable it."""
    result = _troubleshoot(monkeypatch, _TroubleshootConnection(status="deprecated"))

    assert result.connection.status == "deprecated"
    assert result.connection.enabled is False
    guidance = " ".join(result.guidance.split())
    assert (
        "connection.status is deprecated. The connection was deleted and cannot be enabled"
        in guidance
    )
    assert "connection.status is inactive" in guidance


def test_troubleshoot_guidance_round_two_rules() -> None:
    """Rerun needs a complete job list and the newest job overall; tools are allowlisted."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    assert "require recent_jobs.error to be null" in guidance
    assert "it must be latest_failed_job.job_id" in guidance
    assert (
        "only call read-only tools, plus run_cloud_sync and cancel_cloud_sync under the "
        "conditions in rule 2" in guidance
    )
    for tool in (
        "update_custom_source_definition",
        "publish_custom_source_definition",
        "deploy_connector_to_cloud",
        "deploy_noop_destination_to_cloud",
        "create_connection_on_cloud",
        "set_cloud_connection_selected_streams",
    ):
        assert tool in guidance
    assert "always removed" not in guidance
    assert (
        "Known stack-trace and internal-detail formats are removed; never quote any that remain"
        in guidance
    )
    assert (
        "total_log_lines_available is the raw line count used by get_cloud_sync_logs"
        in guidance
    )
    assert "filtered_line_count" in guidance


def test_get_cloud_sync_status_uses_one_job_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status, counts and start time all come from a single job snapshot."""
    calls: list[int] = []
    start_time = datetime(2026, 9, 1, tzinfo=timezone.utc)

    def _snapshot() -> SyncJobSnapshot:
        calls.append(1)
        return SyncJobSnapshot(
            status=JobStatusEnum.SUCCEEDED,
            bytes_synced=42,
            records_synced=7,
            start_time=start_time,
        )

    sync_result = SimpleNamespace(
        job_id=9, job_url="https://cloud.example.com/jobs/9", get_job_snapshot=_snapshot
    )
    connection = SimpleNamespace(get_sync_result=lambda job_id=None: sync_result)
    workspace = SimpleNamespace(get_connection=lambda connection_id: connection)
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )

    result = cloud_mcp.get_cloud_sync_status(
        cast(Context, object()),
        connection_id="connection-id",
        job_id=9,
        workspace_id=None,
        include_attempts=False,
    )

    assert calls == [1]
    assert result["status"] == JobStatusEnum.SUCCEEDED
    assert result["bytes_synced"] == 42
    assert result["records_synced"] == 7
    assert result["start_time"] == start_time.isoformat()


def test_troubleshoot_isolates_malformed_log_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed log payload fills log_tail.error only."""
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[
                    _TroubleshootAttempt(0, "failed", TypeError("bad log payload"))
                ],
            )
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.log_tail.error == "bad log payload"
    assert result.latest_failed_job.job_id == 4
    assert result.latest_failed_job.error is None


def test_troubleshoot_billing_status_access_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 403 from get_billing_status, even wrapped from a token-mint HTTPError, sets billing.error."""
    response = requests.Response()
    response.status_code = 403
    http_error = requests.HTTPError("403 Client Error", response=response)

    def get_billing_status() -> None:
        raise AirbyteError(
            message="Failed to retrieve organization billing information."
        ) from http_error

    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name="Organization",
        get_billing_status=get_billing_status,
    )

    billing = _troubleshoot(
        monkeypatch, _TroubleshootConnection(), organization=organization
    ).billing

    assert billing.error == (
        "Billing lookup was denied for this organization; this requires "
        "ORGANIZATION_READER permission."
    )
    assert billing.status is not None
    assert billing.status.billing_info_available is False


def test_describe_passthrough_warnings_are_bounded() -> None:
    """Context-layer warnings passed through to describe output are capped."""
    connector = _DescribedConnector(
        enabled_features=frozenset({ConnectorFeature.DIRECT_ACCESS})
    )
    connector.workspace = SimpleNamespace(_has_context_layer_api=lambda: True)
    connector.inspect_result = SimpleNamespace(
        warnings=["w" * (cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS * 2)]
    )

    warnings = _describe(connector).warnings

    assert len(warnings) == 1
    assert len(warnings[0]) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert warnings[0].endswith(cloud_mcp.TRUNCATION_MARKER)


def test_troubleshoot_guidance_round_three_rules() -> None:
    """Rerun checks refresh and clear jobs; log follow-ups target the same job and attempt."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    assert "with job_type='refresh' and with job_type='clear'" in guidance
    assert "which returns sync and reset jobs only" in guidance
    assert (
        "call get_cloud_sync_logs with job_id=log_tail.job_id and "
        "attempt_number=log_tail.attempt_number" in guidance
    )
    assert "total_log_lines_available counts that job and attempt's lines" in guidance


def test_filter_log_lines_keeps_error_line_without_frames() -> None:
    """An exception-like line that does not start a stack trace is kept."""
    lines = ["java.io.IOException: connection refused", "retrying"]

    assert cloud_mcp._filter_log_lines(lines) == lines  # noqa: SLF001


def test_troubleshoot_log_tail_all_lines_filtered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When every raw log line is filtered, the message says so instead of 'No logs'."""
    log_text = "java.lang.IllegalStateException: x\n\tat io.airbyte.Foo.bar(Foo.java:1)"
    connection = _TroubleshootConnection(
        jobs=[
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", log_text)],
            )
        ]
    )

    log_tail = _troubleshoot(monkeypatch, connection).log_tail

    assert log_tail.log_text is None or log_tail.log_text == ""
    assert log_tail.total_log_lines_available == 2
    assert log_tail.filtered_line_count == 2
    assert log_tail.message == (
        "All 2 log lines were removed as stack traces or internal details; "
        "use get_cloud_sync_logs with this job_id and attempt_number if needed."
    )


@pytest.mark.parametrize(
    "attempt_data",
    [
        pytest.param(
            {"id": 1, "status": None, "createdAt": 1767225600}, id="null-status"
        ),
        pytest.param(
            {"id": 1, "status": "failed", "createdAt": None}, id="null-created-at"
        ),
        pytest.param({"id": 1}, id="missing-fields"),
    ],
)
def test_troubleshoot_skips_unreadable_attempt(
    monkeypatch: pytest.MonkeyPatch, attempt_data: dict[str, object]
) -> None:
    """A malformed attempt is skipped and noted; readable attempts and the log tail remain."""
    malformed = SyncAttempt(
        workspace=cast(CloudWorkspace, None),
        connection=cast(CloudConnection, None),
        job_id=4,
        attempt_number=1,
        _attempt_data={"attempt": attempt_data},
    )
    good = _TroubleshootAttempt(0, "failed", "good log line")
    connection = _TroubleshootConnection(
        jobs=[_job(4, JobStatusEnum.FAILED, attempts=[good, malformed])]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert [a.attempt_number for a in result.latest_failed_job.attempts] == [0]
    assert result.latest_failed_job.error == "Attempt 1 could not be read."
    assert result.log_tail.attempt_number == 0
    assert result.log_tail.log_text == "good log line"
    assert result.log_tail.error is None


@pytest.mark.parametrize(
    ("status_code", "expected_error"),
    [
        pytest.param(
            403,
            "Billing lookup was denied for this organization; this requires "
            "ORGANIZATION_READER permission.",
            id="forbidden",
        ),
        pytest.param(
            500,
            "Billing information could not be retrieved: "
            "Failed to retrieve organization billing information. (HTTP status 500)",
            id="server-error",
        ),
    ],
)
def test_troubleshoot_billing_status_failure_sets_error(
    monkeypatch: pytest.MonkeyPatch, status_code: int, expected_error: str
) -> None:
    """Any get_billing_status failure sets billing.error; 401/403 names the billing lookup."""

    def get_billing_status() -> None:
        raise AirbyteError(
            message="Failed to retrieve organization billing information.",
            context={"status_code": status_code},
        )

    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name="Organization",
        get_billing_status=get_billing_status,
    )

    billing = _troubleshoot(
        monkeypatch, _TroubleshootConnection(), organization=organization
    ).billing

    assert billing.error == expected_error
    assert billing.status is not None
    assert billing.status.billing_info_available is False


class _DecodeErrorSyncResult(_TroubleshootSyncResult):
    """Sync result whose job lookup goes through `api_util.get_job_info`."""

    def get_job_snapshot(self) -> SyncJobSnapshot:
        api_util.get_job_info(
            job_id=self.job_id,
            api_root="https://api.example.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=None,
        )
        raise AssertionError("get_job_info should have raised")


@pytest.mark.parametrize(
    "decode_error",
    [
        KeyError("leaked-body"),
        AttributeError("'dict' object has no attribute 'x': leaked-body"),
        TypeError("leaked-body"),
        ValueError("leaked-body"),
    ],
)
def test_troubleshoot_job_info_decode_error_isolated(
    monkeypatch: pytest.MonkeyPatch, decode_error: Exception
) -> None:
    """A model-decode error in `get_job_info` fills only recent_jobs.error, without the body."""

    def _get_job(_request: object) -> object:
        raise decode_error

    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: SimpleNamespace(jobs=SimpleNamespace(get_job=_get_job)),
    )
    connection = _TroubleshootConnection(
        jobs=[
            _DecodeErrorSyncResult(
                job_id=5,
                status=JobStatusEnum.RUNNING,
                start_time=datetime(2026, 1, 5, tzinfo=timezone.utc),
            ),
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", "boom")],
            ),
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.recent_jobs.error == (
        "Some jobs could not be read and are omitted: Job 5: Unexpected API response."
    )
    assert "leaked-body" not in result.model_dump_json()
    assert [job.job_id for job in result.recent_jobs.jobs] == [4]
    assert result.latest_failed_job.job_id == 4
    assert result.log_tail.log_text == "boom"
    assert result.source_check.succeeded is True
    assert result.billing.error is None


def test_api_util_decode_error_has_ids_only_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The normalized decode error carries only IDs and does not chain the decode error."""

    def _get_connection(_request: object) -> object:
        raise AttributeError("leaked-body")

    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: SimpleNamespace(
            connections=SimpleNamespace(get_connection=_get_connection)
        ),
    )

    with pytest.raises(AirbyteError, match="Unexpected API response.") as info:
        api_util.get_connection(
            workspace_id="workspace-id",
            connection_id="connection-id",
            api_root="https://api.example.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=None,
        )

    assert info.value.context == {
        "workspace_id": "workspace-id",
        "connection_id": "connection-id",
    }
    assert info.value.__cause__ is None
    assert "leaked-body" not in str(info.value)


def test_troubleshoot_no_readable_job_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """When jobs were listed but none could be read, the failed-job sections say so."""
    connection = _TroubleshootConnection(
        jobs=[
            _SnapshotErrorSyncResult(
                job_id=5,
                status=JobStatusEnum.FAILED,
                start_time=datetime(2026, 1, 5, tzinfo=timezone.utc),
            )
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    expected = "No job status could be read; see recent_jobs.error."
    assert result.recent_jobs.error is not None
    assert result.latest_failed_job.error == expected
    assert result.log_tail.error == expected
    assert result.latest_failed_job.message is None
    assert result.log_tail.message is None


@pytest.mark.parametrize(
    "frame",
    [
        "    at async Promise.all (index 0)",
        "    at Array.forEach (<anonymous>)",
        "    at new Promise (<anonymous>)",
        "    at Object.<anonymous> (/app/index.js:1:2)",
        "    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)",
        "    at /app/lib/leaked.js:1:2",
        "    at async main.run:10:5",
        "    at JSON.parse (native)",
    ],
)
@pytest.mark.parametrize(
    "prefix", ["", "[2026-09-28T11:54:32] ERROR: ", "2026-09-28 11:54:32 source > "]
)
def test_filter_log_lines_drops_node_frames(frame: str, prefix: str) -> None:
    """Node frames with and without file:line are dropped with their header."""
    lines = ["before", f"{prefix}Error: leaked-node", f"{prefix}{frame}", "after"]

    assert cloud_mcp._filter_log_lines(lines) == ["before", "after"]  # noqa: SLF001


def test_filter_log_lines_frame_block_continues_on_any_at_line() -> None:
    """Inside a frame block, any `at ...` line continues the block."""
    lines = [
        "before",
        "    at Array.forEach (<anonymous>)",
        "    at some unusual frame format",
        "after",
    ]

    assert cloud_mcp._filter_log_lines(lines) == ["before", "after"]  # noqa: SLF001


@pytest.mark.parametrize(
    "line",
    [
        "java.lang.IllegalStateException: leaked\\n\\tat io.airbyte.Foo.bar(Foo.java:12)",
        "failure: leaked\\\\n\\\\tat io.airbyte.Foo.bar(Foo.java:12)",
        "Error: failed\\n    at Object.run (/app/index.js:1:2)",
        "Sync failed: Traceback (most recent call last): File x.py leaked",
        '{"exception": "leaked-exception"}',
        '{\\"throwable\\": \\"leaked\\"}',
        "{'stack': 'leaked'}",
        '"internalMessage": "leaked"',
        "stack_trace=leaked",
    ],
)
def test_filter_log_lines_drops_serialized_traces_and_keys(line: str) -> None:
    """One-line serialized traces and quoted internal-detail keys are dropped."""
    assert cloud_mcp._filter_log_lines(["before", line, "after"]) == [  # noqa: SLF001
        "before",
        "after",
    ]


@pytest.mark.parametrize(
    "line",
    [
        "Increase the stack size: exception handling is described in the docs",
        "throwable objects are retried",
        "the exception: none",
    ],
)
def test_filter_log_lines_keeps_prose_with_key_words(line: str) -> None:
    """Unquoted `exception`/`throwable`/`stack` in prose are kept."""
    assert cloud_mcp._filter_log_lines([line]) == [line]  # noqa: SLF001


def test_bounded_message_marker_survives_cap() -> None:
    """The stack-trace marker is appended after the cap, so it always survives."""
    message = "x" * 5_000 + "\n    at Array.forEach (<anonymous>)"

    bounded, truncated = cloud_mcp._bounded_message(message)  # noqa: SLF001

    assert bounded is not None
    assert bounded.endswith(cloud_mcp.STACK_TRACE_REMOVED_MARKER)
    assert len(bounded) <= cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert truncated is True


def _failure(index: int) -> SyncAttemptFailure:
    return SyncAttemptFailure(
        failure_origin="source",
        failure_type="system_error",
        external_message=f"failure {index}",
        retryable=False,
    )


def test_troubleshoot_bounds_attempts_and_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the last attempts and the first failures per attempt are reported."""
    attempts = [
        _TroubleshootAttempt(
            number,
            "failed",
            f"log {number}",
            failures=[_failure(index) for index in range(7)],
        )
        for number in range(12)
    ]
    connection = _TroubleshootConnection(
        jobs=[_job(4, JobStatusEnum.FAILED, attempts=attempts)]
    )

    failed_job = _troubleshoot(monkeypatch, connection).latest_failed_job

    assert failed_job.attempts_omitted == 2
    assert [a.attempt_number for a in failed_job.attempts] == list(range(2, 12))
    assert [f.external_message for f in failed_job.attempts[-1].failures] == [
        f"failure {index}" for index in range(5)
    ]
    assert all(a.failures_omitted == 2 for a in failed_job.attempts)


def test_troubleshoot_bounds_api_string_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """Organization, billing, attempt status and failure origin/type strings are capped."""
    huge = "y" * 5_000
    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name=huge,
        get_billing_status=lambda: SimpleNamespace(
            payment_status=huge, subscription_status=huge, is_account_locked=False
        ),
    )
    attempt = _TroubleshootAttempt(
        0,
        huge,
        "boom",
        failures=[
            SyncAttemptFailure(
                failure_origin=huge,
                failure_type=huge,
                external_message="x",
                retryable=None,
            )
        ],
    )
    connection = _TroubleshootConnection(
        jobs=[_job(4, JobStatusEnum.FAILED, attempts=[attempt])]
    )

    result = _troubleshoot(monkeypatch, connection, organization=organization)

    limit = cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    status = result.billing.status
    assert status is not None
    row = result.latest_failed_job.attempts[0]
    for value in [
        status.organization_name,
        status.payment_status,
        status.subscription_status,
        row.status,
        row.failures[0].failure_origin,
        row.failures[0].failure_type,
    ]:
        assert value is not None
        assert len(value) == limit
        assert value.endswith(cloud_mcp.TRUNCATION_MARKER)


_LEAKY_CHECK_RESPONSE: dict[str, object] = {
    "status": "failed",
    "message": None,
    "jobInfo": {
        "failureReason": {
            "failureOrigin": "airbyte_platform",
            "failureType": "system_error",
            "externalMessage": "Workload launch failed.",
            "internalMessage": "leaked-internal-detail",
            "stacktrace": "java.lang.IllegalStateException: leaked-stacktrace",
        }
    },
}


class _ApiCheckConnector:
    """Connector double whose check goes through `api_util.check_connector`."""

    connector_id = "source-id"
    connector_type = "source"

    def check(self, *, raise_on_error: bool) -> CheckResult:
        assert raise_on_error is False
        success, message = api_util.check_connector(
            actor_id=self.connector_id,
            connector_type="source",
            client_id=None,
            client_secret=None,
            bearer_token=None,
            config_api_root="https://config.example.com",
        )
        return CheckResult(success=success, error_message=message)


def _check_cloud_connector_with(
    monkeypatch: pytest.MonkeyPatch,
    response: object,
    make_request: Callable[..., object] | None = None,
) -> cloud_mcp.ConnectorCheckResult:
    monkeypatch.setattr(
        api_util, "_make_config_api_request", make_request or (lambda **_: response)
    )
    workspace = SimpleNamespace(get_source=lambda *, source_id: _ApiCheckConnector())
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )
    return cloud_mcp.check_cloud_connector(
        ctx=cast(Context, object()),
        connector_id="source-id",
        connector_type="source",
        workspace_id="workspace-id",
    )


def test_check_connector_error_context_has_no_raw_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The check error carries only failure origin, type and message, not the response."""
    monkeypatch.setattr(
        api_util, "_make_config_api_request", lambda **_: _LEAKY_CHECK_RESPONSE
    )

    with pytest.raises(AirbyteError) as exc_info:
        _ApiCheckConnector().check(raise_on_error=False)

    rendered = str(exc_info.value)
    assert "leaked-internal-detail" not in rendered
    assert "leaked-stacktrace" not in rendered
    assert "response" not in exc_info.value.context
    assert exc_info.value.context["failure_origin"] == "airbyte_platform"
    assert exc_info.value.context["failure_type"] == "system_error"
    assert exc_info.value.context["external_message"] == "Workload launch failed."


def test_check_connector_error_context_bounds_external_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The external message carried in the check error's context is capped."""
    response = {
        "status": "failed",
        "jobInfo": {
            "failureReason": {
                "failureOrigin": "airbyte_platform",
                "externalMessage": "z" * 9_000,
            }
        },
    }
    monkeypatch.setattr(api_util, "_make_config_api_request", lambda **_: response)

    with pytest.raises(AirbyteError) as exc_info:
        _ApiCheckConnector().check(raise_on_error=False)

    message = exc_info.value.context["external_message"]
    assert len(message) == 2_000
    assert message.endswith(" [truncated]")


def test_check_cloud_connector_platform_failure_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A platform-origin check failure raises a safe error instead of a failed check."""
    with pytest.raises(AirbyteError) as exc_info:
        _check_cloud_connector_with(monkeypatch, _LEAKY_CHECK_RESPONSE)

    assert exc_info.value.get_message() == (
        "Check did not complete (failure origin: airbyte_platform): Workload launch failed."
    )
    rendered = str(exc_info.value)
    assert "leaked-internal-detail" not in rendered
    assert "leaked-stacktrace" not in rendered
    assert exc_info.value.__cause__ is None


def test_check_cloud_connector_platform_failure_without_message_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A platform failure with no external message raises the bounded fallback text."""
    response = {
        "status": "failed",
        "jobInfo": {
            "failureReason": {
                "failureOrigin": "airbyte_platform",
                "internalMessage": "leaked-internal-detail",
            }
        },
    }

    with pytest.raises(AirbyteError) as exc_info:
        _check_cloud_connector_with(monkeypatch, response)

    assert exc_info.value.get_message() == "Connector check did not complete."
    assert "leaked-internal-detail" not in str(exc_info.value)


def test_check_cloud_connector_connector_failure_is_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connector-origin check failure without a message is a failed check result."""
    response = {
        "status": "failed",
        "jobInfo": {
            "failureReason": {
                "failureOrigin": "source",
                "externalMessage": "Invalid password.",
                "internalMessage": "leaked-internal-detail",
            }
        },
    }

    result = _check_cloud_connector_with(monkeypatch, response)

    assert result.succeeded is False
    assert result.message == "Invalid password."


def test_check_cloud_connector_http_403_raises_without_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An Airbyte API 403 raises; the error keeps the status code but no response body."""
    forbidden = AirbyteError(
        message="API request failed with status 403",
        context={
            "status_code": 403,
            "response": {"text": '{"internalMessage": "leaked-internal-detail"}'},
            "body": "leaked-request-body",
        },
    )

    def _raise(**_: object) -> object:
        raise forbidden

    with pytest.raises(AirbyteError) as exc_info:
        _check_cloud_connector_with(monkeypatch, None, make_request=_raise)

    rendered = str(exc_info.value)
    assert "leaked-internal-detail" not in rendered
    assert "leaked-request-body" not in rendered
    assert exc_info.value.context == {
        "connector_id": "source-id",
        "connector_type": "source",
        "status_code": 403,
    }
    assert exc_info.value.get_message() == "API request failed with status 403"


@pytest.mark.parametrize(
    ("make_request", "expected_message"),
    [
        pytest.param(None, "Unexpected check response.", id="null-body"),
        pytest.param(
            requests.ConnectionError("connection refused"),
            "Check request failed: connection refused",
            id="transport-error",
        ),
    ],
)
def test_check_cloud_connector_non_check_errors_raise(
    monkeypatch: pytest.MonkeyPatch,
    make_request: Exception | None,
    expected_message: str,
) -> None:
    """Unexpected bodies and transport errors raise instead of looking like check failures."""

    def _raise(**_: object) -> object:
        assert make_request is not None
        raise make_request

    with pytest.raises(AirbyteError) as exc_info:
        _check_cloud_connector_with(
            monkeypatch, None, make_request=None if make_request is None else _raise
        )

    assert exc_info.value.get_message() == expected_message


def test_check_cloud_connector_message_filters_stack_traces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A normal failed-check message is filtered and bounded."""
    response = {"status": "failed", "message": _TRACE_MESSAGE}

    result = _check_cloud_connector_with(monkeypatch, response)

    assert result.succeeded is False
    assert result.message == "Invalid API key. [stack trace removed]"


class _NonDictJobSyncResult(_TroubleshootSyncResult):
    """Sync result whose snapshot reads the Config API job through a real `SyncResult`."""

    def get_job_snapshot(self) -> SyncJobSnapshot:
        workspace = SimpleNamespace(
            api_root="https://api.example.com/v1",
            config_api_root="https://config.example.com",
            client_id=None,
            client_secret=None,
            bearer_token="token",
        )
        SyncResult(
            workspace=cast(CloudWorkspace, workspace),
            connection=cast(CloudConnection, SimpleNamespace(connection_id="c")),
            job_id=self.job_id,
        )._fetch_job_with_attempts()  # noqa: SLF001
        raise AssertionError("the non-dict body should have raised")


@pytest.mark.parametrize("body", [None, [], "leaked-body"])
def test_troubleshoot_non_dict_config_api_job_omits_only_that_job(
    monkeypatch: pytest.MonkeyPatch, body: object
) -> None:
    """A non-object Config API job body omits only that job, without the body."""
    monkeypatch.setattr(api_util, "_make_config_api_request", lambda **_: body)
    connection = _TroubleshootConnection(
        jobs=[
            _NonDictJobSyncResult(
                job_id=5,
                status=JobStatusEnum.RUNNING,
                start_time=datetime(2026, 1, 5, tzinfo=timezone.utc),
            ),
            _job(
                4,
                JobStatusEnum.FAILED,
                attempts=[_TroubleshootAttempt(0, "failed", "boom")],
            ),
        ]
    )

    result = _troubleshoot(monkeypatch, connection)

    assert result.recent_jobs.error == (
        "Some jobs could not be read and are omitted: Job 5: Unexpected API response."
    )
    assert [job.job_id for job in result.recent_jobs.jobs] == [4]
    assert result.latest_failed_job.job_id == 4
    assert "leaked-body" not in result.model_dump_json()


def test_bounded_message_keeps_marker_space_when_all_lines_removed() -> None:
    """A message whose lines are all removed is just the marker, leading space included."""
    bounded, truncated = cloud_mcp._bounded_message(  # noqa: SLF001
        "    at Array.forEach (<anonymous>)"
    )

    assert bounded == cloud_mcp.STACK_TRACE_REMOVED_MARKER
    assert bounded.startswith(" ")
    assert truncated is False


def test_guidance_describes_combined_markers() -> None:
    """Guidance says text containing the truncation marker was cut and markers can combine."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    assert 'Text containing " [truncated]"' in guidance
    assert "both markers can appear together" in guidance


def test_troubleshoot_billing_non_str_organization_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-string organization name becomes None instead of failing the section."""
    organization = SimpleNamespace(
        organization_id="org-id",
        organization_name=12345,
        get_billing_status=lambda: SimpleNamespace(
            payment_status="ok",
            subscription_status="subscribed",
            is_account_locked=False,
        ),
    )

    billing = _troubleshoot(
        monkeypatch, _TroubleshootConnection(), organization=organization
    ).billing

    assert billing.error is None
    assert billing.status is not None
    assert billing.status.organization_name is None


@pytest.mark.parametrize("value", [12345, None, ["x"], {"a": 1}])
def test_bounded_text_non_str_is_none(value: object) -> None:
    """`_bounded_text` returns None for anything that is not a string."""
    assert cloud_mcp._bounded_text(value) is None  # noqa: SLF001


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.MissingSchema("Invalid URL 'api.example.com'"),
        requests.exceptions.InvalidURL("bad url"),
        requests.exceptions.JSONDecodeError("Expecting value", "", 0),
    ],
)
def test_api_util_decode_handler_propagates_request_errors(
    monkeypatch: pytest.MonkeyPatch, error: requests.RequestException
) -> None:
    """Request errors that subclass ValueError are not rewritten as decode errors."""

    def _get_connection(_request: object) -> object:
        raise error

    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: SimpleNamespace(
            connections=SimpleNamespace(get_connection=_get_connection)
        ),
    )

    with pytest.raises(requests.RequestException) as info:
        api_util.get_connection(
            workspace_id="workspace-id",
            connection_id="connection-id",
            api_root="https://api.example.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=None,
        )

    assert info.value is error


def _workspace_with_org_info(
    monkeypatch: pytest.MonkeyPatch, info: dict[str, object]
) -> CloudWorkspace:
    monkeypatch.setattr(
        CloudWorkspace, "_organization_info", property(lambda _self: info)
    )
    return CloudWorkspace(workspace_id="workspace-id", bearer_token="token")


@pytest.mark.parametrize(
    "info",
    [
        {"organizationId": 12345, "organizationName": "Organization"},
        {"organizationId": "org-id", "organizationName": ["Organization"]},
    ],
)
def test_get_organization_rejects_non_str_fields(
    monkeypatch: pytest.MonkeyPatch, info: dict[str, object]
) -> None:
    """Non-string organization fields are treated as incomplete organization info."""
    workspace = _workspace_with_org_info(monkeypatch, info)

    with pytest.raises(AirbyteError, match="Organization info is incomplete."):
        workspace.get_organization(raise_on_error=True)
    assert workspace.get_organization(raise_on_error=False) is None


def test_troubleshoot_billing_non_str_organization_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-string organization ID sets billing.error and leaves other sections intact."""
    real_workspace = _workspace_with_org_info(
        monkeypatch, {"organizationId": 12345, "organizationName": "Organization"}
    )
    monkeypatch.setattr(
        _TroubleshootWorkspace,
        "get_organization",
        lambda _self, *, raise_on_error=True: real_workspace.get_organization(
            raise_on_error=raise_on_error
        ),
    )
    connection = _TroubleshootConnection(jobs=[_job(2, JobStatusEnum.SUCCEEDED)])

    result = _troubleshoot(monkeypatch, connection)

    assert result.billing.error == (
        "Organization lookup failed: Organization info is incomplete."
    )
    assert result.billing.status is None
    assert result.connection.connection_id == "connection-id"
    assert result.source_check.succeeded is True
    assert [job.job_id for job in result.recent_jobs.jobs] == [2]


@pytest.mark.parametrize(
    "billing",
    [
        {"paymentStatus": ["locked"], "subscriptionStatus": "subscribed"},
        {"paymentStatus": "okay", "subscriptionStatus": {"state": "unsubscribed"}},
    ],
)
def test_organization_billing_status_ignores_non_str_values(
    monkeypatch: pytest.MonkeyPatch, billing: dict[str, object]
) -> None:
    """Array or object billing statuses are treated as unknown instead of raising."""
    monkeypatch.setattr(
        api_util, "get_organization_info", lambda **_: {"billing": billing}
    )
    organization = CloudOrganization(organization_id="org-id", bearer_token="token")

    info = organization.get_billing_status()

    assert not isinstance(info.payment_status, (list, dict))
    assert not isinstance(info.subscription_status, (list, dict))
    assert info.is_account_locked is False


@pytest.mark.parametrize(
    "lines",
    [
        pytest.param(
            [
                "before",
                "internalMessage: private details",
                "  request payload with leaked-credentials",
                "",
                "\tleaked-second-line",
                "after",
            ],
            id="indented",
        ),
        pytest.param(
            [
                "2026-01-01 00:00:00 platform > before",
                '2026-01-01 00:00:01 platform > "stackTrace": "leaked-first',
                "leaked-unindented-continuation",
                'leaked-last"',
                "2026-01-01 00:00:02 platform > after",
            ],
            id="unprefixed-lines-in-prefixed-log",
        ),
    ],
)
def test_filter_log_lines_drops_multiline_internal_details(lines: list[str]) -> None:
    """The continuation lines of a multi-line internal-detail value are dropped with it."""
    kept = cloud_mcp._filter_log_lines(lines)  # noqa: SLF001

    assert kept == [lines[0], lines[-1]]


def test_bounded_message_drops_multiline_internal_detail() -> None:
    """A message keeps its text but not an internal detail's continuation lines."""
    message, truncated = cloud_mcp._bounded_message(  # noqa: SLF001
        "Sync failed.\ninternalMessage: private details\n  leaked-payload\nCheck the host."
    )

    assert message == "Sync failed.\nCheck the host. [stack trace removed]"
    assert truncated is False


@pytest.mark.parametrize(
    "line",
    [
        "Failure boom\\u000a\\u0009at com.foo.Bar.baz(Bar.java:12)",
        "Failure boom\\n\\u0009at com.foo.Bar.baz(Bar.java:12)",
        "Failure boom\\\\\\n\\\\\\tat com.foo.Bar.baz(Bar.java:12)",
        "Failure boom\\r\\tat com.foo.Bar.baz(Bar.java:12)",
        'Failure\\n  File \\"/app/x.py\\", line 12, in foo\\n    leaked(token)',
        '{\\\\\\"message\\\\\\":\\\\\\"bad\\\\\\",\\\\\\"internalMessage\\\\\\":\\\\\\"leaked\\\\\\"}',
        "{\\u0022internalMessage\\u0022:\\u0022leaked\\u0022}",
        "{\\u0022exception\\u0022:\\u0022leaked\\u0022}",
        "Internal Message: 'leaked'",
        "internal-message: leaked",
        "Stack Trace: java.lang.X at com.foo.Bar(Bar.java:1)",
    ],
)
def test_filter_log_lines_drops_escaped_and_spaced_forms(line: str) -> None:
    """Escaped line breaks and quotes, and spaced key names, do not hide internal details."""
    assert cloud_mcp._filter_log_lines(["before", line, "after"]) == [  # noqa: SLF001
        "before",
        "after",
    ]


def test_section_error_text_hides_validation_error_input() -> None:
    """A model validation error is reported without echoing the rejected value."""
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        cloud_mcp.TroubleshootAttempt(
            attempt_number=cast(int, "leaked-value"), status="failed", created_at="x"
        )

    text = cloud_mcp._section_error_text(exc_info.value)  # noqa: SLF001

    assert text == "Unexpected API response."


def test_troubleshoot_bounds_organization_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """A huge organization ID is cut like every other free-text field."""
    organization = _healthy_organization()
    organization.organization_id = "o" * 100_000

    billing = _troubleshoot(
        monkeypatch, _TroubleshootConnection(), organization=organization
    ).billing

    assert billing.status is not None
    assert (
        len(billing.status.organization_id) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    )
    assert billing.status.organization_id.endswith(cloud_mcp.TRUNCATION_MARKER)


def test_troubleshoot_guidance_limits_reruns_to_active_unlocked_connections() -> None:
    """Guidance does not allow a rerun on a disabled connection or a locked account."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    assert "connection.status to be active" in guidance
    assert "billing.status.is_account_locked to be false" in guidance
    assert "do not enable it yourself" in guidance


def test_filter_log_lines_keeps_records_after_prefixed_internal_detail() -> None:
    """Lines that start a new log record are kept, whatever their prefix format."""
    lines = [
        "2026-01-01 12:00:00 source > ERROR failure stack_trace: leaked",
        "leaked-continuation",
        "2026-01-01 12:00:01 INFO i.a.w.Worker(run):123 - Source thread complete.",
        "2026-01-01 12:00:02 UTC source > password authentication failed",
        "[2026-01-01T12:00:03] platform summary",
    ]

    assert cloud_mcp._filter_log_lines(lines) == lines[2:]  # noqa: SLF001


def test_filter_log_lines_keeps_sibling_keys_of_internal_detail() -> None:
    """Only lines indented deeper than an internal-detail key continue its value."""
    lines = [
        "{",
        '  "stackTrace": [',
        '    "leaked.Frame(Frame.java:1)",',
        "  ],",
        '  "externalMessage": "Invalid credentials"',
        "}",
    ]

    kept = cloud_mcp._filter_log_lines(lines)  # noqa: SLF001

    assert kept == ["{", "  ],", '  "externalMessage": "Invalid credentials"', "}"]


@pytest.mark.parametrize("filler", ["\\", " ", '\\"'])
def test_bounded_message_handles_long_repetitive_lines(filler: str) -> None:
    """A long run of backslashes, spaces or escaped quotes is capped without stalling."""
    message, truncated = cloud_mcp._bounded_message(  # noqa: SLF001
        filler * 200_000 + "\n\tat io.airbyte.Foo.bar(Foo.java:12)"
    )

    assert message is not None
    assert len(message) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert message.endswith(
        cloud_mcp.TRUNCATION_MARKER + cloud_mcp.STACK_TRACE_REMOVED_MARKER
    )
    assert truncated is True


def test_troubleshoot_guidance_limits_cancel_to_confirmed_job() -> None:
    """Guidance only allows cancelling a specific job the user confirmed is stuck."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    assert "Only cancel after the user confirms" in guidance
    assert "pass that job's job_id" in guidance


def test_troubleshoot_missing_connection_status_is_not_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connection without a status is reported as unknown, not as disabled."""
    connection = _TroubleshootConnection(status=cast(str, None))

    section = _troubleshoot(monkeypatch, connection).connection

    assert section.status is None
    assert section.enabled is None
    assert section.error == "Connection status is missing from the API response."


def test_bounded_message_drops_traceback_started_on_internal_detail_line() -> None:
    """A traceback that starts on an internal-detail line is dropped with its exception line."""
    message, _ = cloud_mcp._bounded_message(  # noqa: SLF001
        "Check failed.\n"
        "stack_trace: Traceback (most recent call last):\n"
        '  File "x.py", line 3, in f\n'
        "KeyError: 'leaked'\n"
        "Check the host."
    )

    assert message == "Check failed.\nCheck the host. [stack trace removed]"


@pytest.mark.parametrize(
    "line",
    [
        '  "exception" : "java.lang.RuntimeException: leaked",',
        '  "stack" : "leaked.Frame(Frame.java:12)"',
        '{"log":"{\\n  \\"throwable\\": \\"leaked\\"\\n}"}',
    ],
)
def test_filter_log_lines_drops_pretty_printed_detail_keys(line: str) -> None:
    """Quoted detail keys are dropped at the start of a line and after an escaped line break."""
    assert cloud_mcp._filter_log_lines(["before", line, "after"]) == [  # noqa: SLF001
        "before",
        "after",
    ]


def test_filter_log_lines_keeps_record_after_prefixed_traceback_detail() -> None:
    """The log record after a traceback held in an internal-detail value is kept."""
    lines = [
        "2026-01-01 00:00:00 source > stack_trace: Traceback (most recent call last):",
        '  File "a.py", line 1, in f',
        "ValueError: leaked",
        "2026-01-01 00:00:01 source > Sync failed: check your API key",
    ]

    assert cloud_mcp._filter_log_lines(lines) == lines[3:]  # noqa: SLF001


def test_check_error_outcome_filters_platform_exception_header() -> None:
    """A platform check error drops the exception header along with its stack frames."""
    error = AirbyteError(
        message="Connector check did not complete.",
        context={
            "failure_origin": "airbyte_platform",
            "external_message": (
                "Workload launch failed.\n"
                "java.lang.IllegalStateException: leaked-host\n"
                "\tat io.airbyte.Foo.bar(Foo.java:12)"
            ),
        },
    )

    assert cloud_mcp._check_error_outcome(error) == (  # noqa: SLF001
        None,
        None,
        "Check did not complete (failure origin: airbyte_platform): "
        "Workload launch failed. [stack trace removed]",
    )


def test_troubleshoot_failed_token_request_is_not_a_missing_permission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 401 from the token endpoint is reported as such, not as a missing role."""
    response = requests.Response()
    response.status_code = 401
    response.url = "https://api.airbyte.com/v1/applications/token"
    error = requests.HTTPError(
        "Token request failed with status 401", response=response
    )

    billing = _troubleshoot(
        monkeypatch, _TroubleshootConnection(), organization=error
    ).billing

    assert billing.error == (
        "Organization lookup failed: Token request failed with status 401"
    )


def test_troubleshoot_guidance_classifies_connector_system_errors() -> None:
    """A connector system error is classified without needing a stack-trace marker."""
    guidance = " ".join(cloud_mcp.TROUBLESHOOT_CONNECTION_GUIDANCE.split())

    assert (
        "connector bug/regression: failure_type system_error from origin source or "
        "destination, especially" in guidance
    )
    assert "troubleshoot_cloud_connection and check_cloud_connector again" in guidance


def test_check_error_outcome_ignores_multiline_failure_origin() -> None:
    """A failure origin carrying more than a name is not rendered."""
    error = AirbyteError(
        message="Connector check did not complete.",
        context={
            "failure_origin": "platform\n\tat io.airbyte.Leaked.frame(Leaked.java:1)",
            "external_message": "Workload launch failed.",
        },
    )

    assert cloud_mcp._check_error_outcome(error) == (  # noqa: SLF001
        None,
        None,
        "Check did not complete (failure origin: unknown): Workload launch failed.",
    )
