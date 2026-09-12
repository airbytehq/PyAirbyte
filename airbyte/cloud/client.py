# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte Cloud client.

## Organization and workspace resolution

Most operations need an organization and/or a workspace context. `CloudClient` derives
that context from what the caller supplies, falling back to the credentials' own scope,
the authenticated user's default workspace, and finally to the authenticated user's
organization memberships.

Two rules govern the whole protocol: an explicitly passed ID always beats an ambient
one, and within each of those tiers an organization ID beats a workspace ID.

### Where a workspace ID comes from

A workspace ID reaches the client from one of four places, in order:

1. The `workspace_id` argument on the operation, such as `CloudClient.get_workspace`.
2. The `X-Airbyte-Workspace-Id` header, when running as an MCP server over HTTP.
3. The `AIRBYTE_CLOUD_WORKSPACE_ID` (or `AIRBYTE_WORKSPACE_ID`) environment variable,
   read when the client is built with `CloudClient.from_auth(env_vars=True)`.
4. The authenticated user's default workspace from their Airbyte user record.

The configured workspace becomes `CloudClient.default_workspace_id`, the ambient
workspace context for the client. Workspace-scoped operations use it, then the
authenticated user's default workspace, whenever no `workspace_id` argument is passed.
`CloudClient.get_workspace` raises when neither is available.

### If a workspace ID is known

The workspace determines the organization: its parent organization is fetched in a
single call and used as the organization context. That covers the common case, and
nothing below applies.

The one exception is an explicit `organization_id` or `organization_name` argument,
which always wins over a workspace-derived organization — as does a configured
`CloudClient.organization_id` over an *ambient* workspace.

### If an organization ID is known but no workspace ID

The organization is used as-is, whether it came from the `organization_id` argument,
from `organization_name` (an exact-name lookup, so it is never used to infer a
default), or from the credentials as `CloudClient.organization_id`.

### If neither is known

`CloudClient.list_workspaces` uses `privilege_scope` to choose the search breadth:
`MEMBER_OF` lists direct workspace grants, `ORGANIZATION_ADMIN` lists workspaces in
member organizations, `INSTANCE_ADMIN` lists every workspace for instance admins, and
`ANY` chooses the broadest scope available to the caller. `CloudClient.get_organization`
called with no arguments resolves the same way: configured `CloudClient.organization_id`
first, then the parent organization of `CloudClient.default_workspace_id`, then the
parent organization of the authenticated user's default workspace, then the memberships
below.

The deprecated `all_organizations=True` alias maps to `privilege_scope=ANY`.

### Why the path matters

The two listing paths differ in completeness, not just speed:

- **Organization-scoped** (an organization or membership scope was resolved) uses the
  Config API, which filters by name server-side and paginates, so results are complete
  and each workspace carries its organization attribution.
- **Cross-organization** (`privilege_scope=INSTANCE_ADMIN`, or `ANY` for an instance admin) uses
  the public API, which has neither an organization filter nor a name filter. Name
  matching happens client-side over every visible workspace, and the responses carry no
  organization attribution.

### Searching organizations

Organization search and limits are also server-side. `CloudClient.list_organizations`
uses the Config API whenever `name_contains` or `limit` is passed, which filters and
paginates on the server; with neither argument it uses the public API, which returns
every visible organization in a single request. `CloudClient.get_organization` fetches
one organization by ID directly, and searches by name through the Config API. These
organization lookup paths fall back to the public listing when the Config API is
unavailable, so self-managed deployments keep working.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn, overload

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.models import (
    CloudDefaultContextInfo,
    CloudOrganizationInfo,
    CloudWorkspaceInfo,
    WorkspacePrivilegeScope,
)
from airbyte.cloud.organizations import CloudOrganization
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteError, AirbyteMissingResourceError


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from airbyte.secrets.base import SecretString


MAX_ORGANIZATION_CANDIDATES = 10
MAX_MEMBER_WORKSPACES = 25
MAX_DEFAULT_WORKSPACE_CANDIDATES = 10


@dataclass(init=False, kw_only=True)
class CloudClient:
    """Authenticated client for Airbyte Cloud and self-managed Airbyte APIs."""

    _credentials: _AirbyteCredentials
    _membership_organization_ids: tuple[str, ...] | None
    _user_permissions: tuple[dict[str, Any], ...] | None
    _direct_workspace_infos: dict[str, CloudWorkspaceInfo | None]
    _workspace_organizations: dict[str, CloudOrganizationInfo | None]
    _authenticated_user_info: dict[str, Any] | None = field(repr=False)
    _authenticated_user_id: str | None = field(repr=False)
    _authenticated_bearer_token: SecretString | None

    def __init__(
        self,
        *,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
        workspace_id: str | None = None,
        organization_id: str | None = None,
    ) -> None:
        """Initialize a `CloudClient` from explicit auth values."""
        self._credentials = _AirbyteCredentials.from_auth(
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
            workspace_id=workspace_id,
            organization_id=organization_id,
            env_vars=False,
        )
        self._membership_organization_ids = None
        self._user_permissions = None
        self._direct_workspace_infos = {}
        self._workspace_organizations = {}
        self._authenticated_user_info = None
        self._authenticated_user_id = None
        self._authenticated_bearer_token = None

    @property
    def client_id(self) -> SecretString | None:
        """OAuth client ID used for authentication."""
        return self._credentials.client_id

    @property
    def client_secret(self) -> SecretString | None:
        """OAuth client secret used for authentication."""
        return self._credentials.client_secret

    @property
    def bearer_token(self) -> SecretString | None:
        """Bearer token used for authentication."""
        return self._credentials.bearer_token

    @property
    def public_api_root(self) -> str:
        """Airbyte Public API root."""
        return self._credentials.public_api_root

    @property
    def config_api_root(self) -> str | None:
        """Airbyte Config API root."""
        return self._credentials.config_api_root

    @property
    def organization_id(self) -> str | None:
        """Default organization ID for organization-scoped operations."""
        return self._credentials.organization_id

    @property
    def default_workspace_id(self) -> str | None:
        """Default workspace ID for workspace-scoped operations."""
        return self._credentials.workspace_id

    @classmethod
    def from_auth(
        cls,
        *,
        env_vars: bool = False,
        organization_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
    ) -> CloudClient:
        """Create a client from explicit inputs and optionally environment variables.

        When `env_vars` is True, environment variables are checked as a fallback
        after any explicitly provided values.
        """
        credentials = _AirbyteCredentials.from_auth(
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
            env_vars=env_vars,
        )
        return cls._from_credentials(credentials)

    @classmethod
    def _from_credentials(cls, credentials: _AirbyteCredentials) -> CloudClient:
        """Create a client from resolved Cloud credentials."""
        return cls(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            bearer_token=credentials.bearer_token,
            public_api_root=credentials.public_api_root,
            config_api_root=credentials.config_api_root,
            workspace_id=credentials.workspace_id,
            organization_id=credentials.organization_id,
        )

    def get_workspace(self, workspace_id: str | None = None) -> CloudWorkspace:
        """Create a `CloudWorkspace` using this client's credentials.

        See the module docstring for how the workspace is resolved.
        """
        resolved_workspace_id = workspace_id or self.resolve_default_workspace_id()
        if not resolved_workspace_id:
            raise exc.PyAirbyteInputError(
                message="Workspace ID is required.",
                guidance=(
                    "No workspace was configured, and no default workspace could be resolved "
                    "for the authenticated user. Provide a workspace ID, or call "
                    "`get_default_cloud_context` to discover your workspaces and organizations."
                ),
            )

        credentials = self._credentials.with_workspace_id(resolved_workspace_id)
        return CloudWorkspace(
            workspace_id=credentials.workspace_id,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            bearer_token=credentials.bearer_token,
            api_root=credentials.public_api_root,
            config_api_root=credentials.config_api_root,
        )

    def create_workspace(
        self,
        *,
        name: str,
        organization_id: str | None = None,
        region_id: str | None = None,
    ) -> CloudWorkspaceInfo:
        """Create an Airbyte workspace."""
        resolved_organization_id = organization_id or self.organization_id
        workspace = api_util.create_workspace(
            name=name,
            organization_id=resolved_organization_id,
            region_id=region_id,
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return CloudWorkspaceInfo.from_api_response(workspace)

    def rename_workspace(
        self,
        workspace_id: str,
        *,
        name: str,
    ) -> CloudWorkspaceInfo:
        """Rename an Airbyte workspace."""
        workspace = api_util.rename_workspace(
            workspace_id=workspace_id,
            name=name,
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return CloudWorkspaceInfo.from_api_response(workspace)

    def permanently_delete_workspace(
        self,
        workspace_id: str,
        *,
        workspace_name: str | None = None,
        safe_mode: bool = True,
    ) -> None:
        """Permanently delete an Airbyte workspace if it has no connections.

        When `safe_mode` is enabled, the workspace name must contain `delete-me`
        or `deleteme`. This also checks for existing connections before deleting
        and raises `AirbyteWorkspaceNotEmptyError` if the workspace is not empty.
        """
        api_util.permanently_delete_workspace(
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

    @overload
    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: None = None,
        organization_name: str | None = None,
        workspace_id: str | None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
        privilege_scope: WorkspacePrivilegeScope = WorkspacePrivilegeScope.MEMBER_OF,
        all_organizations: bool = False,
    ) -> list[CloudWorkspaceInfo]:
        raise NotImplementedError

    @overload
    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: str,
        organization_name: str | None = None,
        workspace_id: str | None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
        privilege_scope: WorkspacePrivilegeScope = WorkspacePrivilegeScope.MEMBER_OF,
        all_organizations: bool = False,
    ) -> list[CloudWorkspaceInfo]:
        raise NotImplementedError

    def list_workspaces(  # noqa: PLR0911, PLR0913
        self,
        name: str | None = None,
        *,
        organization_id: str | None = None,
        organization_name: str | None = None,
        workspace_id: str | None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
        privilege_scope: WorkspacePrivilegeScope = WorkspacePrivilegeScope.MEMBER_OF,
        all_organizations: bool = False,
    ) -> list[CloudWorkspaceInfo]:
        """List workspaces available to this client.

        `privilege_scope` controls whether this lists direct member workspaces,
        organization workspaces, or instance-wide workspaces. The deprecated
        `all_organizations` alias maps to `WorkspacePrivilegeScope.ANY`.
        """
        if limit is not None and limit <= 0:
            raise exc.PyAirbyteInputError(message="`limit` must be greater than 0.")
        if organization_id is not None and organization_name is not None:
            raise exc.PyAirbyteInputError(
                message="Provide either organization ID or organization name."
            )
        has_explicit_organization = organization_id is not None or organization_name is not None
        has_explicit_workspace = workspace_id is not None

        if all_organizations:
            if privilege_scope is not WorkspacePrivilegeScope.MEMBER_OF:
                raise exc.PyAirbyteInputError(
                    message="all_organizations cannot be combined with privilege_scope."
                )
            warnings.warn(
                "`all_organizations` is deprecated; use `privilege_scope` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            privilege_scope = WorkspacePrivilegeScope.ANY
        if name_contains is not None and name_filter is not None:
            raise exc.PyAirbyteInputError(
                message="You can provide name_contains or name_filter, but not both."
            )
        if name is not None and name_contains is not None:
            raise exc.PyAirbyteInputError(
                message="You can provide name or name_contains, but not both."
            )
        if has_explicit_organization or has_explicit_workspace:
            resolved_organization_id = self._resolve_workspace_organization_id(
                organization_id=organization_id,
                organization_name=organization_name,
                workspace_id=workspace_id,
            )
            if resolved_organization_id is None:
                return []
            return self._list_workspaces_in_organizations(
                (resolved_organization_id,),
                name=name,
                name_contains=name_contains,
                name_filter=name_filter,
                limit=limit,
            )

        if privilege_scope is WorkspacePrivilegeScope.MEMBER_OF:
            return self._list_member_workspaces(
                name=name,
                name_contains=name_contains,
                name_filter=name_filter,
                limit=limit,
            )

        if privilege_scope is WorkspacePrivilegeScope.INSTANCE_ADMIN:
            if not self._is_instance_admin():
                raise exc.PyAirbyteInputError(
                    message="privilege_scope=instance_admin requires the instance_admin permission."
                )
            return self._list_unscoped_workspaces(
                name=name,
                name_contains=name_contains,
                name_filter=name_filter,
                limit=limit,
            )

        if privilege_scope is WorkspacePrivilegeScope.ANY and self._is_instance_admin():
            return self._list_unscoped_workspaces(
                name=name,
                name_contains=name_contains,
                name_filter=name_filter,
                limit=limit,
            )

        if privilege_scope in {
            WorkspacePrivilegeScope.ORGANIZATION_ADMIN,
            WorkspacePrivilegeScope.ANY,
        }:
            organization_ids = self._get_membership_organization_ids()
            if not organization_ids:
                return []
            return self._list_workspaces_in_organizations(
                organization_ids,
                name=name,
                name_contains=name_contains,
                name_filter=name_filter,
                limit=limit,
            )

        raise exc.PyAirbyteInputError(message="Unsupported workspace privilege scope.")

    def _list_member_workspaces(
        self,
        *,
        name: str | None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
    ) -> list[CloudWorkspaceInfo]:
        """List workspaces granted directly to the authenticated user."""
        workspace_ids = self._get_direct_workspace_ids()
        workspaces: list[CloudWorkspaceInfo] = []
        name_substring = name_contains.casefold() if name_contains is not None else None
        for direct_workspace_id in workspace_ids:
            workspace = self._get_direct_workspace_info(direct_workspace_id)
            if workspace is None:
                continue
            if name is not None and workspace.name != name:
                continue
            if name_substring is not None and name_substring not in workspace.name.casefold():
                continue
            if name_filter is not None and not name_filter(workspace.name):
                continue
            workspaces.append(workspace)
            if limit is not None and len(workspaces) == limit:
                break
        return workspaces

    def _list_unscoped_workspaces(
        self,
        *,
        name: str | None,
        name_contains: str | None,
        name_filter: Callable[[str], bool] | None,
        limit: int | None,
    ) -> list[CloudWorkspaceInfo]:
        """List workspaces across the instance."""
        if name_contains is not None:
            name_substring = name_contains.casefold()

            def matches_name(workspace_name: str) -> bool:
                return name_substring in workspace_name.casefold()

            name_filter = matches_name
            name = None
        workspaces = api_util.list_workspaces(
            workspace_id="",
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            name_filter=name_filter,
            name=name,
            limit=limit,
        )
        return [CloudWorkspaceInfo.from_api_response(workspace) for workspace in workspaces]

    def _list_workspaces_in_organizations(
        self,
        organization_ids: tuple[str, ...],
        *,
        name: str | None,
        name_contains: str | None,
        name_filter: Callable[[str], bool] | None,
        limit: int | None,
    ) -> list[CloudWorkspaceInfo]:
        """List and combine workspaces from one or more organizations."""
        workspace_infos: list[CloudWorkspaceInfo] = []
        for organization_id in organization_ids:
            remaining_limit = None if limit is None else limit - len(workspace_infos)
            if remaining_limit == 0:
                break
            workspaces = api_util.list_workspaces_in_organization(
                organization_id=organization_id,
                api_root=self.public_api_root,
                config_api_root=self.config_api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self._get_config_api_bearer_token(),
                name_contains=name_contains or name,
                limit=None if name is not None or name_filter is not None else remaining_limit,
            )
            organization_workspaces = [
                CloudWorkspaceInfo.from_mapping(workspace) for workspace in workspaces
            ]
            if name is not None:
                organization_workspaces = [
                    workspace for workspace in organization_workspaces if workspace.name == name
                ]
            if name_filter is not None:
                organization_workspaces = [
                    workspace
                    for workspace in organization_workspaces
                    if name_filter(workspace.name)
                ]
            workspace_infos.extend(organization_workspaces)
            if limit is not None and len(workspace_infos) >= limit:
                break
        return workspace_infos[:limit] if limit is not None else workspace_infos

    def _resolve_workspace_organization_id(
        self,
        *,
        organization_id: str | None,
        organization_name: str | None,
        workspace_id: str | None,
    ) -> str | None:
        """Resolve the organization for a workspace listing."""
        if organization_id is not None or organization_name is not None:
            if organization_id is not None:
                return organization_id
            # Do not use explicit name lookup to infer a default organization.
            return self.get_organization(organization_name=organization_name).organization_id

        if workspace_id is not None:
            return self._get_workspace_parent_organization_id(workspace_id)

        return self._resolve_ambient_organization_id()

    def _resolve_ambient_organization_id(self) -> str | None:
        """Resolve an organization from configured client context or memberships."""
        if self.organization_id is not None:
            return self.organization_id
        if self.default_workspace_id is not None:
            try:
                return self._get_workspace_parent_organization_id(self.default_workspace_id)
            except (exc.AirbyteError, exc.PyAirbyteInputError):
                pass
        user_default_workspace_id = self._get_user_default_workspace_id()
        if user_default_workspace_id:
            try:
                return self._get_workspace_parent_organization_id(user_default_workspace_id)
            except (exc.AirbyteError, exc.PyAirbyteInputError):
                pass

        try:
            organization_ids = self._get_membership_organization_ids()
        except (exc.AirbyteError, exc.PyAirbyteInputError):
            return None
        if len(organization_ids) > 1:
            self._raise_ambiguous_organization_error(organization_ids)
        return organization_ids[0] if organization_ids else None

    def _get_config_api_bearer_token(self) -> SecretString | None:
        """Get and cache a bearer token for Config API requests."""
        if self._authenticated_bearer_token is not None:
            return self._authenticated_bearer_token
        if self.bearer_token is not None:
            self._authenticated_bearer_token = self.bearer_token
        elif self.client_id is not None and self.client_secret is not None:
            self._authenticated_bearer_token = api_util.get_bearer_token(
                client_id=self.client_id,
                client_secret=self.client_secret,
                api_root=self.public_api_root,
            )
        return self._authenticated_bearer_token

    def _get_workspace_parent_organization_id(self, workspace_id: str) -> str:
        """Resolve a workspace's parent organization ID."""
        organization = api_util.get_workspace_organization_info(
            workspace_id=workspace_id,
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self._get_config_api_bearer_token(),
        )
        resolved_organization_id = organization.get("organizationId")
        if isinstance(resolved_organization_id, str) and resolved_organization_id:
            return resolved_organization_id
        raise exc.PyAirbyteInputError(
            message="The workspace response did not include an organization ID.",
            context={"workspace_id": workspace_id, "response": organization},
        )

    def get_workspace_parent_organization_id(self, workspace_id: str) -> str | None:
        """Return the parent organization ID of a workspace, or `None` if it cannot be resolved."""
        try:
            return self._get_workspace_parent_organization_id(workspace_id)
        except (exc.AirbyteError, exc.PyAirbyteInputError):
            return None

    def _get_authenticated_user_info(self) -> dict[str, Any]:
        """Get and cache the Airbyte user record for the current credentials."""
        if self._authenticated_user_info is not None:
            return self._authenticated_user_info

        bearer_token = self._get_config_api_bearer_token()
        if bearer_token is None:
            raise exc.PyAirbyteInputError(
                message="No authentication credentials provided.",
                guidance="Provide either client credentials or a bearer token.",
            )
        auth_user_id = api_util.get_user_id_from_bearer_token(bearer_token)
        self._authenticated_user_info = api_util.get_user_by_auth_id(
            auth_user_id,
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=bearer_token,
        )
        return self._authenticated_user_info

    def _get_authenticated_user_id(self) -> str:
        """Get and cache the Airbyte user ID for the current credentials."""
        if self._authenticated_user_id is not None:
            return self._authenticated_user_id

        user = self._get_authenticated_user_info()
        user_id = user.get("userId")
        if not isinstance(user_id, str) or not user_id:
            raise exc.PyAirbyteInputError(
                message="The Airbyte user response did not include a user ID.",
                context={"response": user},
            )
        self._authenticated_user_id = user_id
        return self._authenticated_user_id

    def _get_user_default_workspace_id(self) -> str | None:
        """Get the authenticated user's default workspace ID, when available."""
        try:
            default_workspace_id = self._get_authenticated_user_info().get("defaultWorkspaceId")
        except (exc.AirbyteError, exc.PyAirbyteInputError):
            return None
        return (
            default_workspace_id
            if isinstance(default_workspace_id, str) and default_workspace_id
            else None
        )

    def resolve_default_workspace_id(self) -> str | None:
        """Resolve the configured or authenticated user's default workspace ID."""
        configured_workspace_id = self.default_workspace_id
        if configured_workspace_id:
            return configured_workspace_id
        user_default_workspace_id = self._get_user_default_workspace_id()
        if user_default_workspace_id:
            return user_default_workspace_id
        try:
            direct_workspace_ids = self._get_direct_workspace_ids()
        except (AirbyteError, exc.PyAirbyteInputError):
            return None
        if len(direct_workspace_ids) > MAX_DEFAULT_WORKSPACE_CANDIDATES:
            return None
        try:
            live_workspace_ids = [
                workspace_id
                for workspace_id in direct_workspace_ids
                if self._get_direct_workspace_info(workspace_id) is not None
            ]
        except (AirbyteError, exc.PyAirbyteInputError):
            return None
        return live_workspace_ids[0] if len(live_workspace_ids) == 1 else None

    def _get_user_permissions(self) -> tuple[dict[str, Any], ...]:
        """Get and cache permissions for the authenticated user."""
        if self._user_permissions is None:
            self._user_permissions = tuple(
                permission
                for permission in api_util.list_permissions_for_user(
                    self._get_authenticated_user_id(),
                    api_root=self.public_api_root,
                    config_api_root=self.config_api_root,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    bearer_token=self._get_config_api_bearer_token(),
                )
                if isinstance(permission, dict)
            )
        return self._user_permissions

    def _get_membership_organization_ids(self) -> tuple[str, ...]:
        """Get and cache organization IDs from the caller's permissions."""
        if self._membership_organization_ids is not None:
            return self._membership_organization_ids

        permissions = self._get_user_permissions()
        organization_ids: list[str] = []
        for permission in permissions:
            permission_organization_id = permission.get("organizationId")
            if (
                isinstance(permission_organization_id, str)
                and permission_organization_id
                and permission_organization_id not in organization_ids
            ):
                organization_ids.append(permission_organization_id)
        self._membership_organization_ids = tuple(organization_ids)
        return self._membership_organization_ids

    def _get_direct_workspace_ids(self) -> tuple[str, ...]:
        """Get unique workspace IDs from the caller's direct permissions."""
        workspace_ids: list[str] = []
        for permission in self._get_user_permissions():
            workspace_id = permission.get("workspaceId")
            if isinstance(workspace_id, str) and workspace_id and workspace_id not in workspace_ids:
                workspace_ids.append(workspace_id)
        return tuple(workspace_ids)

    def _get_direct_workspace_info(self, workspace_id: str) -> CloudWorkspaceInfo | None:
        """Fetch a directly granted workspace, or `None` if the grant is stale (404)."""
        if workspace_id in self._direct_workspace_infos:
            return self._direct_workspace_infos[workspace_id]
        try:
            workspace = api_util.get_workspace(
                workspace_id=workspace_id,
                api_root=self.public_api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
            )
        except exc.AirbyteMissingResourceError:
            self._direct_workspace_infos[workspace_id] = None
            return None
        workspace_info = CloudWorkspaceInfo.from_api_response(workspace)
        self._direct_workspace_infos[workspace_id] = workspace_info
        return workspace_info

    def _get_workspace_organization(self, workspace_id: str) -> CloudOrganizationInfo | None:
        """Fetch and cache organization info for a workspace."""
        if workspace_id in self._workspace_organizations:
            return self._workspace_organizations[workspace_id]
        try:
            organization = api_util.get_workspace_organization_info(
                workspace_id=workspace_id,
                api_root=self.public_api_root,
                config_api_root=self.config_api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self._get_config_api_bearer_token(),
            )
        except (AirbyteError, NotImplementedError):
            # The workspace is readable via the public API but its organization is not
            # (e.g. the caller lacks org-level read, or no Config API root can be derived
            # from a custom public API root). Keep the live workspace and leave the
            # organization unknown.
            return None
        organization_id = organization.get("organizationId")
        if not isinstance(organization_id, str) or not organization_id:
            return None
        organization_info = CloudOrganizationInfo(
            organization_id=organization_id,
            organization_name=(
                organization.get("organizationName")
                if isinstance(organization.get("organizationName"), str)
                else None
            ),
        )
        self._workspace_organizations[workspace_id] = organization_info
        return organization_info

    def _is_instance_admin(self) -> bool:
        """Return whether the caller has an instance-admin permission."""
        return any(
            permission.get("permissionType") == "instance_admin"
            for permission in self._get_user_permissions()
        )

    def get_default_context_for_user(self) -> CloudDefaultContextInfo:
        """Describe the authenticated user's explicit Cloud affinities."""
        user_id: str | None = None
        user_name: str | None = None
        user_email: str | None = None
        try:
            user = self._get_authenticated_user_info()
        except (AirbyteError, exc.PyAirbyteInputError):
            pass
        else:
            user_id = user.get("userId") if isinstance(user.get("userId"), str) else None
            user_name = user.get("name") if isinstance(user.get("name"), str) else None
            user_email = user.get("email") if isinstance(user.get("email"), str) else None

        default_workspace_id = self.resolve_default_workspace_id()
        try:
            permissions = self._get_user_permissions()
        except (AirbyteError, exc.PyAirbyteInputError):
            permissions = ()
            membership_organization_ids = ()
            member_workspaces = []
            member_organizations_truncated = False
            member_workspaces_truncated = False
        else:
            membership_organization_ids = self._get_membership_organization_ids()
            member_organizations_truncated = (
                len(membership_organization_ids) > MAX_ORGANIZATION_CANDIDATES
            )
            member_workspaces_truncated = (
                len(self._get_direct_workspace_ids()) > MAX_MEMBER_WORKSPACES
            )
            try:
                member_workspaces = self.list_workspaces(
                    privilege_scope=WorkspacePrivilegeScope.MEMBER_OF,
                    limit=MAX_MEMBER_WORKSPACES,
                )
            except (AirbyteError, exc.PyAirbyteInputError):
                member_workspaces = []

        member_organizations = [
            CloudOrganizationInfo.model_validate(candidate)
            for candidate in self._get_organization_candidates(
                membership_organization_ids[:MAX_ORGANIZATION_CANDIDATES]
            )
        ]
        default_workspace_info: CloudWorkspaceInfo | None = None
        default_workspace_organization: CloudOrganizationInfo | None = None
        if default_workspace_id is not None:
            try:
                default_workspace_info = self._get_direct_workspace_info(default_workspace_id)
            except (AirbyteError, exc.PyAirbyteInputError):
                default_workspace_info = None
            if default_workspace_info is not None:
                default_workspace_organization = self._get_workspace_organization(
                    default_workspace_id
                )
            # The default workspace's organization may not be an explicit membership.
            if default_workspace_organization is not None and all(
                organization.organization_id != default_workspace_organization.organization_id
                for organization in member_organizations
            ):
                member_organizations.append(default_workspace_organization)
        discovery_hints: list[str] = []
        if any(permission.get("permissionType") == "instance_admin" for permission in permissions):
            discovery_hints.append(
                "Instance-admin access may include every organization and workspace in the "
                "instance. Use list_cloud_organizations(name_contains=...) or "
                "list_cloud_workspaces(organization_id=...) to discover others."
            )
        if membership_organization_ids:
            discovery_hints.append(
                "Organization membership grants access to every workspace in those "
                "organizations. Use list_cloud_workspaces(organization_id=<id>) to "
                "discover workspaces."
            )
        return CloudDefaultContextInfo(
            user_id=user_id,
            user_name=user_name,
            user_email=user_email,
            default_workspace_id=default_workspace_id,
            default_workspace_name=(
                default_workspace_info.name if default_workspace_info else None
            ),
            default_organization_id=(
                default_workspace_organization.organization_id
                if default_workspace_organization is not None
                else None
            ),
            default_organization_name=(
                default_workspace_organization.organization_name
                if default_workspace_organization is not None
                else None
            ),
            configured_workspace_id=self.default_workspace_id,
            configured_organization_id=self.organization_id,
            member_organizations=member_organizations,
            member_workspaces=member_workspaces,
            member_organizations_truncated=member_organizations_truncated,
            member_workspaces_truncated=member_workspaces_truncated,
            discovery_hints=discovery_hints,
        )

    def _get_organization_candidates(
        self,
        organization_ids: tuple[str, ...],
    ) -> list[dict[str, str | None]]:
        """Get names for membership-derived organization candidates."""
        candidates: list[dict[str, str | None]] = []
        for organization_id in organization_ids:
            organization_name = None
            try:
                organization_info = api_util.get_organization_info(
                    organization_id=organization_id,
                    api_root=self.public_api_root,
                    config_api_root=self.config_api_root,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    bearer_token=self._get_config_api_bearer_token(),
                )
            except AirbyteError:
                pass
            else:
                candidate_name = organization_info.get("organizationName")
                if isinstance(candidate_name, str):
                    organization_name = candidate_name
            candidates.append(
                {
                    "organization_id": organization_id,
                    "organization_name": organization_name,
                }
            )
        return candidates

    def _raise_ambiguous_organization_error(
        self,
        organization_ids: tuple[str, ...],
    ) -> NoReturn:
        """Raise an error enumerating the caller's candidate organizations."""
        candidates = self._get_organization_candidates(
            organization_ids[:MAX_ORGANIZATION_CANDIDATES]
        )
        candidate_details = ", ".join(
            f"{candidate['organization_id']} "
            f"({candidate['organization_name'] or 'name unavailable'})"
            for candidate in candidates
        )
        raise exc.PyAirbyteInputError(
            message=(
                "Multiple organization memberships were found for these credentials. Retry "
                "with one of these "
                "organization IDs "
                f"(showing {len(candidates)} of {len(organization_ids)}): {candidate_details}. "
                "Call `get_default_cloud_context` to see your memberships."
            ),
            context={
                "organization_ids": list(organization_ids),
                "organization_candidates": candidates,
                "total_candidates": len(organization_ids),
            },
        )

    def list_organizations(
        self,
        *,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> list[CloudOrganization]:
        """List organizations available to this client.

        See the module docstring for how organization search and limits are resolved.
        """
        if limit is not None and limit <= 0:
            raise exc.PyAirbyteInputError(message="`limit` must be greater than 0.")

        if name_contains is not None or limit is not None:
            try:
                return self._list_organizations_by_user_id(
                    name_contains=name_contains,
                    limit=limit,
                )
            except AirbyteError:
                pass

        organizations = self._fetch_organizations()
        if name_contains is not None:
            name_substring = name_contains.casefold()
            organizations = [
                organization
                for organization in organizations
                if name_substring in (organization.organization_name or "").casefold()
            ]
        return organizations if limit is None else organizations[:limit]

    def _list_organizations_by_user_id(
        self,
        *,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> list[CloudOrganization]:
        """List organizations via the Config API, with server-side search and paging."""
        user_id = self._get_authenticated_user_id()
        return [
            self._organization_from_mapping(organization)
            for organization in api_util.list_organizations_for_user_id(
                user_id=user_id,
                api_root=self.public_api_root,
                config_api_root=self.config_api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self._get_config_api_bearer_token(),
                name_contains=name_contains,
                limit=limit,
            )
        ]

    def _organization_from_mapping(
        self,
        organization: Mapping[str, Any],
    ) -> CloudOrganization:
        """Build a `CloudOrganization` from a Config API organization mapping."""
        return CloudOrganization(
            organization_id=organization["organizationId"],
            organization_name=organization.get("organizationName"),
            email=organization.get("email"),
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            public_api_root=self.public_api_root,
            config_api_root=self.config_api_root,
        )

    def _fetch_organizations(self) -> list[CloudOrganization]:
        """Fetch all organizations available to this client."""
        return [
            CloudOrganization(
                organization_id=organization.organization_id,
                organization_name=organization.organization_name,
                email=organization.email,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
                public_api_root=self.public_api_root,
                config_api_root=self.config_api_root,
            )
            for organization in api_util.list_organizations_for_user(
                api_root=self.public_api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
            )
        ]

    def _resolve_default_organization_id(self) -> str | None:
        """Resolve the organization to use when no organization argument is given."""
        return self._resolve_ambient_organization_id()

    def _get_organization_by_id(self, organization_id: str) -> CloudOrganization | None:
        """Look up a single organization via the Config API, if available."""
        try:
            organization_info = api_util.get_organization_info(
                organization_id=organization_id,
                api_root=self.public_api_root,
                config_api_root=self.config_api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self._get_config_api_bearer_token(),
            )
        except AirbyteError:
            return None
        if not isinstance(organization_info.get("organizationId"), str):
            return None
        return self._organization_from_mapping(organization_info)

    def _search_organizations_by_name(
        self,
        organization_name: str | None,
    ) -> list[CloudOrganization]:
        """Get organizations whose names contain `organization_name`, if available."""
        if organization_name is not None:
            try:
                return self._list_organizations_by_user_id(name_contains=organization_name)
            except AirbyteError:
                pass
        return self._fetch_organizations()

    def get_organization(
        self,
        organization_id: str | None = None,
        *,
        organization_name: str | None = None,
    ) -> CloudOrganization:
        """Resolve an organization by ID or exact name.

        See the module docstring for how the organization is resolved when no
        argument is given.
        """
        resolved_organization_id = organization_id
        if resolved_organization_id and organization_name:
            raise exc.PyAirbyteInputError(
                message="Provide either organization ID or organization name."
            )
        if resolved_organization_id is None and organization_name is None:
            resolved_organization_id = self._resolve_default_organization_id()
        if not resolved_organization_id and not organization_name:
            raise exc.PyAirbyteInputError(
                message="Organization ID or organization name is required.",
                guidance=(
                    "Provide an organization ID or name, or call `get_default_cloud_context` "
                    "to discover your organizations."
                ),
            )

        if resolved_organization_id:
            organization = self._get_organization_by_id(resolved_organization_id)
            if organization is not None:
                return organization
            matching_organizations = [
                candidate
                for candidate in self._fetch_organizations()
                if candidate.organization_id == resolved_organization_id
            ]
        else:
            matching_organizations = [
                candidate
                for candidate in self._search_organizations_by_name(organization_name)
                if candidate.organization_name == organization_name
            ]

        if not matching_organizations:
            raise AirbyteMissingResourceError(
                resource_type="organization",
                resource_name_or_id=resolved_organization_id or organization_name,
            )
        if len(matching_organizations) > 1:
            total_matches = len(matching_organizations)
            shown_matches = matching_organizations[:10]
            match_details = ", ".join(
                f"{organization.organization_id} ({organization.email or 'email unavailable'})"
                for organization in shown_matches
            )
            raise exc.PyAirbyteInputError(
                message=(
                    "Organization name matches multiple organizations. Provide an "
                    f"organization ID to disambiguate. Matching organizations "
                    f"(showing {len(shown_matches)} of {total_matches}): {match_details}"
                ),
                context={
                    "organization_name": organization_name,
                    "matching_organizations": [
                        {
                            "organization_id": organization.organization_id,
                            "email": organization.email,
                        }
                        for organization in shown_matches
                    ],
                    "total_matches": total_matches,
                },
            )

        return matching_organizations[0]
