# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte Cloud client.

## Organization and workspace resolution

Most operations need an organization and/or a workspace context. `CloudClient` derives
that context from what the caller supplies, falling back to the credentials' own scope
and finally to the authenticated user's organization memberships.

Two rules govern the whole protocol: an explicitly passed ID always beats an ambient
one, and within each of those tiers an organization ID beats a workspace ID.

### Where a workspace ID comes from

A workspace ID reaches the client from one of three places, in order:

1. The `workspace_id` argument on the operation, such as `CloudClient.get_workspace`.
2. The `X-Airbyte-Workspace-Id` header, when running as an MCP server over HTTP.
3. The `AIRBYTE_CLOUD_WORKSPACE_ID` (or `AIRBYTE_WORKSPACE_ID`) environment variable,
   read when the client is built with `CloudClient.from_auth(env_vars=True)`.

The last two become `CloudClient.default_workspace_id`, the ambient workspace context
for the client. Workspace-scoped operations use it whenever no `workspace_id` argument
is passed; `CloudClient.get_workspace` raises when neither is available, since an
organization can hold many workspaces and there is nothing to guess from.

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

`CloudClient.list_workspaces` falls back to the authenticated user's memberships: the
organizations that user holds permissions on, read once and cached for the life of the
client. `CloudClient.get_organization` called with no arguments resolves the same way:
configured `CloudClient.organization_id` first, then the parent organization of
`CloudClient.default_workspace_id`, then the memberships below.

- Exactly one membership — that organization is the context.
- Several memberships — discovery stops with a `PyAirbyteInputError` that both carries
  and enumerates in its message the first ten candidate organization IDs and names, so
  the caller can retry with one of them instead of having to list organizations first.
- No memberships — which happens for credentials whose grants are not
  organization-scoped — listing falls back to the cross-organization path below.

Passing `all_organizations=True` selects that path deliberately and cannot be combined
with an organization or workspace argument.

### Why the path matters

The two listing paths differ in completeness, not just speed:

- **Organization-scoped** (an organization was resolved) uses the Config API, which
  filters by name server-side and paginates, so results are complete and each workspace
  carries its organization attribution.
- **Cross-organization** (no organization resolved, or `all_organizations=True`) uses
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

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn, overload

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.models import CloudWorkspaceInfo
from airbyte.cloud.organizations import CloudOrganization
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteError, AirbyteMissingResourceError


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from airbyte.secrets.base import SecretString


MAX_ORGANIZATION_CANDIDATES = 10


@dataclass(init=False, kw_only=True)
class CloudClient:
    """Authenticated client for Airbyte Cloud and self-managed Airbyte APIs."""

    _credentials: _AirbyteCredentials
    _membership_organization_ids: tuple[str, ...] | None
    _authenticated_user_id: str | None
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
        resolved_workspace_id = workspace_id or self._credentials.workspace_id
        if not resolved_workspace_id:
            raise exc.PyAirbyteInputError(
                message="Workspace ID is required.",
                guidance="Provide a workspace ID.",
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
        all_organizations: bool = False,
    ) -> list[CloudWorkspaceInfo]:
        raise NotImplementedError

    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: str | None = None,
        organization_name: str | None = None,
        workspace_id: str | None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
        all_organizations: bool = False,
    ) -> list[CloudWorkspaceInfo]:
        """List workspaces available to this client.

        See the module docstring for how the organization context is resolved.
        """
        if limit is not None and limit <= 0:
            raise exc.PyAirbyteInputError(message="`limit` must be greater than 0.")
        if organization_id is not None and organization_name is not None:
            raise exc.PyAirbyteInputError(
                message="Provide either organization ID or organization name."
            )
        has_explicit_organization = organization_id is not None or organization_name is not None
        has_explicit_workspace = workspace_id is not None

        if all_organizations and (has_explicit_organization or has_explicit_workspace):
            raise exc.PyAirbyteInputError(
                message=(
                    "The all_organizations option cannot be combined with an "
                    "organization or workspace ID."
                )
            )
        if name_contains is not None and name_filter is not None:
            raise exc.PyAirbyteInputError(
                message="You can provide name_contains or name_filter, but not both."
            )
        if name is not None and name_contains is not None:
            raise exc.PyAirbyteInputError(
                message="You can provide name or name_contains, but not both."
            )
        resolved_organization_id = (
            None
            if all_organizations
            else self._resolve_workspace_organization_id(
                organization_id=organization_id,
                organization_name=organization_name,
                workspace_id=workspace_id,
            )
        )
        if resolved_organization_id is None:
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

        # The organization-scoped path delegates `name_contains` casing to the server.
        workspaces = api_util.list_workspaces_in_organization(
            organization_id=resolved_organization_id,
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self._get_config_api_bearer_token(),
            name_contains=name_contains or name,
            limit=None if name is not None or name_filter is not None else limit,
        )
        workspace_infos = [CloudWorkspaceInfo.from_mapping(workspace) for workspace in workspaces]
        if name is not None:
            workspace_infos = [workspace for workspace in workspace_infos if workspace.name == name]
        if name_filter is not None:
            workspace_infos = [
                workspace for workspace in workspace_infos if name_filter(workspace.name)
            ]
        if limit is not None and (name is not None or name_filter is not None):
            workspace_infos = workspace_infos[:limit]
        return workspace_infos

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

    def _get_authenticated_user_id(self) -> str:
        """Get and cache the Airbyte user ID for the current credentials."""
        if self._authenticated_user_id is not None:
            return self._authenticated_user_id

        bearer_token = self._get_config_api_bearer_token()
        if bearer_token is None:
            raise exc.PyAirbyteInputError(
                message="No authentication credentials provided.",
                guidance="Provide either client credentials or a bearer token.",
            )
        auth_user_id = api_util.get_user_id_from_bearer_token(bearer_token)
        user = api_util.get_user_by_auth_id(
            auth_user_id,
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=bearer_token,
        )
        user_id = user.get("userId")
        if not isinstance(user_id, str) or not user_id:
            raise exc.PyAirbyteInputError(
                message="The Airbyte user response did not include a user ID.",
                context={"response": user},
            )
        self._authenticated_user_id = user_id
        return self._authenticated_user_id

    def _get_membership_organization_ids(self) -> tuple[str, ...]:
        """Get and cache organization IDs from the caller's permissions."""
        if self._membership_organization_ids is not None:
            return self._membership_organization_ids

        permissions = api_util.list_permissions_for_user(
            self._get_authenticated_user_id(),
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self._get_config_api_bearer_token(),
        )
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
                "Multiple organization memberships were found for these credentials. "
                "Retry with one of these organization IDs "
                f"(showing {len(candidates)} of {len(organization_ids)}): {candidate_details}"
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
                message="Organization ID or organization name is required."
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
