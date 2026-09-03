# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents workspaces.

> ## ⚠️ Experimental Interface
>
> **The Airbyte Agents Python interfaces are experimental.** Class names, method signatures,
> and result models may change or be removed without notice between minor versions of
> PyAirbyte. Pin an exact PyAirbyte version if you depend on them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.agents import _api_util
from airbyte.agents.connectors import AgentConnector, _resolve_connector_lookup
from airbyte.agents.models import AgentConnectorInfo, AgentWorkspaceInfo
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteError, PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte.secrets.base import SecretString


class AgentWorkspace:
    """A workspace on the Airbyte Agents platform.

    Airbyte Cloud credentials authenticate against the Agents API, so this class reads the
    same `AIRBYTE_CLOUD_*` environment variables as `airbyte.cloud.CloudWorkspace`.

    ```python
    from airbyte import agents

    workspace = agents.AgentWorkspace.from_env()
    for connector in workspace.list_connectors():
        print(connector.name)
    ```
    """

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        organization_id: str | None = None,
        name: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
    ) -> None:
        """Initialize an `AgentWorkspace`.

        Credentials fall back to the `AIRBYTE_CLOUD_*` environment variables when they are
        not passed explicitly.
        """
        credentials = _AirbyteCredentials.from_auth(
            workspace_id=workspace_id,
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            # Mirrors `CloudWorkspace.__init__`: any explicit credential disables env
            # fallback, since an env bearer token plus explicit client creds is rejected
            # as mutually exclusive auth.
            env_vars=not (client_id or client_secret or bearer_token),
        )
        if not credentials.workspace_id:
            raise PyAirbyteInputError(
                message="Workspace ID is required.",
                guidance=(
                    "Provide `workspace_id`, or set the `AIRBYTE_CLOUD_WORKSPACE_ID` "
                    "environment variable."
                ),
            )

        self._credentials = credentials

        self.workspace_id: str = credentials.workspace_id
        """The workspace ID."""

        self.organization_id: str | None = credentials.organization_id
        """The organization ID, sent to the Agents API when it is known."""

        self.name: str | None = name
        """The workspace name, when known. Use `get_info()` to fetch it from the API."""

    @classmethod
    def from_env(
        cls,
        workspace_id: str | None = None,
        *,
        organization_id: str | None = None,
    ) -> AgentWorkspace:
        """Create an `AgentWorkspace` from the `AIRBYTE_CLOUD_*` environment variables.

        The variables used are `AIRBYTE_CLOUD_BEARER_TOKEN` or the
        `AIRBYTE_CLOUD_CLIENT_ID` and `AIRBYTE_CLOUD_CLIENT_SECRET` pair, along with
        `AIRBYTE_CLOUD_WORKSPACE_ID` and `AIRBYTE_CLOUD_ORGANIZATION_ID`.
        """
        return cls(workspace_id=workspace_id, organization_id=organization_id)

    def get_info(self) -> AgentWorkspaceInfo:
        """Fetch this workspace from the Agents API.

        A successful call is authoritative proof that the workspace is reachable through
        the Agents API with these credentials.
        """
        return AgentWorkspaceInfo.model_validate(
            _api_util.get_agent_workspace(
                workspace_id=self.workspace_id,
                credentials=self._credentials,
                organization_id=self.organization_id,
            )
        )

    def list_connectors(self) -> list[AgentConnector]:
        """List the connectors configured in this workspace."""
        return [
            AgentConnector(
                connector_id=info.id,
                name=info.name,
                credentials=self._credentials,
            )
            for info in (
                AgentConnectorInfo.model_validate(record)
                for record in _api_util.list_agent_connectors(
                    workspace_id=self.workspace_id,
                    credentials=self._credentials,
                    organization_id=self.organization_id,
                )
            )
        ]

    def get_connector(
        self,
        id_or_name: str | None = None,
        /,
        *,
        id: str | None = None,  # noqa: A002  # Shadows `id` deliberately, as a short alias.
        connector_id: str | None = None,
        name: str | None = None,
    ) -> AgentConnector:
        """Get a connector in this workspace, by ID or by name.

        Pass a single positional value to look the connector up by either its ID or its
        name, or name the argument to be explicit: `id` and `connector_id` are synonyms,
        so pass whichever reads better.

        Lookup by an explicit ID does not call the Agents API. Every other form lists the
        workspace's connectors and matches on ID first, then on an exact name, then on a
        unique substring, so `name="GitHub"` finds a connector named
        `GitHub - <workspace_id>`. Name matching is case-insensitive.
        """
        lookup = _resolve_connector_lookup(
            id_or_name,
            id=id,
            connector_id=connector_id,
            name=name,
        )

        if lookup.connector_id and not lookup.name:
            return AgentConnector(connector_id=lookup.connector_id, credentials=self._credentials)

        connectors = self.list_connectors()
        if lookup.connector_id:
            id_matches = [
                connector
                for connector in connectors
                if connector.connector_id == lookup.connector_id
            ]
            if id_matches:
                return id_matches[0]

        name_lower = (lookup.name or "").lower()
        matches = [
            connector
            for connector in connectors
            if connector.name and connector.name.lower() == name_lower
        ] or [
            connector
            for connector in connectors
            if connector.name and name_lower in connector.name.lower()
        ]
        if not matches:
            raise AirbyteError(
                message="No connector found with the given ID or name.",
                guidance="Use `list_connectors()` to see the available connectors.",
                context={"lookup": lookup.name, "workspace_id": self.workspace_id},
            )
        if len(matches) > 1:
            raise AirbyteError(
                message="Multiple connectors matched the given name.",
                guidance="Pass `connector_id`, or a name that matches only one connector.",
                context={
                    "name": lookup.name,
                    "matched_names": [connector.name for connector in matches],
                },
            )
        return matches[0]

    def as_cloud_workspace(self) -> CloudWorkspace:
        """Return this workspace as an `airbyte.cloud.CloudWorkspace`.

        Every Agents workspace is also a Cloud workspace, so this conversion always
        succeeds without calling either API.
        """
        return CloudWorkspace(
            workspace_id=self.workspace_id,
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            bearer_token=self._credentials.bearer_token,
            api_root=self._credentials.public_api_root,
            config_api_root=self._credentials.config_api_root,
        )

    @classmethod
    def from_cloud_workspace(
        cls,
        cloud_workspace: CloudWorkspace,
        *,
        organization_id: str | None = None,
        verify: bool = True,
    ) -> AgentWorkspace:
        """Return a Cloud workspace as an `AgentWorkspace`.

        Cloud workspace IDs are also Agents workspace IDs, but not every Cloud workspace is
        reachable through the Agents API: the organization needs an Airbyte Agents
        subscription. By default this is verified by fetching the workspace from the Agents
        API, which raises `AirbyteError` when it is not eligible. Pass `verify=False` to
        skip that call.

        Raises `PyAirbyteInputError` when the Cloud workspace uses non-public Cloud API
        roots, since an `AgentWorkspace` cannot carry them.
        """
        _api_util.check_public_cloud_api_roots(
            cloud_workspace._credentials,  # noqa: SLF001  # Same-domain conversion.
        )
        workspace = cls(
            workspace_id=cloud_workspace.workspace_id,
            organization_id=organization_id,
            client_id=cloud_workspace.client_id,
            client_secret=cloud_workspace.client_secret,
            bearer_token=cloud_workspace.bearer_token,
        )
        if verify:
            workspace.get_info()
        return workspace
