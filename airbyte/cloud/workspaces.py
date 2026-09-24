# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

By overriding `api_root`, you can use this module to interact with self-managed Airbyte instances,
both OSS and Enterprise.

## Usage Examples

Get a new workspace object and deploy a source to it:

```python
import airbyte as ab
from airbyte import cloud

workspace = cloud.CloudWorkspace(
    workspace_id="...",
    client_id="...",
    client_secret="...",
)

# Deploy a source to the workspace
source = ab.get_source("source-faker", config={"count": 100})
deployed_source = workspace.deploy_source(
    name="test-source",
    source=source,
)

# Run a check on the deployed source and raise an exception if the check fails
check_result = deployed_source.check(raise_on_error=True)

# Permanently delete the newly-created source
workspace.permanently_delete_source(deployed_source)
```
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload

import yaml

from airbyte import exceptions as exc
from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._direct_connectors import connector_docs
from airbyte._direct_connectors.models import (
    _SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS,
    DirectAccessGuidance,
    DirectAccessGuidanceIndexEntry,
)
from airbyte._util import api_util, deployment, text_util
from airbyte._util.api_util import get_web_url_root
from airbyte.cloud import connectors as cloud_connectors
from airbyte.cloud import organizations as cloud_organizations
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.client_config import CloudClientConfig
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import (
    CloudWorkspaceInfo,
    ConnectorFeature,
    ConnectorType,
    OrganizationFeature,
)
from airbyte.constants import SECRETS_HYDRATION_PREFIX
from airbyte.destinations.base import Destination
from airbyte.exceptions import AirbyteError
from airbyte.secrets.base import SecretString
from airbyte.secrets.hydration import detect_hardcoded_secrets


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.cloud.organizations import CloudOrganization
    from airbyte.sources.base import Source


def _deferred_credentials_config(
    config: object,
    *,
    definition_id: str | None,
) -> tuple[dict[str, Any], str]:
    """Validate the inputs for a deferred-credential deploy.

    Returns the configuration and definition ID. `SecretString` values and
    `secret_reference::` strings are rejected; plain-text credential fields (per the
    global secrets mask) are rejected too.
    """
    if not isinstance(config, dict):
        raise exc.PyAirbyteInputError(
            message="Deferred deployment requires a configuration dictionary.",
            guidance="Pass the non-secret configuration values, not a connector object.",
        )
    if not definition_id:
        raise exc.PyAirbyteInputError(
            message="`definition_id` is required when `defer_credentials=True`.",
        )

    def _reject_secrets(value: object) -> None:
        if isinstance(value, SecretString) or (
            isinstance(value, str) and value.startswith(SECRETS_HYDRATION_PREFIX)
        ):
            raise exc.PyAirbyteInputError(
                message="Deferred deployment does not accept secret values or references.",
                guidance="Omit credentials; the user supplies them in Airbyte Cloud.",
            )
        if isinstance(value, dict):
            for nested in value.values():
                _reject_secrets(nested)
        elif isinstance(value, (list, tuple)):
            for nested in value:
                _reject_secrets(nested)

    _reject_secrets(config)
    found = detect_hardcoded_secrets(config=config, spec_json_schema=None)
    if found:
        raise exc.PyAirbyteInputError(
            message="Deferred deployment does not accept credential values.",
            guidance="Omit credentials; the user supplies them in Airbyte Cloud.",
            context={"fields": [".".join(p) for p in found]},
        )
    return dict(config), definition_id


@dataclass(init=False, kw_only=True)  # noqa: PLR0904  # Core cloud API facade.
class CloudWorkspace:
    """A remote workspace on the Airbyte Cloud.

    By overriding `api_root`, you can use this class to interact with self-managed Airbyte
    instances, both OSS and Enterprise.

    Two authentication methods are supported (mutually exclusive):
    1. OAuth2 client credentials (client_id + client_secret)
    2. Bearer token authentication

    Example with client credentials:
        ```python
        workspace = CloudWorkspace(
            workspace_id="...",
            client_id="...",
            client_secret="...",
        )
        ```

    Example with bearer token:
        ```python
        workspace = CloudWorkspace(
            workspace_id="...",
            bearer_token="...",
        )
        ```
    """

    workspace_id: str
    client_id: SecretString | None
    client_secret: SecretString | None
    api_root: str
    config_api_root: str | None
    """The Config API root URL."""
    bearer_token: SecretString | None

    # Internal credentials objects (set in __init__, excluded from repr)
    _credentials: _AirbyteCredentials = field(init=False, repr=False)
    _client_config: CloudClientConfig = field(init=False, repr=False)

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        api_root: str | None = None,
        config_api_root: str | None = None,
        bearer_token: str | SecretString | None = None,
        organization_id: str | None = None,
    ) -> None:
        """Validate and initialize credentials.

        `organization_id` is optional. The workspace's parent organization is always looked up
        from the Config API; when given, this value is checked against that lookup (a mismatch
        raises) and used only as a fallback when the lookup itself is unavailable.
        """
        env_vars = not (client_id or client_secret or bearer_token)
        credentials = _AirbyteCredentials.from_auth(
            workspace_id=workspace_id,
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=api_root,
            config_api_root=config_api_root,
            env_vars=env_vars,
        )
        if not credentials.workspace_id:
            raise exc.PyAirbyteInputError(
                message="Workspace ID is required.",
                guidance=(
                    "Provide a workspace ID, or call `get_default_cloud_context` to discover "
                    "available workspaces."
                ),
            )

        self._credentials = credentials
        self.workspace_id = credentials.workspace_id or ""
        self.client_id = credentials.client_id
        self.client_secret = credentials.client_secret
        self.bearer_token = credentials.bearer_token
        self.api_root = credentials.public_api_root
        self.config_api_root = credentials.config_api_root

        # Create internal CloudClientConfig object (validates mutual exclusivity)
        self._client_config = CloudClientConfig(
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            api_root=self.api_root,
            config_api_root=self.config_api_root,
        )

    @classmethod
    def from_env(
        cls,
        workspace_id: str | None = None,
        *,
        api_root: str | None = None,
        config_api_root: str | None = None,
    ) -> CloudWorkspace:
        """Create a CloudWorkspace using credentials from environment variables.

        This factory method resolves credentials from environment variables,
        providing a convenient way to create a workspace without explicitly
        passing credentials.

        Two authentication methods are supported (mutually exclusive):
        1. Bearer token (checked first)
        2. OAuth2 client credentials (fallback)

        Environment variables used:
            - `AIRBYTE_CLOUD_BEARER_TOKEN`: Bearer token (alternative to client credentials).
            - `AIRBYTE_CLOUD_CLIENT_ID`: OAuth client ID (for client credentials flow).
            - `AIRBYTE_CLOUD_CLIENT_SECRET`: OAuth client secret (for client credentials flow).
            - `AIRBYTE_CLOUD_WORKSPACE_ID`: The workspace ID (if not passed as argument).
            - `AIRBYTE_CLOUD_API_URL`: Optional. The API root URL (defaults to Airbyte Cloud).
            - `AIRBYTE_CLOUD_CONFIG_API_URL`: Optional. The Config API root URL.

        Args:
            workspace_id: The workspace ID. If not provided, will be resolved from
                the `AIRBYTE_CLOUD_WORKSPACE_ID` environment variable.
            api_root: The API root URL. If not provided, will be resolved from
                the `AIRBYTE_CLOUD_API_URL` environment variable, or default to
                the Airbyte Cloud API.
            config_api_root: The Config API root URL. If not provided, will be resolved
                from the `AIRBYTE_CLOUD_CONFIG_API_URL` environment variable.

        Returns:
            A CloudWorkspace instance configured with credentials from the environment.

        Raises:
            PyAirbyteInputError: If required credentials are not found in
                the environment or are incomplete.

        Example:
            ```python
            # With workspace_id from environment
            workspace = CloudWorkspace.from_env()

            # With explicit workspace_id
            workspace = CloudWorkspace.from_env(workspace_id="your-workspace-id")
            ```
        """
        return cls(
            workspace_id=workspace_id,
            api_root=api_root,
            config_api_root=config_api_root,
        )

    @property
    def workspace_url(self) -> str | None:
        """The web URL of the workspace."""
        return f"{get_web_url_root(self.api_root)}/workspaces/{self.workspace_id}"

    @cached_property
    def _organization_info(self) -> dict[str, Any]:
        """Fetch and cache organization info for this workspace.

        Uses the Config API endpoint for an efficient O(1) lookup.
        This is an internal method; use get_organization() for public access.
        """
        return api_util.get_workspace_organization_info(
            workspace_id=self.workspace_id,
            api_root=self.api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )

    @overload
    def get_organization(self) -> CloudOrganization: ...

    @overload
    def get_organization(
        self,
        *,
        raise_on_error: Literal[True],
    ) -> CloudOrganization: ...

    @overload
    def get_organization(
        self,
        *,
        raise_on_error: Literal[False],
    ) -> CloudOrganization | None: ...

    def get_organization(
        self,
        *,
        raise_on_error: bool = True,
    ) -> CloudOrganization | None:
        """Get the organization this workspace belongs to.

        Fetching organization info requires ORGANIZATION_READER permissions on the organization,
        which may not be available with workspace-scoped credentials.

        Args:
            raise_on_error: If True (default), raises AirbyteError on permission or API errors.
                If False, returns None instead of raising.

        Returns:
            CloudOrganization object with organization_id and organization_name,
            or None if raise_on_error=False and an error occurred.

        Raises:
            AirbyteError: If raise_on_error=True and the organization info cannot be fetched
                (e.g., due to insufficient permissions or missing data).
        """
        try:
            info = self._organization_info
        except (AirbyteError, NotImplementedError):
            if raise_on_error:
                raise
            return None

        organization_id = info.get("organizationId")
        organization_name = info.get("organizationName")

        # Validate that both organization_id and organization_name are non-null and non-empty
        if not organization_id or not organization_name:
            if raise_on_error:
                raise AirbyteError(
                    message="Organization info is incomplete.",
                    context={
                        "organization_id": organization_id,
                        "organization_name": organization_name,
                    },
                )
            return None

        organization_credentials = self._credentials.with_organization_id(organization_id)
        return cloud_organizations.CloudOrganization(
            organization_id=organization_id,
            organization_name=organization_name,
            client_id=organization_credentials.client_id,
            client_secret=organization_credentials.client_secret,
            bearer_token=organization_credentials.bearer_token,
            public_api_root=organization_credentials.public_api_root,
            config_api_root=organization_credentials.config_api_root,
        )

    # Airbyte Agents (Context layer) status

    def _has_context_layer_api(self) -> bool:
        """Return whether a Context layer (Agents) API exists for this workspace's API roots.

        Answered from configuration alone, without any network call. When `False`, every
        feature flag on this workspace and its connectors is `False`.
        """
        return deployment.is_agents_api_available(
            public_api_root=self.api_root,
            config_api_root=self.config_api_root,
        )

    @cached_property
    def enabled_features(self) -> frozenset[OrganizationFeature]:
        """The features enabled for this workspace. Resolved on first access and cached.

        `DIRECT_ACCESS` is reported when AI agents can use this workspace's connectors
        through the Airbyte Context layer. Cloud enforces organization and workspace
        enrollment on every Context layer request, so the flag reflects Context layer API
        availability for the workspace's deployment roots without any API call;
        per-connector enablement is reported by connector features.
        """
        if not self._has_context_layer_api():
            return frozenset()

        return frozenset({OrganizationFeature.DIRECT_ACCESS})

    def is_feature_enabled(self, feature: OrganizationFeature) -> bool:
        """Whether `feature` is enabled for this workspace.

        Uses the cached feature set when available; search indexing has not launched yet,
        so it always returns `False` without an API call.
        """
        if feature == OrganizationFeature.SEARCH_INDEXING:
            return False
        return feature in self.enabled_features

    def _get_connector_features(
        self,
        connector: cloud_connectors.CloudConnector,
    ) -> frozenset[ConnectorFeature]:
        """Resolve the enabled features for one connector in this workspace.

        A docs probe against the Context layer reports whether the connector
        is enabled for agent access. Search indexing has not launched yet, so it is never
        reported as enabled.
        """
        if not self._has_context_layer_api():
            return frozenset()

        if connector.connector_type == ConnectorType.DESTINATION:
            if connector.definition_id not in _SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS:
                return frozenset()
            if connector._context_layer_inspect(warnings=[]) is None:  # noqa: SLF001
                return frozenset()
            return frozenset({ConnectorFeature.DIRECT_ACCESS, ConnectorFeature.DIRECT_SQL_QUERY})

        if connector._context_layer_inspect(warnings=[]) is None:  # noqa: SLF001
            return frozenset()

        return frozenset({ConnectorFeature.DIRECT_ACCESS, ConnectorFeature.DIRECT_API_QUERY})

    # Test connection and creds

    def connect(self) -> None:
        """Check that the workspace is reachable and raise an exception otherwise.

        Note: It is not necessary to call this method before calling other operations. It
              serves primarily as a simple check to ensure that the workspace is reachable
              and credentials are correct.
        """
        _ = api_util.get_workspace(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        print(f"Successfully connected to workspace: {self.workspace_url}")

    # Get sources, destinations, and connections

    def get_connection(
        self,
        connection_id: str,
    ) -> CloudConnection:
        """Get a connection by ID.

        This method does not fetch data from the API. It returns a `CloudConnection` object,
        which will be loaded lazily as needed.
        """
        return CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )

    def get_source(
        self,
        source_id: str,
    ) -> cloud_connectors.CloudSource:
        """Get a source by ID.

        This method does not fetch data from the API. It returns a `CloudSource` object,
        which will be loaded lazily as needed.
        """
        return cloud_connectors.CloudSource(
            workspace=self,
            connector_id=source_id,
        )

    def get_destination(
        self,
        destination_id: str,
    ) -> cloud_connectors.CloudDestination:
        """Get a destination by ID.

        This method does not fetch data from the API. It returns a `CloudDestination` object,
        which will be loaded lazily as needed.
        """
        return cloud_connectors.CloudDestination(
            workspace=self,
            connector_id=destination_id,
        )

    def get_connector(
        self,
        id_or_name: str | None = None,
        /,
        *,
        connector_id: str | None = None,
        name: str | None = None,
    ) -> cloud_connectors.CloudConnector:
        """Get a connector by ID or by name.

        An explicit ID (positional or `connector_id="..."`) returns an untyped
        `CloudConnector` without any API call; its kind is resolved lazily on first use.
        A `name=...` lookup lists the workspace's connectors and matches on an exact
        name (case-insensitive), then on a unique substring, returning the matched
        `CloudSource` or `CloudDestination`.
        """
        lookup = agents_api_util._resolve_connector_lookup(  # noqa: SLF001
            id_or_name,
            id=None,
            connector_id=connector_id,
            name=name,
        )

        if lookup.connector_id:
            return cloud_connectors.CloudConnector(workspace=self, connector_id=lookup.connector_id)

        connectors = self.list_connectors()
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
            raise exc.AirbyteError(
                message="No connector found with the given ID or name.",
                guidance="Use `list_connectors()` to see the available connectors.",
                context={"lookup": lookup.name, "workspace_id": self.workspace_id},
            )
        if len(matches) > 1:
            raise exc.AirbyteError(
                message="Multiple connectors matched the given name.",
                guidance="Pass `connector_id`, or a name that matches only one connector.",
                context={
                    "name": lookup.name,
                    "matched_names": [connector.name for connector in matches],
                },
            )
        return matches[0]

    def _list_guidance(self) -> list[DirectAccessGuidanceIndexEntry]:
        """List the direct-access guidance available to this workspace.

        The index is derived locally from the workspace's connectors: one entry per
        source, plus one per SQL passthrough destination. Use `get_agent_skill_docs()`
        to check whether a given entry's docs exist on the server.
        """
        entries = [
            DirectAccessGuidanceIndexEntry(
                id=connector_docs.source_skill_id(source.connector_id),
                kind="connector_source",
                title=source.name,
            )
            for source in self.list_sources()
        ]
        entries.extend(
            DirectAccessGuidanceIndexEntry(
                id=connector_docs.destination_skill_id(destination.connector_id),
                kind="connector_destination",
                title=destination.name,
            )
            for destination in self.list_destinations()
            if destination.definition_id in _SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS
        )
        return entries

    def get_agent_skill_docs(
        self,
        docs_skill_id: str | None = None,
        /,
        *,
        connector_id: str | None = None,
        section: str | None = None,
    ) -> DirectAccessGuidance:
        """Returns the requested skill document by ID for an AI agent.

        Pass either a fully-qualified `docs_skill_id` (positional) or a
        `connector_id` (source or destination); exactly one is required.

        `section` is optional; if omitted, the summary overview is returned along with
        the list of available sections.
        """
        if connector_id is not None and docs_skill_id is not None:
            raise exc.PyAirbyteInputError(
                message="Provide exactly one of `docs_skill_id` or `connector_id`.",
            )
        if connector_id is not None:
            return self.get_connector(connector_id).read_agent_skill_docs(section=section)
        if docs_skill_id is None:
            raise exc.PyAirbyteInputError(
                message="Provide exactly one of `docs_skill_id` or `connector_id`.",
            )
        if docs_skill_id.startswith(connector_docs.DESTINATION_SKILL_PREFIX):
            destination = self.get_destination(
                connector_docs.connector_id_from_skill_id(docs_skill_id)
            )
            return destination.read_agent_skill_docs(section=section)
        return DirectAccessGuidance.model_validate(
            agents_api_util.read_cloud_skill_docs(
                workspace_id=self.workspace_id,
                skill_id=docs_skill_id,
                credentials=self._credentials,
                section=section,
            )
        )

    # Deploy sources and destinations

    def deploy_source(
        self,
        name: str,
        source: Source | dict[str, Any],
        *,
        unique: bool = True,
        random_name_suffix: bool = False,
        definition_id: str | None = None,
        defer_credentials: bool = False,
    ) -> cloud_connectors.CloudSource:
        """Deploy a source to the workspace.

        Returns the newly deployed source.

        Args:
            name: The name to use when deploying.
            source: The source object to deploy, or (with `defer_credentials=True`) a
                dictionary of non-secret configuration values.
            unique: Whether to require a unique name. If `True`, duplicate names
                are not allowed. Defaults to `True`.
            random_name_suffix: Whether to append a random suffix to the name.
            definition_id: The source definition ID. Required with `defer_credentials=True`.
            defer_credentials: Save a draft with partial configuration. A person completes
                credentials and other missing settings at the returned source's `connector_url`.
                A successful connection check promotes the draft. Raises
                `AirbyteDeferredSetupError` if Cloud does not acknowledge draft mode.
        """
        if defer_credentials:
            return cloud_connectors.CloudSource(
                workspace=self,
                connector_id=self._deploy_deferred(
                    connector_type="source",
                    name=name,
                    config=source,
                    definition_id=definition_id,
                    unique=unique,
                    random_name_suffix=random_name_suffix,
                ),
            )
        if isinstance(source, dict):
            raise exc.PyAirbyteInputError(
                message="`source` must be a `Source` object unless `defer_credentials=True`.",
            )

        source_config_dict = source._hydrated_config.copy()  # noqa: SLF001 (non-public API)
        source_config_dict["sourceType"] = source.name.replace("source-", "")

        if random_name_suffix:
            name += f" (ID: {text_util.generate_random_suffix()})"

        if unique:
            existing = self.list_sources(name=name)
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="source",
                    resource_name=name,
                )

        deployed_source = api_util.create_source(
            name=name,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            config=source_config_dict,
            definition_id=definition_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return cloud_connectors.CloudSource(
            workspace=self,
            connector_id=deployed_source.source_id,
        )

    def deploy_destination(
        self,
        name: str,
        destination: Destination | dict[str, Any],
        *,
        unique: bool = True,
        random_name_suffix: bool = False,
        definition_id: str | None = None,
        defer_credentials: bool = False,
    ) -> cloud_connectors.CloudDestination:
        """Deploy a destination to the workspace.

        Returns the newly deployed destination ID.

        Args:
            name: The name to use when deploying.
            destination: The destination to deploy. Can be a local Airbyte `Destination` object or a
                dictionary of configuration values.
            unique: Whether to require a unique name. If `True`, duplicate names
                are not allowed. Defaults to `True`.
            random_name_suffix: Whether to append a random suffix to the name.
            definition_id: The destination definition ID. Required with `defer_credentials=True`;
                otherwise the type is inferred from `destinationType`.
            defer_credentials: Create the destination without its credentials. See
                `deploy_source`.
        """
        if defer_credentials:
            return cloud_connectors.CloudDestination(
                workspace=self,
                connector_id=self._deploy_deferred(
                    connector_type="destination",
                    name=name,
                    config=destination,
                    definition_id=definition_id,
                    unique=unique,
                    random_name_suffix=random_name_suffix,
                ),
            )

        if isinstance(destination, Destination):
            destination_conf_dict = destination._hydrated_config.copy()  # noqa: SLF001 (non-public API)
            destination_conf_dict["destinationType"] = destination.name.replace("destination-", "")
            # raise ValueError(destination_conf_dict)
        else:
            destination_conf_dict = destination.copy()
            if "destinationType" not in destination_conf_dict:
                raise exc.PyAirbyteInputError(
                    message="Missing `destinationType` in configuration dictionary.",
                )

        if random_name_suffix:
            name += f" (ID: {text_util.generate_random_suffix()})"

        if unique:
            existing = self.list_destinations(name=name)
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="destination",
                    resource_name=name,
                )

        deployed_destination = api_util.create_destination(
            name=name,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            config=destination_conf_dict,  # Wants a dataclass but accepts dict
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return cloud_connectors.CloudDestination(
            workspace=self,
            connector_id=deployed_destination.destination_id,
        )

    def _deploy_deferred(
        self,
        *,
        connector_type: Literal["source", "destination"],
        name: str,
        config: object,
        definition_id: str | None,
        unique: bool,
        random_name_suffix: bool,
    ) -> str:
        """Create a connector with deferred credentials on the Config API and return its ID."""
        config_dict, definition_id = _deferred_credentials_config(
            config, definition_id=definition_id
        )

        if random_name_suffix:
            name += f" (ID: {text_util.generate_random_suffix()})"

        if unique:
            existing = (
                self.list_sources(name=name)
                if connector_type == "source"
                else self.list_destinations(name=name)
            )
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type=connector_type,
                    resource_name=name,
                )

        return api_util.create_connector_deferred(
            connector_type=connector_type,
            name=name,
            workspace_id=self.workspace_id,
            definition_id=definition_id,
            config=config_dict,
            api_root=self.api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )

    def permanently_delete_source(
        self,
        source: str | cloud_connectors.CloudSource,
        *,
        safe_mode: bool = True,
    ) -> None:
        """Delete a source from the workspace.

        You can pass either the source ID `str` or a deployed `Source` object.

        Args:
            source: The source ID or CloudSource object to delete
            safe_mode: If True, requires the source name to contain "delete-me" or "deleteme"
                (case insensitive) to prevent accidental deletion. Defaults to True.
        """
        if not isinstance(source, (str, cloud_connectors.CloudSource)):
            raise exc.PyAirbyteInputError(
                message="Invalid source type.",
                input_value=type(source).__name__,
            )

        api_util.delete_source(
            source_id=(
                source.connector_id if isinstance(source, cloud_connectors.CloudSource) else source
            ),
            source_name=(source.name if isinstance(source, cloud_connectors.CloudSource) else None),
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

    # Deploy and delete destinations

    def permanently_delete_destination(
        self,
        destination: str | cloud_connectors.CloudDestination,
        *,
        safe_mode: bool = True,
    ) -> None:
        """Delete a deployed destination from the workspace.

        You can pass either the `Cache` class or the deployed destination ID as a `str`.

        Args:
            destination: The destination ID or CloudDestination object to delete
            safe_mode: If True, requires the destination name to contain "delete-me" or "deleteme"
                (case insensitive) to prevent accidental deletion. Defaults to True.
        """
        if not isinstance(destination, (str, cloud_connectors.CloudDestination)):
            raise exc.PyAirbyteInputError(
                message="Invalid destination type.",
                input_value=type(destination).__name__,
            )

        api_util.delete_destination(
            destination_id=(
                destination if isinstance(destination, str) else destination.destination_id
            ),
            destination_name=(
                destination.name
                if isinstance(destination, cloud_connectors.CloudDestination)
                else None
            ),
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

    # Deploy and delete connections

    def deploy_connection(
        self,
        connection_name: str,
        *,
        source: cloud_connectors.CloudSource | str,
        selected_streams: list[str],
        destination: cloud_connectors.CloudDestination | str,
        table_prefix: str | None = None,
    ) -> CloudConnection:
        """Create a new connection between an already deployed source and destination.

        Returns the newly deployed connection object.

        Args:
            connection_name: The name of the connection.
            source: The deployed source. You can pass a source ID or a CloudSource object.
            destination: The deployed destination. You can pass a destination ID or a
                CloudDestination object.
            table_prefix: Optional. The table prefix to use when syncing to the destination.
            selected_streams: The selected stream names to sync within the connection.
        """
        if not selected_streams:
            raise exc.PyAirbyteInputError(
                guidance="You must provide `selected_streams` when creating a connection."
            )

        source_id: str = source if isinstance(source, str) else source.connector_id
        destination_id: str = (
            destination if isinstance(destination, str) else destination.connector_id
        )

        deployed_connection = api_util.create_connection(
            name=connection_name,
            source_id=source_id,
            destination_id=destination_id,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            selected_stream_names=selected_streams,
            prefix=table_prefix or "",
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )

        return CloudConnection(
            workspace=self,
            connection_id=deployed_connection.connection_id,
            source=deployed_connection.source_id,
            destination=deployed_connection.destination_id,
        )

    def permanently_delete_connection(
        self,
        connection: str | CloudConnection,
        *,
        cascade_delete_source: bool = False,
        cascade_delete_destination: bool = False,
        safe_mode: bool = True,
    ) -> None:
        """Delete a deployed connection from the workspace.

        Args:
            connection: The connection ID or CloudConnection object to delete
            cascade_delete_source: If True, also delete the source after deleting the connection
            cascade_delete_destination: If True, also delete the destination after deleting
                the connection
            safe_mode: If True, requires the connection name to contain "delete-me" or "deleteme"
                (case insensitive) to prevent accidental deletion. Defaults to True. Also applies
                to cascade deletes.
        """
        if connection is None:
            raise ValueError("No connection ID provided.")

        if isinstance(connection, str):
            connection = CloudConnection(
                workspace=self,
                connection_id=connection,
            )

        api_util.delete_connection(
            connection_id=connection.connection_id,
            connection_name=connection.name,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

        if cascade_delete_source:
            self.permanently_delete_source(
                source=connection.source_id,
                safe_mode=safe_mode,
            )
        if cascade_delete_destination:
            self.permanently_delete_destination(
                destination=connection.destination_id,
                safe_mode=safe_mode,
            )

    # List workspaces, sources, destinations, and connections

    def list_workspaces(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
        limit: int | None = None,
    ) -> list[CloudWorkspaceInfo]:
        """List workspaces available to the current credentials, with an optional limit."""
        return [
            CloudWorkspaceInfo.from_api_response(workspace)
            for workspace in api_util.list_workspaces(
                workspace_id="",
                api_root=self.api_root,
                name=name,
                name_filter=name_filter,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
                limit=limit,
            )
        ]

    def rename(
        self,
        name: str,
    ) -> CloudWorkspace:
        """Rename this workspace."""
        api_util.rename_workspace(
            workspace_id=self.workspace_id,
            name=name,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return self

    def permanently_delete(
        self,
        *,
        workspace_name: str | None = None,
        safe_mode: bool = True,
    ) -> None:
        """Permanently delete this workspace if it has no connections.

        When `safe_mode` is enabled, the workspace name must contain `delete-me`
        or `deleteme`. This also checks for existing connections before deleting
        and raises `AirbyteWorkspaceNotEmptyError` if the workspace is not empty.
        """
        api_util.permanently_delete_workspace(
            workspace_id=self.workspace_id,
            workspace_name=workspace_name,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

    def list_connections(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
        limit: int | None = None,
    ) -> list[CloudConnection]:
        """List connections by name in the workspace, with an optional limit."""
        connections = api_util.list_connections(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
            limit=limit,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return [
            CloudConnection._from_connection_response(  # noqa: SLF001 (non-public API)
                workspace=self,
                connection_response=connection,
            )
            for connection in connections
        ]

    def list_sources(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
        limit: int | None = None,
    ) -> list[cloud_connectors.CloudSource]:
        """List all sources in the workspace, with an optional limit."""
        sources = api_util.list_sources(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
            limit=limit,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return [
            cloud_connectors.CloudSource._from_source_response(  # noqa: SLF001 (non-public API)
                workspace=self,
                source_response=source,
            )
            for source in sources
        ]

    def list_destinations(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
        limit: int | None = None,
    ) -> list[cloud_connectors.CloudDestination]:
        """List all destinations in the workspace, with an optional limit."""
        destinations = api_util.list_destinations(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
            limit=limit,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return [
            cloud_connectors.CloudDestination._from_destination_response(  # noqa: SLF001 (non-public API)
                workspace=self,
                destination_response=destination,
            )
            for destination in destinations
        ]

    def list_connectors(
        self,
        *,
        connector_type: ConnectorType | None = None,
        feature_filter: ConnectorFeature | None = None,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> list[cloud_connectors.CloudConnector]:
        """List sources and destinations in the workspace, with optional filters.

        Items are `CloudSource` and `CloudDestination` objects. Enabled features are
        resolved (and cached) only when `feature_filter` is set; otherwise
        `enabled_features` resolves lazily on first access.

        Args:
            connector_type: Return only sources or only destinations.
            feature_filter: Return only connectors with this feature enabled.
            name_contains: Case-insensitive substring to match against connector names.
            limit: Maximum number of connectors to return.
        """
        if limit is not None and limit <= 0:
            raise exc.PyAirbyteInputError(message="`limit` must be greater than 0.")

        connectors: list[cloud_connectors.CloudConnector] = []
        if connector_type in {None, ConnectorType.SOURCE}:
            connectors.extend(self.list_sources())
        if connector_type in {None, ConnectorType.DESTINATION}:
            connectors.extend(self.list_destinations())

        if name_contains:
            needle = name_contains.casefold()
            connectors = [
                connector
                for connector in connectors
                if connector.name is not None and needle in connector.name.casefold()
            ]

        matches: list[cloud_connectors.CloudConnector] = []
        for connector in connectors:
            if feature_filter is not None:
                connector._enabled_features = self._get_connector_features(  # noqa: SLF001
                    connector
                )
                if feature_filter not in connector._get_enabled_features():  # noqa: SLF001
                    continue
            matches.append(connector)
            if limit is not None and len(matches) >= limit:
                break
        return matches

    def publish_custom_source_definition(
        self,
        name: str,
        *,
        manifest_yaml: dict[str, Any] | Path | str | None = None,
        docker_image: str | None = None,
        docker_tag: str | None = None,
        unique: bool = True,
        pre_validate: bool = True,
        testing_values: dict[str, Any] | None = None,
    ) -> cloud_connectors.CustomCloudSourceDefinition:
        """Publish a custom source connector definition.

        You must specify EITHER manifest_yaml (for YAML connectors) OR both docker_image
        and docker_tag (for Docker connectors), but not both.

        Args:
            name: Display name for the connector definition
            manifest_yaml: Low-code CDK manifest (dict, Path to YAML file, or YAML string)
            docker_image: Docker repository (e.g., 'airbyte/source-custom')
            docker_tag: Docker image tag (e.g., '1.0.0')
            unique: Whether to enforce name uniqueness
            pre_validate: Whether to validate manifest client-side (YAML only)
            testing_values: Optional configuration values to use for testing in the
                Connector Builder UI. If provided, these values are stored as the complete
                testing values object for the connector builder project (replaces any existing
                values), allowing immediate test read operations.

        Returns:
            CustomCloudSourceDefinition object representing the created definition

        Raises:
            PyAirbyteInputError: If both or neither of manifest_yaml and docker_image provided
            AirbyteDuplicateResourcesError: If unique=True and name already exists
        """
        is_yaml = manifest_yaml is not None
        is_docker = docker_image is not None

        if is_yaml == is_docker:
            raise exc.PyAirbyteInputError(
                message=(
                    "Must specify EITHER manifest_yaml (for YAML connectors) OR "
                    "docker_image + docker_tag (for Docker connectors), but not both"
                ),
                context={
                    "manifest_yaml_provided": is_yaml,
                    "docker_image_provided": is_docker,
                },
            )

        if is_docker and docker_tag is None:
            raise exc.PyAirbyteInputError(
                message="docker_tag is required when docker_image is specified",
                context={"docker_image": docker_image},
            )

        if unique:
            existing = self.list_custom_source_definitions(
                definition_type="yaml" if is_yaml else "docker",
            )
            if any(d.name == name for d in existing):
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="custom_source_definition",
                    resource_name=name,
                )

        if is_yaml:
            manifest_dict: dict[str, Any]
            if isinstance(manifest_yaml, Path):
                manifest_dict = yaml.safe_load(manifest_yaml.read_text())
            elif isinstance(manifest_yaml, str):
                manifest_dict = yaml.safe_load(manifest_yaml)
            elif manifest_yaml is not None:
                manifest_dict = manifest_yaml
            else:
                raise exc.PyAirbyteInputError(
                    message="manifest_yaml is required for YAML connectors",
                    context={"name": name},
                )

            if pre_validate:
                api_util.validate_yaml_manifest(manifest_dict, raise_on_error=True)

            result = api_util.create_custom_yaml_source_definition(
                name=name,
                workspace_id=self.workspace_id,
                manifest=manifest_dict,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
            )
            custom_definition = cloud_connectors.CustomCloudSourceDefinition._from_yaml_response(  # noqa: SLF001
                self, result
            )

            # Set testing values if provided
            if testing_values is not None:
                custom_definition.set_testing_values(testing_values)

            return custom_definition

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    def list_custom_source_definitions(
        self,
        *,
        definition_type: Literal["yaml", "docker"],
    ) -> list[cloud_connectors.CustomCloudSourceDefinition]:
        """List custom source connector definitions.

        Args:
            definition_type: Connector type to list ("yaml" or "docker"). Required.

        Returns:
            List of CustomCloudSourceDefinition objects matching the specified type
        """
        if definition_type == "yaml":
            yaml_definitions = api_util.list_custom_yaml_source_definitions(
                workspace_id=self.workspace_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
            )
            return [
                cloud_connectors.CustomCloudSourceDefinition._from_yaml_response(self, d)  # noqa: SLF001
                for d in yaml_definitions
            ]

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    def get_custom_source_definition(
        self,
        definition_id: str,
        *,
        definition_type: Literal["yaml", "docker"],
    ) -> cloud_connectors.CustomCloudSourceDefinition:
        """Get a specific custom source definition by ID.

        Args:
            definition_id: The definition ID
            definition_type: Connector type ("yaml" or "docker"). Required.

        Returns:
            CustomCloudSourceDefinition object
        """
        if definition_type == "yaml":
            result = api_util.get_custom_yaml_source_definition(
                workspace_id=self.workspace_id,
                definition_id=definition_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
            )
            return cloud_connectors.CustomCloudSourceDefinition._from_yaml_response(self, result)  # noqa: SLF001

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )
