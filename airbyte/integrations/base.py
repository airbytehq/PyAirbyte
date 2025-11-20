# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Integration base class for Sonar connectors."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

from airbyte import exceptions as exc
from airbyte._connector_base import ConnectorBase


if TYPE_CHECKING:
    from airbyte._executors.sonar import SonarExecutor


class Integration(ConnectorBase):
    """A class representing a Sonar integration connector.

    Integrations are YAML-based connectors that provide API access through
    resource and verb operations, rather than the stream-based model used
    by sources and destinations.

    Example:
        ```python
        from airbyte import get_integration

        integration = get_integration(
            name="my-api", yaml_path="./connectors/my-api.yaml", secrets={"api_key": "sk_test_..."}
        )

        result = integration.execute("customers", "list", params={"limit": 10})

        result = await integration.aexecute("customers", "get", params={"id": "123"})
        ```
    """

    connector_type = "integration"

    def __init__(
        self,
        executor: SonarExecutor,
        name: str,
        yaml_path: Path | str,
        *,
        validate: bool = False,
    ) -> None:
        """Initialize the integration.

        Args:
            executor: SonarExecutor instance
            name: Integration name
            yaml_path: Path to connector YAML definition
            validate: Whether to validate the YAML on initialization
        """
        super().__init__(
            executor=executor,
            name=name,
            config=None,  # Integrations don't use config in the same way
            validate=False,  # Skip base class validation
        )
        self.yaml_path = Path(yaml_path)
        self._resources: list[str] | None = None

        if validate:
            self._validate_yaml()

    def _validate_yaml(self) -> None:
        """Validate the connector YAML definition.

        Raises:
            PyAirbyteInputError: If YAML is invalid
        """
        try:
            from connector_sdk.config_loader import load_connector_config  # noqa: PLC0415

            load_connector_config(str(self.yaml_path))
        except ImportError as ex:
            raise exc.PyAirbyteInputError(
                message=(
                    "connector-sdk is required for Sonar integrations. "
                    "Install it with: poetry install --with integrations"
                ),
                guidance="Run: poetry install --with integrations",
            ) from ex
        except Exception as ex:
            raise exc.PyAirbyteInputError(
                message=f"Invalid connector YAML: {ex}",
                context={"yaml_path": str(self.yaml_path)},
            ) from ex

    def list_resources(self) -> list[str]:
        """List available resources in the connector.

        Returns:
            List of resource names
        """
        if self._resources is None:
            try:
                from connector_sdk.config_loader import load_connector_config  # noqa: PLC0415

                config = load_connector_config(str(self.yaml_path))
                self._resources = [r.name for r in config.resources]
            except Exception as ex:
                raise exc.PyAirbyteInputError(
                    message=f"Failed to load resources from YAML: {ex}",
                    context={"yaml_path": str(self.yaml_path)},
                ) from ex

        return self._resources

    def list_verbs(self, resource: str) -> list[str]:
        """List available verbs for a resource.

        Args:
            resource: Resource name

        Returns:
            List of verb names (e.g., ["get", "list", "create"])
        """

        def _raise_not_found(available: list[str]) -> NoReturn:
            raise exc.PyAirbyteInputError(
                message=f"Resource '{resource}' not found",
                context={
                    "resource": resource,
                    "available_resources": available,
                },
            )

        try:
            from connector_sdk.config_loader import load_connector_config  # noqa: PLC0415

            config = load_connector_config(str(self.yaml_path))
            for r in config.resources:
                if r.name == resource:
                    return [v.value for v in r.verbs]

            _raise_not_found([r.name for r in config.resources])
        except Exception as ex:
            if isinstance(ex, exc.PyAirbyteInputError):
                raise
            raise exc.PyAirbyteInputError(
                message=f"Failed to load verbs for resource '{resource}': {ex}",
                context={"yaml_path": str(self.yaml_path), "resource": resource},
            ) from ex

    async def aexecute(
        self,
        resource: str,
        verb: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a verb on a resource asynchronously.

        Args:
            resource: Resource name (e.g., "customers")
            verb: Verb to execute (e.g., "list", "get", "create")
            params: Parameters for the operation

        Returns:
            API response as dictionary

        Example:
            ```python
            result = await integration.aexecute("customers", "list", params={"limit": 10})
            ```
        """
        from airbyte._executors.sonar import SonarExecutor  # noqa: PLC0415

        if not isinstance(self.executor, SonarExecutor):
            raise exc.PyAirbyteInternalError(message="Executor must be a SonarExecutor instance")

        return await self.executor.aexecute(resource, verb, params)

    def execute(
        self,
        resource: str,
        verb: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a verb on a resource synchronously.

        This is a convenience wrapper around aexecute() that handles
        the event loop automatically.

        Args:
            resource: Resource name (e.g., "customers")
            verb: Verb to execute (e.g., "list", "get", "create")
            params: Parameters for the operation

        Returns:
            API response as dictionary

        Example:
            ```python
            result = integration.execute("customers", "get", params={"id": "cus_123"})
            ```
        """
        try:
            asyncio.get_running_loop()
            raise exc.PyAirbyteInputError(
                message=(
                    "Cannot call execute() from within an async context. " "Use aexecute() instead."
                ),
                guidance="Use await integration.aexecute(...) in async code",
            )
        except RuntimeError:
            return asyncio.run(self.aexecute(resource, verb, params))

    async def aexecute_batch(
        self,
        operations: list[tuple[str, str, dict[str, Any] | None]],
    ) -> list[dict[str, Any]]:
        """Execute multiple operations concurrently.

        Args:
            operations: List of (resource, verb, params) tuples

        Returns:
            List of responses in the same order as operations

        Example:
            ```python
            results = await integration.aexecute_batch(
                [
                    ("customers", "list", {"limit": 10}),
                    ("customers", "get", {"id": "cus_123"}),
                    ("products", "list", {"limit": 5}),
                ]
            )
            ```
        """
        from airbyte._executors.sonar import SonarExecutor  # noqa: PLC0415

        if not isinstance(self.executor, SonarExecutor):
            raise exc.PyAirbyteInternalError(message="Executor must be a SonarExecutor instance")

        return await self.executor.aexecute_batch(operations)

    def execute_batch(
        self,
        operations: list[tuple[str, str, dict[str, Any] | None]],
    ) -> list[dict[str, Any]]:
        """Execute multiple operations concurrently (sync wrapper).

        Args:
            operations: List of (resource, verb, params) tuples

        Returns:
            List of responses in the same order as operations
        """
        try:
            asyncio.get_running_loop()
            raise exc.PyAirbyteInputError(
                message=(
                    "Cannot call execute_batch() from within an async context. "
                    "Use aexecute_batch() instead."
                ),
                guidance="Use await integration.aexecute_batch(...) in async code",
            )
        except RuntimeError:
            return asyncio.run(self.aexecute_batch(operations))

    def check(self) -> None:
        """Check connector connectivity.

        For Sonar integrations, this is a no-op unless the YAML defines
        an 'authorize' verb that can be used for health checks.

        Raises:
            NotImplementedError: If no health check mechanism is available
        """
        try:
            resources = self.list_resources()
            for resource in resources:
                verbs = self.list_verbs(resource)
                if "authorize" in verbs:
                    self.execute(resource, "authorize", params={})
                    return
        except Exception:
            pass

        raise NotImplementedError(
            "This integration does not define a health check mechanism. "
            "Add an 'authorize' verb to enable connectivity checks."
        )


__all__ = [
    "Integration",
]
