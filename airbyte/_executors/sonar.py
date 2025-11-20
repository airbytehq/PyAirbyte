# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sonar Integration Executor for YAML-based connectors."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from airbyte import exceptions as exc
from airbyte._executors.base import Executor


if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any

    from airbyte.registry import ConnectorMetadata


class SonarExecutor(Executor):
    """Executor for Sonar YAML-based integration connectors.

    This executor wraps the connector-sdk's ConnectorExecutor to run
    YAML-defined connectors without subprocess execution.
    """

    def __init__(
        self,
        *,
        name: str,
        yaml_path: Path | str,
        secrets: dict[str, Any] | None = None,
        enable_logging: bool = False,
        log_file: str | None = None,
        metadata: ConnectorMetadata | None = None,
        target_version: str | None = None,
    ) -> None:
        """Initialize Sonar executor.

        Args:
            name: Connector name
            yaml_path: Path to connector YAML definition
            secrets: Secret credentials (will be converted to SecretStr)
            enable_logging: Enable request/response logging
            log_file: Path to log file
            metadata: Connector metadata (optional)
            target_version: Target version (not used for YAML connectors)
        """
        super().__init__(name=name, metadata=metadata, target_version=target_version)

        self.yaml_path = Path(yaml_path)
        self.secrets = secrets or {}
        self.enable_logging = enable_logging
        self.log_file = log_file
        self._executor: Any = None  # connector_sdk.ConnectorExecutor

        if not self.yaml_path.exists():
            raise exc.PyAirbyteInputError(
                message=f"Connector YAML file not found: {yaml_path}",
                input_value=str(yaml_path),
            )

    def _get_executor(self) -> Any:  # noqa: ANN401
        """Lazily create connector-sdk executor.

        Returns:
            ConnectorExecutor instance

        Raises:
            PyAirbyteInputError: If connector-sdk is not installed
        """
        if self._executor is None:
            try:
                from connector_sdk import ConnectorExecutor  # noqa: PLC0415
                from connector_sdk.secrets import SecretStr  # noqa: PLC0415
            except ImportError as ex:
                raise exc.PyAirbyteInputError(
                    message=(
                        "connector-sdk is required for Sonar integrations. "
                        "Install it with: poetry install --with integrations"
                    ),
                    guidance="Run: poetry install --with integrations",
                ) from ex

            # Convert secrets to SecretStr
            secret_dict = {
                key: SecretStr(value) if not isinstance(value, SecretStr) else value
                for key, value in self.secrets.items()
            }

            self._executor = ConnectorExecutor(
                config_path=str(self.yaml_path),
                secrets=secret_dict,
                enable_logging=self.enable_logging,
                log_file=self.log_file,
            )

        return self._executor

    @property
    def _cli(self) -> list[str]:
        """Get CLI args (not used for Sonar executor).

        This property is required by the Executor base class but is not
        used for YAML-based connectors.
        """
        return []

    def execute(
        self,
        args: list[str],
        *,
        stdin: Any = None,  # noqa: ANN401
        suppress_stderr: bool = False,
    ) -> Iterator[str]:
        """Execute is not supported for Sonar connectors.

        Sonar connectors use async execute methods instead of subprocess execution.
        Use Integration.execute() or Integration.aexecute() instead.

        Raises:
            NotImplementedError: Always raised
        """
        raise NotImplementedError(
            "Sonar connectors do not support subprocess execution. "
            "Use Integration.execute() or Integration.aexecute() instead."
        )

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
        """
        executor = self._get_executor()
        return await executor.execute(resource, verb, params)

    async def aexecute_batch(
        self,
        operations: list[tuple[str, str, dict[str, Any] | None]],
    ) -> list[dict[str, Any]]:
        """Execute multiple operations concurrently.

        Args:
            operations: List of (resource, verb, params) tuples

        Returns:
            List of responses in the same order as operations
        """
        executor = self._get_executor()
        return await executor.execute_batch(operations)

    def ensure_installation(self, *, auto_fix: bool = True) -> None:
        """Ensure connector is available (no-op for YAML connectors)."""
        pass

    def install(self) -> None:
        """Install connector (no-op for YAML connectors)."""
        pass

    def uninstall(self) -> None:
        """Uninstall connector (no-op for YAML connectors)."""
        pass

    def get_installed_version(
        self,
        *,
        raise_on_error: bool = False,
        recheck: bool = False,  # noqa: ARG002
    ) -> str | None:
        """Get connector version from YAML metadata.

        Returns:
            Version string if available in YAML, None otherwise
        """
        try:
            from connector_sdk.config_loader import load_connector_config  # noqa: PLC0415

            config = load_connector_config(str(self.yaml_path))
        except Exception:
            if raise_on_error:
                raise
            return None
        else:
            return config.connector.version if config.connector else None


__all__ = [
    "SonarExecutor",
]
