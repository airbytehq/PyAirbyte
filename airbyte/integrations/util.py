# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Utility functions for working with integrations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airbyte._executors.sonar import SonarExecutor
from airbyte.integrations.base import Integration


if TYPE_CHECKING:
    from pathlib import Path


def get_integration(
    name: str,
    yaml_path: Path | str,
    secrets: dict[str, Any] | None = None,
    *,
    enable_logging: bool = False,
    log_file: str | None = None,
    validate: bool = False,
) -> Integration:
    """Get a Sonar integration connector.

    Args:
        name: Integration name
        yaml_path: Path to connector YAML definition
        secrets: Secret credentials for authentication
        enable_logging: Enable request/response logging
        log_file: Path to log file (if enable_logging=True)
        validate: Whether to validate the YAML on initialization

    Returns:
        Integration instance

    Example:
        ```python
        from airbyte import get_integration

        integration = get_integration(
            name="my-api", yaml_path="./connectors/my-api.yaml", secrets={"api_key": "sk_test_..."}
        )

        resources = integration.list_resources()

        result = integration.execute("customers", "list", params={"limit": 10})
        ```
    """
    executor = SonarExecutor(
        name=name,
        yaml_path=yaml_path,
        secrets=secrets,
        enable_logging=enable_logging,
        log_file=log_file,
    )

    return Integration(
        executor=executor,
        name=name,
        yaml_path=yaml_path,
        validate=validate,
    )


__all__ = [
    "get_integration",
]
