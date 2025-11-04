# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""NoOp executor for connectors that don't require local execution."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from airbyte import exceptions as exc
from airbyte._executors.base import Executor
from airbyte._util.registry_spec import get_connector_spec_from_registry


if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import IO

    from airbyte._message_iterators import AirbyteMessageIterator
    from airbyte.sources.registry import ConnectorMetadata


logger = logging.getLogger("airbyte")


class NoOpExecutor(Executor):
    """Executor that fetches specs from registry without local installation.

    This executor is useful for scenarios where you need to validate connector
    configurations but don't need to actually run the connector locally (e.g.,
    when deploying to Airbyte Cloud).

    The NoOpExecutor:
    - Fetches connector specs from the Airbyte registry
    - Supports the 'spec' command for configuration validation
    - Does not support execution commands (check, discover, read, write)
    - Does not require Docker or Python installation
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        metadata: ConnectorMetadata | None = None,
        target_version: str | None = None,
    ) -> None:
        """Initialize the NoOp executor."""
        super().__init__(
            name=name,
            metadata=metadata,
            target_version=target_version,
        )
        self._cached_spec: dict[str, Any] | None = None

    @property
    def _cli(self) -> list[str]:
        """Return a placeholder CLI command.

        This is never actually used since execute() is overridden.
        """
        return ["noop"]

    def execute(
        self,
        args: list[str],
        *,
        stdin: IO[str] | AirbyteMessageIterator | None = None,
        suppress_stderr: bool = False,
    ) -> Iterator[str]:
        """Execute a command and return an iterator of STDOUT lines.

        Only the 'spec' command is supported. Other commands will raise an error.
        """
        _ = stdin, suppress_stderr  # Unused

        if args == ["spec"]:
            if self._cached_spec is None:
                spec = get_connector_spec_from_registry(
                    self.name,
                    version=self.target_version,
                    platform="cloud",
                )
                if spec is None:
                    spec = get_connector_spec_from_registry(
                        self.name,
                        version=self.target_version,
                        platform="oss",
                    )

                if spec is None:
                    logger.warning(
                        f"Could not fetch spec for connector '{self.name}' from registry. "
                        f"Pre-validation will not be available."
                    )
                    yield from []
                    return

                self._cached_spec = spec

            spec_message = {
                "type": "SPEC",
                "spec": {
                    "connectionSpecification": self._cached_spec,
                },
            }
            yield json.dumps(spec_message)
            return

        raise exc.AirbyteConnectorExecutableNotFoundError(
            connector_name=self.name,
            guidance=(
                "NoOpExecutor only supports the 'spec' command for configuration validation. "
                "To run connector operations (check, discover, read), "
                "use a Docker or Python executor. "
                "For MCP cloud deployments, don't use no_executor=True "
                "if you need to run connector operations locally."
            ),
        )

    def ensure_installation(self, *, auto_fix: bool = True) -> None:
        """No-op: NoOpExecutor doesn't require installation."""
        _ = auto_fix  # Unused
        pass

    def install(self) -> None:
        """No-op: NoOpExecutor doesn't require installation."""
        pass

    def uninstall(self) -> None:
        """No-op: NoOpExecutor doesn't manage installations."""
        pass


__all__ = [
    "NoOpExecutor",
]
