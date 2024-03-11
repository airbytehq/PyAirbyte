"""Python Source."""

from __future__ import annotations

from typing import Any

from airbyte import exceptions as exc
from airbyte._executor import VenvExecutor
from airbyte.sources.cli import CLISource
from airbyte.sources.registry import ConnectorMetadata, get_connector_metadata


class PythonSource(CLISource):
    """Python Source class."""

    def __init__(
        self,
        pip_url: str | None,
        version: str | None,
        name: str,
        config: dict[str, Any] | None = None,
        streams: str | list[str] | None = None,
        *,
        validate: bool = False,
        install_if_missing: bool = True,
    ) -> None:
        metadata: ConnectorMetadata | None = None
        try:
            metadata = get_connector_metadata(name)
        except exc.AirbyteConnectorNotRegisteredError:
            if not pip_url:
                # We don't have a pip url or registry entry, so we can't install the connector
                raise

        executor = VenvExecutor(
            name=name,
            metadata=metadata,
            target_version=version,
            pip_url=pip_url,
        )
        if install_if_missing:
            executor.ensure_installation()

        super().__init__(
            executor=executor,
            name=name,
            config=config,
            streams=streams,
            validate=validate,
        )
