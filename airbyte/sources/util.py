# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Utility functions for working with sources."""

from __future__ import annotations

import shutil
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

from airbyte import exceptions as exc
from airbyte._executor import DockerExecutor, PathExecutor, VenvExecutor
from airbyte._util.telemetry import EventState, log_install_state
from airbyte.sources.base import Source
from airbyte.sources.registry import ConnectorMetadata, get_connector_metadata


def get_connector(
    name: str,
    config: dict[str, Any] | None = None,
    *,
    version: str | None = None,
    pip_url: str | None = None,
    local_executable: Path | str | None = None,
    install_if_missing: bool = True,
) -> Source:
    """Deprecated. Use get_source instead."""
    warnings.warn(
        "The `get_connector()` function is deprecated and will be removed in a future version."
        "Please use `get_source()` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_source(
        name=name,
        config=config,
        version=version,
        pip_url=pip_url,
        local_executable=local_executable,
        install_if_missing=install_if_missing,
    )


# This non-public function includes the `docker_image` parameter, which is not exposed in the
# public API. See the `experimental` module for more info.
def _get_source(  # noqa: PLR0912  # Too many branches
    name: str,
    config: dict[str, Any] | None = None,
    *,
    streams: str | list[str] | None = None,
    version: str | None = None,
    pip_url: str | None = None,
    local_executable: Path | str | None = None,
    docker_image: str | bool = False,
    install_if_missing: bool = True,
) -> Source:
    """Get a connector by name and version.

    Args:
        name: connector name
        config: connector config - if not provided, you need to set it later via the set_config
            method.
        streams: list of stream names to select for reading. If set to "*", all streams will be
            selected. If not provided, you can set it later via the `select_streams()` or
            `select_all_streams()` method.
        version: connector version - if not provided, the currently installed version will be used.
            If no version is installed, the latest available version will be used. The version can
            also be set to "latest" to force the use of the latest available version.
        pip_url: connector pip URL - if not provided, the pip url will be inferred from the
            connector name.
        local_executable: If set, the connector will be assumed to already be installed and will be
            executed using this path or executable name. Otherwise, the connector will be installed
            automatically in a virtual environment.
        docker_image: If set, the connector will be executed using Docker. You can specify `True`
            to use the default image for the connector, or you can specify a custom image name.
            If `version` is specified and your image name does not already contain a tag
            (e.g. `my-image:latest`), the version will be appended as a tag (e.g. `my-image:0.1.0`).
        install_if_missing: Whether to install the connector if it is not available locally. This
            parameter is ignored when local_executable is set.
    """
    if sum([bool(local_executable), bool(docker_image), bool(pip_url)]) > 1:
        raise exc.PyAirbyteInputError(
            message=(
                "You can only specify one of the settings: 'local_executable', 'docker_image', "
                "or 'pip_url'."
            ),
            context={
                "local_executable": local_executable,
                "docker_image": docker_image,
                "pip_url": pip_url,
            },
        )

    if local_executable:
        if version:
            raise exc.PyAirbyteInputError(
                message="Param 'version' is not supported when 'local_executable' is set."
            )

        if isinstance(local_executable, str):
            if "/" in local_executable or "\\" in local_executable:
                # Assume this is a path
                local_executable = Path(local_executable).absolute()
            else:
                which_executable: str | None = None
                which_executable = shutil.which(local_executable)
                if not which_executable and sys.platform == "win32":
                    # Try with the .exe extension
                    local_executable = f"{local_executable}.exe"
                    which_executable = shutil.which(local_executable)

                if which_executable is None:
                    raise exc.AirbyteConnectorExecutableNotFoundError(
                        connector_name=name,
                        context={
                            "executable": local_executable,
                            "working_directory": Path.cwd().absolute(),
                        },
                    ) from FileNotFoundError(local_executable)
                local_executable = Path(which_executable).absolute()

        print(f"Using local `{name}` executable: {local_executable!s}")
        return Source(
            name=name,
            config=config,
            streams=streams,
            executor=PathExecutor(
                name=name,
                path=local_executable,
            ),
        )

    if docker_image:
        if docker_image is True:
            # Use the default image name for the connector
            docker_image = f"airbyte/{name}"

        if version is not None and ":" in docker_image:
            raise exc.PyAirbyteInputError(
                message="The 'version' parameter is not supported when a tag is already set in the "
                "'docker_image' parameter.",
                context={
                    "docker_image": docker_image,
                    "version": version,
                },
            )

        if ":" not in docker_image:
            docker_image = f"{docker_image}:{version or 'latest'}"

        temp_dir = tempfile.gettempdir()
        return Source(
            name=name,
            config=config,
            streams=streams,
            executor=DockerExecutor(
                name=name,
                executable=[
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "--volume",
                    f"{temp_dir}:{temp_dir}",
                    docker_image,
                ],
            ),
        )

    # else: we are installing a connector in a virtual environment:

    metadata: ConnectorMetadata | None = None
    try:
        metadata = get_connector_metadata(name)
    except exc.AirbyteConnectorNotRegisteredError as ex:
        if not pip_url:
            log_install_state(name, state=EventState.FAILED, exception=ex)
            # We don't have a pip url or registry entry, so we can't install the connector
            raise

    try:
        executor = VenvExecutor(
            name=name,
            metadata=metadata,
            target_version=version,
            pip_url=pip_url,
        )
        if install_if_missing:
            executor.ensure_installation()

        return Source(
            name=name,
            config=config,
            streams=streams,
            executor=executor,
        )
    except Exception as e:
        log_install_state(name, state=EventState.FAILED, exception=e)
        raise


# This thin wrapper exposes only non-experimental functions.
# Aka, exclude the `docker_image` parameter for now.
# See the `experimental` module for more info.
def get_source(
    name: str,
    config: dict[str, Any] | None = None,
    *,
    streams: str | list[str] | None = None,
    version: str | None = None,
    pip_url: str | None = None,
    local_executable: Path | str | None = None,
    install_if_missing: bool = True,
) -> Source:
    """Get a connector by name and version.

    Args:
        name: connector name
        config: connector config - if not provided, you need to set it later via the set_config
            method.
        streams: list of stream names to select for reading. If set to "*", all streams will be
            selected. If not provided, you can set it later via the `select_streams()` or
            `select_all_streams()` method.
        version: connector version - if not provided, the currently installed version will be used.
            If no version is installed, the latest available version will be used. The version can
            also be set to "latest" to force the use of the latest available version.
        pip_url: connector pip URL - if not provided, the pip url will be inferred from the
            connector name.
        local_executable: If set, the connector will be assumed to already be installed and will be
            executed using this path or executable name. Otherwise, the connector will be installed
            automatically in a virtual environment.
        install_if_missing: Whether to install the connector if it is not available locally. This
            parameter is ignored when local_executable is set.
    """
    return _get_source(
        name=name,
        config=config,
        streams=streams,
        version=version,
        pip_url=pip_url,
        local_executable=local_executable,
        install_if_missing=install_if_missing,
    )


__all__ = [
    "get_source",
]
