# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import shutil
import sys
import tempfile
from json import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING, cast

import requests
import yaml
from rich import print

from airbyte import exceptions as exc
from airbyte._executors.declarative import DeclarativeExecutor
from airbyte._executors.docker import DockerExecutor
from airbyte._executors.local import PathExecutor
from airbyte._executors.python import VenvExecutor
from airbyte._util.telemetry import EventState, log_install_state  # Non-public API
from airbyte.sources.registry import ConnectorMetadata, get_connector_metadata


if TYPE_CHECKING:
    from airbyte._executors.base import Executor


def get_connector_executor(  # noqa: PLR0912, PLR0913, PLR0915 # Too complex
    name: str,
    *,
    version: str | None = None,
    pip_url: str | None = None,
    local_executable: Path | str | None = None,
    docker_image: bool | str = False,
    use_host_network: bool = False,
    source_manifest: bool | dict | Path | str = False,
    install_if_missing: bool = True,
    install_root: Path | None = None,
) -> Executor:
    """This factory function creates an executor for a connector.

    For documentation of each arg, see the function `airbyte.sources.util.get_source()`.
    """
    if (
        sum(
            [
                bool(local_executable),
                bool(docker_image),
                bool(pip_url),
                bool(source_manifest),
            ]
        )
        > 1
    ):
        raise exc.PyAirbyteInputError(
            message=(
                "You can only specify one of the settings: 'local_executable', 'docker_image', "
                "'source_manifest', or 'pip_url'."
            ),
            context={
                "local_executable": local_executable,
                "docker_image": docker_image,
                "pip_url": pip_url,
                "source_manifest": source_manifest,
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
                return PathExecutor(
                    name=name,
                    path=local_executable,
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
        local_mount_dir = Path().absolute() / name
        local_mount_dir.mkdir(exist_ok=True)

        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--volume",
            f"{local_mount_dir}:/local/",
            "--volume",
            f"{temp_dir}:{temp_dir}",
        ]

        if use_host_network is True:
            docker_cmd.extend(["--network", "host"])

        docker_cmd.extend([docker_image])

        return DockerExecutor(
            name=name,
            executable=docker_cmd,
        )

    if source_manifest:
        if source_manifest is True:
            # Auto-set the manifest to a valid http address URL string
            source_manifest = (
                "https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations"
                f"/connectors/{name}/{name.replace('-', '_')}/manifest.yaml"
            )
        if isinstance(source_manifest, str):
            print("Installing connector from YAML manifest:", source_manifest)
            # Download the manifest file
            response = requests.get(url=source_manifest)
            response.raise_for_status()  # Raise an exception if the download failed

            if "class_name:" in response.text:
                raise exc.AirbyteConnectorInstallationError(
                    message=(
                        "The provided manifest requires additional code files (`class_name` key "
                        "detected). This feature is not compatible with the declarative YAML "
                        "executor. To use this executor, please try again with the Python "
                        "executor."
                    ),
                    connector_name=name,
                    context={
                        "manifest_url": source_manifest,
                    },
                )

            try:
                source_manifest = cast(dict, yaml.safe_load(response.text))
            except JSONDecodeError as ex:
                raise exc.AirbyteConnectorInstallationError(
                    connector_name=name,
                    context={
                        "manifest_url": source_manifest,
                    },
                ) from ex

        if isinstance(source_manifest, Path):
            source_manifest = cast(dict, yaml.safe_load(source_manifest.read_text()))

        # Source manifest is a dict at this point
        return DeclarativeExecutor(
            manifest=source_manifest,
        )

    # else: we are installing a connector in a Python virtual environment:

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
            install_root=install_root,
        )
        if install_if_missing:
            executor.ensure_installation()

    except Exception as e:
        log_install_state(name, state=EventState.FAILED, exception=e)
        raise
    else:
        # No exceptions were raised, so return the executor.
        return executor
