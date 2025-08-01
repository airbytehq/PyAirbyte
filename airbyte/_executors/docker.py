# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

from airbyte import exceptions as exc
from airbyte._executors.base import Executor


logger = logging.getLogger("airbyte")


DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR = "/airbyte/tmp"
"""Default temp dir in an Airbyte connector's Docker image."""


class DockerExecutor(Executor):
    def __init__(
        self,
        name: str | None = None,
        *,
        executable: list[str],
        target_version: str | None = None,
        volumes: dict[Path, str] | None = None,
    ) -> None:
        self.executable: list[str] = executable
        self.volumes: dict[Path, str] = volumes or {}
        name = name or executable[0]
        super().__init__(name=name, target_version=target_version)

    def ensure_installation(
        self,
        *,
        auto_fix: bool = True,
    ) -> None:
        """Ensure that the connector executable can be found.

        The auto_fix parameter is ignored for this executor type.
        """
        _ = auto_fix
        try:
            assert (
                shutil.which("docker") is not None
            ), "Docker couldn't be found on your system. Please Install it."
            self.execute(["spec"])
        except Exception as e:
            raise exc.AirbyteConnectorExecutableNotFoundError(
                connector_name=self.name,
            ) from e

    def install(self) -> None:
        """Install the Docker image by pulling it."""
        docker_image = self.executable[-1]

        logger.info(f"Pulling Docker image: {docker_image}")

        try:
            result = subprocess.run(
                ["docker", "pull", docker_image],
                check=False,
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                raise exc.AirbyteConnectorInstallationError(
                    message=f"Failed to pull Docker image '{docker_image}'.",
                    connector_name=self.name,
                    context={
                        "docker_image": docker_image,
                        "exit_code": result.returncode,
                        "stderr": result.stderr,
                    },
                )

            logger.info(f"Successfully pulled Docker image: {docker_image}")

        except FileNotFoundError as e:
            raise exc.AirbyteConnectorInstallationError(
                message=(
                    "Docker command not found. Please ensure Docker is installed "
                    "and available in PATH."
                ),
                connector_name=self.name,
            ) from e

    def uninstall(self) -> None:
        """Uninstall the Docker image by removing it."""
        docker_image = self.executable[-1]

        logger.info(f"Removing Docker image: {docker_image}")

        try:
            check_result = subprocess.run(
                ["docker", "image", "inspect", docker_image],
                check=False,
                capture_output=True,
                text=True,
            )

            if check_result.returncode != 0:
                logger.info(f"Docker image '{docker_image}' not found locally, nothing to remove.")
                return

            result = subprocess.run(
                ["docker", "rmi", docker_image],
                check=False,
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                raise exc.AirbyteConnectorInstallationError(
                    message=f"Failed to remove Docker image '{docker_image}'.",
                    connector_name=self.name,
                    context={
                        "docker_image": docker_image,
                        "exit_code": result.returncode,
                        "stderr": result.stderr,
                    },
                )

            logger.info(f"Successfully removed Docker image: {docker_image}")

        except FileNotFoundError as e:
            raise exc.AirbyteConnectorInstallationError(
                message=(
                    "Docker command not found. Please ensure Docker is installed "
                    "and available in PATH."
                ),
                connector_name=self.name,
            ) from e

    @property
    def _cli(self) -> list[str]:
        """Get the base args of the CLI executable."""
        return self.executable

    def map_cli_args(self, args: list[str]) -> list[str]:
        """Map local file paths to the container's volume paths."""
        new_args = []
        for arg in args:
            if Path(arg).exists():
                # This is a file path and we need to map it to the same file within the
                # relative path of the file within the container's volume.
                for local_volume, container_path in self.volumes.items():
                    if Path(arg).is_relative_to(local_volume):
                        logger.debug(
                            f"Found file input path `{arg}` "
                            f"relative to container-mapped volume: {local_volume}"
                        )
                        mapped_path = Path(container_path) / Path(arg).relative_to(local_volume)
                        logger.debug(f"Mapping `{arg}` -> `{mapped_path}`")
                        new_args.append(str(mapped_path))
                        break
                else:
                    # No break reached; a volume was found for this file path
                    logger.warning(
                        f"File path `{arg}` is not relative to any volume path "
                        f"in the provided volume mappings: {self.volumes}. "
                        "The file may not be available to the container at runtime."
                    )
                    new_args.append(arg)

            else:
                new_args.append(arg)

        if args != new_args:
            logger.debug(
                f"Mapping local-to-container CLI args: {args} -> {new_args} "
                f"based upon volume definitions: {self.volumes}"
            )

        return new_args
