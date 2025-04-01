# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

from airbyte import exceptions as exc
from airbyte._executors.base import Executor
from airbyte.constants import TEMP_DIR_OVERRIDE


if TYPE_CHECKING:
    from airbyte.http_caching.cache import AirbyteConnectorCache


logger = logging.getLogger("airbyte")


DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR = "/airbyte/tmp"
"""Default temp dir in an Airbyte connector's Docker image."""


class DockerExecutor(Executor):
    def __init__(
        self,
        name: str,
        docker_image: str,
        *,
        use_host_network: bool = False,
        http_cache: AirbyteConnectorCache | None = None,
    ) -> None:
        self.docker_image: str = docker_image
        self.use_host_network: bool = use_host_network

        local_mount_dir = Path().absolute() / name
        local_mount_dir.mkdir(exist_ok=True)
        self.volumes = {
            local_mount_dir: "/local",
            (TEMP_DIR_OVERRIDE or Path(tempfile.gettempdir())): DEFAULT_AIRBYTE_CONTAINER_TEMP_DIR,
        }

        super().__init__(
            name=name or self.docker_image,
            http_cache=http_cache,
        )

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

    def install(self) -> NoReturn:
        raise exc.AirbyteConnectorInstallationError(
            message="Connector cannot be installed because it is not managed by PyAirbyte.",
            connector_name=self.name,
        )

    def uninstall(self) -> NoReturn:
        raise exc.AirbyteConnectorInstallationError(
            message="Connector cannot be uninstalled because it is not managed by PyAirbyte.",
            connector_name=self.name,
        )

    @property
    def _cli(self) -> list[str]:
        """Get the executable CLI command.

        For Docker executors, this is the `docker run` command with the necessary arguments
        to run the connector in a Docker container. This will be extended (suffixed) by the
        connector's CLI commands and arguments, such as 'spec' or 'check --config=...'.
        """
        result: list[str] = [
            "docker",
            "run",
            "--rm",
            "-i",
        ]
        for local_dir, container_dir in self.volumes.items():
            result.extend(["--volume", f"{local_dir}:{container_dir}"])

        if self.http_cache:
            for key, value in self.http_cache.get_env_vars().items():
                result.extend(["-e", f"{key}={value}"])

        if self.use_host_network:
            result.extend(["--network", "host"])

        result.append(self.docker_image)
        return result

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
