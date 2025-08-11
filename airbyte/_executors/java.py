# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Java connector executor with automatic JRE management.

This module provides the JavaExecutor class for running Java-based Airbyte connectors.
It automatically downloads and manages Zulu JRE installations in ~/.airbyte/java directory
and handles connector tar file extraction and execution.

Fallback Logic:
- When use_java_tar=False: Java execution is explicitly disabled, fallback to Docker
- When use_java_tar=None and Docker available: Use Docker as the more stable option
- When use_java_tar=None and Docker unavailable: Use Java with automatic JRE download
- When use_java_tar is truthy: Use Java executor with specified or auto-detected tar file
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from overrides import overrides
from rich import print  # noqa: A004  # Allow shadowing the built-in

from airbyte import exceptions as exc
from airbyte._executors.base import Executor
from airbyte._util.telemetry import EventState, log_install_state
from airbyte.version import get_version


if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import IO

    from airbyte.sources.registry import ConnectorMetadata

from airbyte._message_iterators import AirbyteMessageIterator


class JavaExecutor(Executor):
    """An executor for Java connectors that manages JRE installation and execution."""

    def __init__(
        self,
        name: str | None = None,
        *,
        metadata: ConnectorMetadata | None = None,
        target_version: str | None = None,
        use_java: Path | str | bool | None = None,
        use_java_tar: Path | str | None = None,
    ) -> None:
        """Initialize a Java connector executor.

        Args:
            name: The name of the connector.
            metadata: (Optional.) The metadata of the connector.
            target_version: (Optional.) The version of the connector to install.
            use_java: (Optional.) Java execution mode: True/False to enable/disable,
                Path/str for custom JRE location, None for auto-detection.
            use_java_tar: (Optional.) Path to connector tar file. If provided,
                implies use_java=True unless use_java is explicitly False.
        """
        super().__init__(name=name, metadata=metadata, target_version=target_version)

        if use_java_tar is not None and use_java is None:
            use_java = True

        self.use_java = use_java
        self.connector_tar_path = Path(use_java_tar) if use_java_tar else None
        self.java_version = "21"
        self.airbyte_home = Path.home() / ".airbyte"
        self.java_cache_dir = self.airbyte_home / "java"

        self.os_name, self.arch = self._detect_platform()
        self.jre_dir = self.java_cache_dir / f"{self.os_name}-{self.arch}"

        self.connector_dir = self.airbyte_home / "connectors" / self.name

        if self.connector_tar_path is None and self.use_java:
            self.connector_tar_path = self._auto_detect_tar_path()

    def _auto_detect_tar_path(self) -> Path | None:
        """Auto-detect the connector tar file path."""
        return None

    def _detect_platform(self) -> tuple[str, str]:
        """Detect the current OS and architecture.

        Returns:
            Tuple of (os_name, architecture) normalized for Azul API.
        """
        system = platform.system().lower()
        if system == "darwin":
            os_name = "macos"
        elif system == "linux":
            os_name = "linux"
        else:
            raise exc.AirbyteConnectorInstallationError(
                message=f"Unsupported operating system: {system}",
                connector_name=self.name,
            )

        machine = platform.machine().lower()
        if machine in {"arm64", "aarch64"}:
            arch = "aarch64"
        elif machine in {"x86_64", "amd64"}:
            arch = "x64"
        else:
            raise exc.AirbyteConnectorInstallationError(
                message=f"Unsupported architecture: {machine}",
                connector_name=self.name,
            )

        return os_name, arch

    def _get_java_executable(self) -> Path:
        """Get the path to the Java executable.

        Returns:
            Path to the java executable.
        """
        java_home_env = os.environ.get("JAVA_HOME")
        if java_home_env:
            java_home = Path(java_home_env)
            java_executable = java_home / "bin" / "java"
            if java_executable.exists():
                print(f"✅ Using JAVA from JAVA_HOME: {java_home}")
                return java_executable

        java_executable = self.jre_dir / "bin" / "java"
        if java_executable.exists():
            print(f"✅ Using cached JRE: {self.jre_dir}")
            return java_executable

        print("🌐 JAVA not found — downloading portable JRE...")
        self._download_jre()
        return java_executable

    def _download_jre(self) -> None:
        """Download and extract JRE from Azul API."""
        print(f"⬇️  Downloading Zulu JRE {self.java_version} for {self.os_name}/{self.arch}...")

        self.jre_dir.mkdir(parents=True, exist_ok=True)

        azul_api_url = (
            f"https://api.azul.com/metadata/v1/zulu/packages/"
            f"?java_version={self.java_version}&os={self.os_name}&arch={self.arch}"
            f"&java_package_type=jre&release_status=ga&availability_types=CA"
        )

        try:
            response = requests.get(
                azul_api_url,
                headers={"User-Agent": f"PyAirbyte/{get_version()}"},
                timeout=30,
            )
            response.raise_for_status()
            azul_data = response.json()
        except requests.RequestException as ex:
            raise exc.AirbyteConnectorInstallationError(
                message="Failed to query Azul API for JRE download.",
                connector_name=self.name,
                context={
                    "azul_api_url": azul_api_url,
                    "os": self.os_name,
                    "arch": self.arch,
                },
            ) from ex

        jre_url = None
        for package in azul_data:
            download_url = package.get("download_url", "")
            if download_url.endswith(".tar.gz"):
                jre_url = download_url
                break

        if not jre_url:
            raise exc.AirbyteConnectorInstallationError(
                message="Failed to find JRE download URL from Azul API.",
                connector_name=self.name,
                context={
                    "azul_api_url": azul_api_url,
                    "os": self.os_name,
                    "arch": self.arch,
                    "available_packages": len(azul_data),
                },
            )

        print(f"🔗 JRE download URL: {jre_url}")
        self._download_and_extract_jre(jre_url)

    def _download_and_extract_jre(self, jre_url: str) -> None:
        """Download and extract JRE from the given URL."""
        try:
            response = requests.get(jre_url, stream=True, timeout=300)
            response.raise_for_status()

            with tarfile.open(fileobj=response.raw, mode="r|gz") as tar:
                self._extract_jre_with_strip_components(tar)

            print(f"✅ JRE downloaded and extracted to: {self.jre_dir}")

        except (requests.RequestException, tarfile.TarError) as ex:
            raise exc.AirbyteConnectorInstallationError(
                message="Failed to download or extract JRE.",
                connector_name=self.name,
                context={
                    "jre_url": jre_url,
                    "jre_dir": str(self.jre_dir),
                },
            ) from ex

    def _extract_jre_with_strip_components(self, tar: tarfile.TarFile) -> None:
        """Extract JRE tar with strip-components=1 equivalent."""
        members = tar.getmembers()
        if not members:
            return

        root_dir = members[0].name.split("/")[0]
        for member in members:
            if member.name.startswith(root_dir + "/"):
                member.name = member.name[len(root_dir) + 1 :]
                if member.name:  # Skip empty names
                    tar.extract(member, self.jre_dir)
            elif member.name == root_dir:
                continue
            else:
                tar.extract(member, self.jre_dir)

    def _extract_connector_tar(self) -> None:
        """Extract the connector tar file to the connector directory."""
        if not self.connector_tar_path or not self.connector_tar_path.exists():
            raise exc.AirbyteConnectorInstallationError(
                message="Connector tar file not found.",
                connector_name=self.name,
                context={
                    "connector_tar_path": str(self.connector_tar_path),
                },
            )

        print(f"📦 Extracting connector tar: {self.connector_tar_path}")

        self.connector_dir.mkdir(parents=True, exist_ok=True)

        try:
            with tarfile.open(self.connector_tar_path, "r") as tar:
                tar.extractall(self.connector_dir)

            print(f"✅ Connector extracted to: {self.connector_dir}")

        except tarfile.TarError as ex:
            raise exc.AirbyteConnectorInstallationError(
                message="Failed to extract connector tar file.",
                connector_name=self.name,
                context={
                    "connector_tar_path": str(self.connector_tar_path),
                    "connector_dir": str(self.connector_dir),
                },
            ) from ex

    def _get_connector_executable(self) -> Path:
        """Get the path to the connector executable."""
        app_dir = self.connector_dir / "airbyte-app"
        if app_dir.exists():
            bin_dir = app_dir / "bin"
            if bin_dir.exists():
                executable = bin_dir / self.name
                if executable.exists():
                    return executable

        for bin_dir in self.connector_dir.rglob("bin"):
            for executable in bin_dir.iterdir():
                if (
                    executable.is_file() and executable.stat().st_mode & 0o111
                ):  # Check if executable
                    return executable

        raise exc.AirbyteConnectorInstallationError(
            message="Could not find connector executable in extracted tar.",
            connector_name=self.name,
            context={
                "connector_dir": str(self.connector_dir),
                "expected_path": str(self.connector_dir / "airbyte-app" / "bin" / self.name),
            },
        )

    @overrides
    def install(self) -> None:
        """Install the Java connector by extracting tar and ensuring JRE is available."""
        try:
            self._extract_connector_tar()

            self._get_java_executable()

            log_install_state(self.name, state=EventState.SUCCEEDED)
            print(f"✅ Java connector '{self.name}' installed successfully!")

        except Exception as ex:
            log_install_state(self.name, state=EventState.FAILED, exception=ex)
            raise

    @overrides
    def uninstall(self) -> None:
        """Uninstall the Java connector by removing the connector directory."""
        if self.connector_dir.exists():
            shutil.rmtree(self.connector_dir)
            print(f"🗑️  Removed connector directory: {self.connector_dir}")

    @overrides
    def ensure_installation(self, *, auto_fix: bool = True) -> None:
        """Ensure that the Java connector is installed and JRE is available."""
        if not self.connector_dir.exists():
            if auto_fix:
                self.install()
            else:
                raise exc.AirbyteConnectorInstallationError(
                    message="Connector directory does not exist.",
                    connector_name=self.name,
                    context={"connector_dir": str(self.connector_dir)},
                )
        else:
            try:
                self._get_connector_executable()
                self._get_java_executable()
            except Exception as ex:
                if auto_fix:
                    self.install()
                else:
                    raise exc.AirbyteConnectorInstallationError(
                        message="Connector or JRE not properly installed.",
                        connector_name=self.name,
                    ) from ex

    @property
    def _cli(self) -> list[str]:
        """Get the base args of the CLI executable."""
        connector_executable = self._get_connector_executable()
        return [str(connector_executable)]

    @overrides
    def execute(
        self,
        args: list[str],
        *,
        stdin: IO[str] | AirbyteMessageIterator | None = None,
        suppress_stderr: bool = False,
    ) -> Iterator[str]:
        """Execute the Java connector with proper environment setup."""
        java_executable = self._get_java_executable()
        java_home = java_executable.parent.parent

        env = os.environ.copy()
        env["JAVA_HOME"] = str(java_home)
        env["PATH"] = f"{java_home / 'bin'}:{env.get('PATH', '')}"

        connector_executable = self._get_connector_executable()

        mapped_args = []
        for arg in args:
            if arg in {"spec", "check", "discover", "read"}:
                mapped_args.append(f"--{arg}")
            else:
                mapped_args.append(arg)

        cmd = [str(connector_executable), *mapped_args]

        print(f"🚀 Running Java connector: {' '.join(cmd)}")

        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if isinstance(stdin, AirbyteMessageIterator) else stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE if not suppress_stderr else subprocess.DEVNULL,
            universal_newlines=True,
            encoding="utf-8",
            env=env,
        )

        if isinstance(stdin, AirbyteMessageIterator):
            try:
                for message in stdin:
                    if process.stdin:
                        process.stdin.write(message.model_dump_json() + "\n")
                        process.stdin.flush()
                if process.stdin:
                    process.stdin.close()
            except BrokenPipeError:
                pass

        if process.stdout:
            try:
                while True:
                    line = process.stdout.readline()
                    if not line:
                        break
                    yield line.rstrip("\n\r")
            finally:
                process.stdout.close()

        exit_code = process.wait()

        if exit_code not in {0, -15}:
            stderr_output = ""
            if process.stderr:
                stderr_output = process.stderr.read()
                process.stderr.close()

            raise exc.AirbyteSubprocessFailedError(
                run_args=cmd,
                exit_code=exit_code,
                log_text=stderr_output,
            )
