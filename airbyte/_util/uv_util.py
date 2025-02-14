"""Utility functions for UV virtual environment management."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def run_uv_command(
    args: list[str],
    env: dict | None = None,
    raise_on_error: bool = True,
) -> subprocess.CompletedProcess:
    """Run a UV command and return the result.

    Args:
        args: Command arguments to pass to UV
        env: Optional environment variables
        raise_on_error: Whether to raise an exception on non-zero return code

    Returns:
        CompletedProcess instance with command output
    """
    result = subprocess.run(
        ["uv", *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if raise_on_error and result.returncode != 0:
        raise exc.AirbyteSubprocessFailedError(
            run_args=args,
            exit_code=result.returncode,
            log_text=result.stderr.decode("utf-8"),
        )
    return result


def create_venv(venv_path: Path, with_pip: bool = True) -> None:
    """Create a virtual environment using UV.

    Args:
        venv_path: Path where the virtual environment should be created
        with_pip: Whether to include pip and other seed packages
    """
    args = ["venv"]
    if with_pip:
        args.append("--seed")
    args.append(str(venv_path))
    run_uv_command(args)


def install_package(venv_path: Path, pip_url: str) -> None:
    """Install a package into a virtual environment using UV.

    Args:
        venv_path: Path to the virtual environment
        pip_url: Package specification (name, URL, or path) to install
    """
    venv_env = get_venv_env(venv_path)
    run_uv_command(
        ["pip", "install", pip_url],
        env=venv_env,
    )


def get_venv_bin_path(venv_path: Path) -> Path:
    """Get the path of the executable 'bin' folder for a virtual env."""
    return venv_path / ("Scripts" if os.name == "nt" else "bin")


def get_venv_env(venv_path: Path) -> dict:
    """Get environment variables for a virtual environment.

    Args:
        venv_path: Path to the virtual environment

    Returns:
        Dict of environment variables
    """
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv_path)
    env["PATH"] = f"{get_venv_bin_path(venv_path)}:{os.environ['PATH']}"
    return env


def get_executable_path(venv_path: Path, executable: str) -> Path:
    """Get the path to an executable in a virtual environment.

    Args:
        venv_path: Path to the virtual environment
        executable: Name of the executable

    Returns:
        Path to the executable
    """
    suffix = ".exe" if os.name == "nt" else ""
    return get_venv_bin_path(venv_path) / f"{executable}{suffix}"
