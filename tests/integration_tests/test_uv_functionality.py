"""Tests for UV functionality and integration validation."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


# TODO: Move these utility functions to a dedicated module under "utils"
def run_uv_command(args: list[str], env: dict | None = None, check: bool = False) -> subprocess.CompletedProcess:
    """Run a UV command and return the result.

    Args:
        args: Command arguments to pass to UV
        env: Optional environment variables
        check: Whether to raise an exception on non-zero return code

    Returns:
        CompletedProcess instance with command output
    """
    return subprocess.run(
        ["uv", *args],
        capture_output=True,
        text=True,
        env=env,
        check=check,
    )


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
    run_uv_command(args, check=True)


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
        check=True,
    )


def get_venv_env(venv_path: Path) -> dict:
    """Get environment variables for a virtual environment.

    Args:
        venv_path: Path to the virtual environment

    Returns:
        Dict of environment variables
    """
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv_path)
    env["PATH"] = f"{venv_path}/bin:{os.environ['PATH']}"
    return env


def get_executable_path(venv_path: Path, executable: str) -> Path:
    """Get the path to an executable in a virtual environment.

    Args:
        venv_path: Path to the virtual environment
        executable: Name of the executable

    Returns:
        Path to the executable
    """
    bin_dir = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    return venv_path / bin_dir / f"{executable}{suffix}"


def test_uv_available() -> None:
    """Test that UV is available on the command line."""
    result = run_uv_command(["--version"])
    assert result.returncode == 0
    assert "uv" in result.stdout.lower()


@pytest.fixture
def temp_venv(tmp_path: Path) -> Path:
    """Create a temporary directory for virtual environment testing.

    Uses pytest's tmp_path fixture which handles Windows cleanup properly.
    """
    venv_path = tmp_path / ".venv"
    return venv_path


def test_uv_venv_creation(temp_venv: Path) -> None:
    """Test that UV can create a virtual environment."""
    result = run_uv_command(["venv", str(temp_venv)])
    assert result.returncode == 0
    assert temp_venv.exists()
    assert (temp_venv / "bin" / "python").exists()


def test_uv_package_install(temp_venv: Path) -> None:
    """Test that UV can install a package and we can execute it."""
    # Create venv and install black
    create_venv(temp_venv, with_pip=True)
    install_package(temp_venv, "black")  # Use black since it's a common tool that creates executables

    # Verify package executable exists and is runnable
    black_path = get_executable_path(temp_venv, "black")
    assert black_path.exists()

    # Try running the package
    result = subprocess.run(
        [str(black_path), "--version"],
        env=get_venv_env(temp_venv),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "black" in result.stdout.lower()
