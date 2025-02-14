"""Tests for UV functionality and integration validation."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def test_uv_available() -> None:
    """Test that UV is available on the command line."""
    # Use poetry's virtual environment where UV is installed
    result = subprocess.run(
        ["poetry", "run", "uv", "--version"],
        capture_output=True,
        text=True,
        env=os.environ.copy(),  # Ensure we have the poetry environment
    )
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
    result = subprocess.run(
        ["poetry", "run", "uv", "venv", str(temp_venv)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert temp_venv.exists()
    assert (temp_venv / "bin" / "python").exists()


def test_uv_package_install(temp_venv: Path) -> None:
    """Test that UV can install a package and we can execute it."""
    env = os.environ.copy()  # Get poetry's environment

    # Create venv with seed packages (pip, setuptools, wheel)
    subprocess.run(
        ["poetry", "run", "uv", "venv", "--seed", str(temp_venv)],
        env=env,
        check=True,
    )

    # Install package
    venv_env = env.copy()
    venv_env["VIRTUAL_ENV"] = str(temp_venv)
    venv_env["PATH"] = f"{temp_venv}/bin:{env['PATH']}"

    # Install package using UV's pip interface
    install_env = env.copy()
    install_env["VIRTUAL_ENV"] = str(temp_venv)
    install_env["PATH"] = f"{temp_venv}/bin:{env['PATH']}"
    subprocess.run(
        [
            "poetry",
            "run",
            "uv",
            "pip",
            "install",
            "black",
        ],  # Use black since it's a common tool that creates executables
        env=install_env,
        capture_output=True,  # Capture output for debugging
        text=True,
        check=True,
    )

    # Verify package executable exists and is runnable
    black_path = temp_venv / "bin" / "black"
    assert black_path.exists()

    # Try running the package
    result = subprocess.run(
        [str(black_path), "--version"],
        env=venv_env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "black" in result.stdout.lower()
