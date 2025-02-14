"""Tests for UV functionality and integration validation."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def test_uv_available() -> None:
    """Test that UV is available on the command line."""
    result = subprocess.run(
        ["uv", "--version"],
        capture_output=True,
        text=True,
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
        ["uv", "venv", str(temp_venv)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert temp_venv.exists()
    assert (temp_venv / "bin" / "python").exists()


def test_uv_package_install(temp_venv: Path) -> None:
    """Test that UV can install a package and we can execute it."""
    # Create venv with seed packages (pip, setuptools, wheel)
    subprocess.run(
        ["uv", "venv", "--seed", str(temp_venv)],
        check=True,
    )

    # Set up environment variables for the virtual environment
    venv_env = os.environ.copy()
    venv_env["VIRTUAL_ENV"] = str(temp_venv)
    venv_env["PATH"] = f"{temp_venv}/bin:{os.environ['PATH']}"

    # Install package using UV directly
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "black",
        ],  # Use black since it's a common tool that creates executables
        env=venv_env,
        capture_output=True,
        text=True,
        check=True,
    )

    # Verify package executable exists and is runnable
    bin_dir = "Scripts" if os.name == "nt" else "bin"
    black_path = temp_venv / bin_dir / ("black.exe" if os.name == "nt" else "black")
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
