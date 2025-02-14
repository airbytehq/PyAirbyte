"""Tests for UV functionality and integration validation."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from airbyte._util.uv_util import (
    create_venv,
    get_executable_path,
    get_venv_env,
    install_package,
    run_uv_command,
)


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
    install_package(
        temp_venv, "black"
    )  # Use black since it's a common tool that creates executables

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
