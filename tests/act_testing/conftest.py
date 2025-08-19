"""
Pytest configuration and fixtures for ACT CLI testing.

This module provides shared fixtures and configuration for testing
GitHub Actions workflows with the ACT CLI.
"""

import pytest
import subprocess
from pathlib import Path


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "act_integration: marks tests as ACT CLI integration tests"
    )
    config.addinivalue_line(
        "markers", "requires_docker: marks tests that require Docker to be running"
    )


@pytest.fixture(scope="session")
def act_available():
    """Check if ACT CLI is available and working."""
    try:
        result = subprocess.run(
            ["act", "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


@pytest.fixture(scope="session")
def docker_available():
    """Check if Docker is available and running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


@pytest.fixture(scope="session")
def repo_root():
    """Get the repository root directory."""
    return Path(__file__).parent.parent.parent


def pytest_collection_modifyitems(config, items):
    """Modify test collection to skip tests based on requirements."""
    act_available = True
    docker_available = True
    
    try:
        subprocess.run(["act", "--version"], capture_output=True, timeout=5)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        act_available = False
    
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=5)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        docker_available = False
    
    skip_act = pytest.mark.skip(reason="ACT CLI not available")
    skip_docker = pytest.mark.skip(reason="Docker not available")
    
    for item in items:
        if "act_integration" in item.keywords and not act_available:
            item.add_marker(skip_act)
        if "requires_docker" in item.keywords and not docker_available:
            item.add_marker(skip_docker)
