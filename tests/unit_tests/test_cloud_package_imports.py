# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Validate the lazily-initialized `airbyte.cloud` package surface."""

from __future__ import annotations

import subprocess
import sys

import pytest

import airbyte.cloud as cloud_pkg


CLOUD_SUBMODULES = [
    "client",
    "client_config",
    "connections",
    "connectors",
    "constants",
    "models",
    "organizations",
    "sync_results",
    "workspaces",
]


@pytest.mark.parametrize("name", sorted(cloud_pkg.__all__))
def test_cloud_public_names_resolve(name: str) -> None:
    """Every name in `airbyte.cloud.__all__` is importable from the package."""
    assert getattr(cloud_pkg, name) is not None
    assert name in dir(cloud_pkg)


def test_cloud_unknown_attribute_raises() -> None:
    with pytest.raises(AttributeError, match="no attribute 'NotAThing'"):
        _ = cloud_pkg.NotAThing  # type: ignore[attr-defined]


@pytest.mark.parametrize("submodule", CLOUD_SUBMODULES)
def test_cloud_submodules_import_in_fresh_interpreter(submodule: str) -> None:
    """Importing any submodule first must not trigger a circular import."""
    result = subprocess.run(
        [sys.executable, "-c", f"import airbyte.cloud.{submodule}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("submodule", CLOUD_SUBMODULES)
def test_cloud_submodules_resolve_as_package_attributes(submodule: str) -> None:
    """`airbyte.cloud.<submodule>` resolves after importing only the package."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import airbyte.cloud; import types; "
            f"assert isinstance(airbyte.cloud.{submodule}, types.ModuleType)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
