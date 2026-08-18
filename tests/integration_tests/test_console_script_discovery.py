# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Regression tests for console script discovery.

See https://github.com/airbytehq/PyAirbyte/issues/290.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from airbyte._executors.python import VenvExecutor

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO_ROOT / "tests/integration_tests/fixtures/source-wrong-exe"


@pytest.fixture(autouse=True)
def _use_uv_for_install(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local connector installs require uv in this environment."""
    monkeypatch.setattr("airbyte._executors.python.NO_UV", False)


def test_discovers_installed_console_script_with_different_name(tmp_path: Path) -> None:
    executor = VenvExecutor(
        name="source-wrong-exe",
        pip_url=str(FIXTURE_DIR),
        install_root=tmp_path,
    )

    executor.install()
    executor.ensure_installation()

    assert executor._resolve_console_script_name() == "wrong-script-name"  # noqa: SLF001
    assert executor._get_connector_path().name == "wrong-script-name"  # noqa: SLF001
