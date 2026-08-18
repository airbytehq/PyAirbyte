# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Regression tests for console script discovery.

See https://github.com/airbytehq/PyAirbyte/issues/290.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from airbyte._executors.python import VenvExecutor

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO_ROOT / "tests/integration_tests/fixtures"


@pytest.fixture(autouse=True)
def _use_uv_for_install(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local connector installs require uv in this environment."""
    monkeypatch.setattr("airbyte._executors.python.NO_UV", False)


@pytest.mark.parametrize(
    "fixture_name, expected_script_name",
    [
        pytest.param(
            "source-wrong-exe", "wrong-script-name", id="exact-distribution-name"
        ),
        pytest.param(
            "source-wrong-exe-normalized",
            "normalized-script-name",
            id="normalized-distribution-name",
        ),
    ],
)
def test_discovers_installed_console_script_with_different_name(
    tmp_path: Path,
    fixture_name: str,
    expected_script_name: str,
) -> None:
    executor = VenvExecutor(
        name="source-wrong-exe",
        pip_url=str(FIXTURE_DIR / fixture_name),
        install_root=tmp_path,
    )

    executor.install()
    executor.ensure_installation()

    assert executor._resolve_console_script_name() == expected_script_name  # noqa: SLF001
    assert executor._get_connector_path().name == expected_script_name  # noqa: SLF001

    executor._get_connector_path().unlink()  # noqa: SLF001
    assert executor._resolve_console_script_name() is None  # noqa: SLF001


def test_declines_ambiguous_console_script_discovery(tmp_path: Path) -> None:
    executor = VenvExecutor(
        name="source-wrong-exe",
        pip_url=str(FIXTURE_DIR / "source-wrong-exe-ambiguous"),
        install_root=tmp_path,
    )

    executor.install()

    assert executor._discover_console_script_name() == [  # noqa: SLF001
        "helper-script-a",
        "helper-script-b",
    ]
    assert executor._resolve_console_script_name() is None  # noqa: SLF001
