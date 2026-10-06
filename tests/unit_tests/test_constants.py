# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("env_value", "expected_no_uv"),
    [
        pytest.param(None, False, id="unset"),
        pytest.param("1", True, id="one"),
        pytest.param("true", True, id="true"),
        pytest.param("TRUE", True, id="mixed-case-true"),
        pytest.param("YeS", True, id="mixed-case-yes"),
        pytest.param("0", False, id="zero"),
        pytest.param("false", False, id="false"),
        pytest.param("no", False, id="no"),
        pytest.param("other", False, id="other"),
    ],
)
def test_no_uv_environment_mapping(env_value: str | None, expected_no_uv: bool) -> None:
    """Verify `AIRBYTE_NO_UV` opts out of uv only for explicit truthy values."""
    environment = os.environ.copy()
    if env_value is None:
        environment.pop("AIRBYTE_NO_UV", None)
    else:
        environment["AIRBYTE_NO_UV"] = env_value

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from airbyte.constants import NO_UV; print(NO_UV)",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
        cwd=Path(__file__).parents[2],
    )

    assert result.stdout.strip() == str(expected_no_uv)
