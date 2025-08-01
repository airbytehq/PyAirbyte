# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import pytest

from airbyte.sources.util import get_source
from airbyte import exceptions as exc


def test_install_failure_log_pypi():
    """Test that the install log is created and contains the expected content."""
    with pytest.raises(exc.AirbyteConnectorNotRegisteredError):
        source = get_source("source-not-found")

    with pytest.raises(exc.AirbyteConnectorInstallationError) as exc_info:
        source = get_source(
            "source-not-found",
            pip_url="https://pypi.org/project/airbyte-not-found",
            install_if_missing=True,
        )

    # Check that the stderr log contains the expected content from a failed install (pip or uv)
    err_msg = str(exc_info.value.__cause__.log_text)
    assert any([
        "Cannot unpack file" in err_msg,
        "Could not install requirement" in err_msg,
        "Failed to parse" in err_msg,
        "Expected direct URL" in err_msg,
    ])
