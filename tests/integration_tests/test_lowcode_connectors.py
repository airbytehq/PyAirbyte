# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path

import airbyte
import jsonschema
import pytest
from airbyte import exceptions as exc
from airbyte import get_source
from airbyte.sources.registry import (
    _LOWCODE_CDK_FILE_NOT_FOUND_ERRORS,
    _LOWCODE_CONNECTORS_FAILING_VALIDATION,
    _LOWCODE_CONNECTORS_NEEDING_PYTHON,
    _LOWCODE_CONNECTORS_UNEXPECTED_ERRORS,
)

UNIT_TEST_DB_PATH: Path = Path(".cache") / "unit_tests" / "test_db.duckdb"


# This goes stale often, such as when python code is added to a no-code connector.
@pytest.mark.flaky(reruns=0)
@pytest.mark.parametrize(
    "connector_name",
    airbyte.get_available_connectors(install_type="yaml"),
)
def test_nocode_connectors_setup(connector_name: str) -> None:
    """Test that all connectors can be initialized.

    If a specific connector fails to initialize, it should be added to the
    hardcoded failure list (tested below).
    """
    try:
        source = get_source(
            name=connector_name,
            source_manifest=True,
        )
        _ = source.config_spec
    except Exception as ex:
        raise AssertionError(
            f"Expected '{connector_name}' init success but got '{type(ex).__name__}'."
            f"You may need to update the `_LOWCODE_CONNECTORS_NEEDING_PYTHON` declaration. \n{ex}"
        )


# This goes stale often, such as when low-code connectors are made fully no-code.
@pytest.mark.flaky(reruns=0)
@pytest.mark.parametrize(
    "failure_group, exception_type",
    [
        (_LOWCODE_CONNECTORS_FAILING_VALIDATION, jsonschema.exceptions.ValidationError),
        (_LOWCODE_CONNECTORS_NEEDING_PYTHON, exc.AirbyteConnectorInstallationError),
        (_LOWCODE_CONNECTORS_UNEXPECTED_ERRORS, Exception),
        (_LOWCODE_CDK_FILE_NOT_FOUND_ERRORS, exc.AirbyteConnectorFailedError),
    ],
)
def test_expected_hardcoded_failures(
    failure_group,
    exception_type: str,
) -> None:
    """Test that hardcoded failure groups are failing as expected.

    If a connector starts passing, this is probably good news, and it should be removed from the
    hardcoded failure list.
    """
    for connector_name in failure_group:
        try:
            source = get_source(
                name=connector_name,
                source_manifest=True,
            )
            _ = source.config_spec
        except Exception as ex:
            if isinstance(ex, exception_type):
                pass
            else:
                raise AssertionError(
                    f"Expected connector {connector_name} to fail with"
                    f" '{exception_type}' but got '{type(ex).__name__}'. "
                )
        else:
            raise AssertionError(
                f"Expected connector {connector_name} to fail with"
                f" '{exception_type}' but got no exception. "
                "This probably means you need to remove this connector from the"
                " hardcoded failure list."
            )
