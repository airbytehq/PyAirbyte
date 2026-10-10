# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for connector-base config validation error text."""

from __future__ import annotations

from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock

import jsonschema
import pytest

from airbyte._connector_base import _json_path_from_absolute_path
from airbyte.exceptions import AirbyteConnectorValidationFailedError
from airbyte.sources.base import Source


SECRET_VALUE = "sk_live_FAKE1234567890"


def test_validate_config_never_repeats_a_hydrated_secret() -> None:
    """A jsonschema failure quotes no hydrated config value anywhere."""
    spec = SimpleNamespace(
        connectionSpecification={
            "type": "object",
            "properties": {
                "credentials": {
                    "type": "object",
                    "properties": {
                        "api_key": {"type": "string", "pattern": "^pk_"},
                    },
                },
            },
        }
    )
    source = Source(executor=Mock(), name="test-source")
    source._spec = spec
    source.set_config({"credentials": {"api_key": SECRET_VALUE}}, validate=False)

    with pytest.raises(AirbyteConnectorValidationFailedError) as exc_info:
        source.validate_config()

    error = exc_info.value
    assert "not valid" in error.get_message()
    assert "credentials.api_key" in error.get_message()
    assert SECRET_VALUE not in str(error)
    assert SECRET_VALUE not in error.get_message()
    assert SECRET_VALUE not in str(error.context)


def test_json_path_from_absolute_path_matches_jsonschema() -> None:
    schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                },
            },
        },
    }
    error = next(
        jsonschema.Draft202012Validator(schema).iter_errors({"items": [{"name": 1}]})
    )

    assert _json_path_from_absolute_path(error.absolute_path) == error.json_path


def test_json_path_from_absolute_path_uses_only_path_components() -> None:
    error = jsonschema.ValidationError("invalid", path=deque(["a", 0, "b"]))

    assert _json_path_from_absolute_path(error.absolute_path) == "$.a[0].b"
