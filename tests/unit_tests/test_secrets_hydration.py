# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for recursive secret hydration in connector configurations."""

from copy import deepcopy

import pytest

from airbyte.exceptions import PyAirbyteSecretNotFoundError
from airbyte.secrets import config as secrets_config
from airbyte.secrets.base import SecretString
from airbyte.secrets.env_vars import EnvVarSecretManager
from airbyte.secrets.hydration import hydrate_secrets


@pytest.fixture(autouse=True)
def _env_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(secrets_config, "_SECRETS_SOURCES", [EnvVarSecretManager()])
    monkeypatch.setenv("PYAIRBYTE_HYDRATION_TEST_TOKEN", "offline-token")
    monkeypatch.delenv("PYAIRBYTE_HYDRATION_TEST_MISSING", raising=False)


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        (
            {"token": "secret_reference::PYAIRBYTE_HYDRATION_TEST_TOKEN"},
            {"token": "offline-token"},
        ),
        (
            {"tokens": ["secret_reference::PYAIRBYTE_HYDRATION_TEST_TOKEN"]},
            {"tokens": ["offline-token"]},
        ),
        (
            {
                "tokens": [
                    ["secret_reference::PYAIRBYTE_HYDRATION_TEST_TOKEN"],
                    {
                        "alternates": [
                            "secret_reference::PYAIRBYTE_HYDRATION_TEST_TOKEN"
                        ]
                    },
                ],
            },
            {"tokens": [["offline-token"], {"alternates": ["offline-token"]}]},
        ),
    ],
    ids=["dict-value", "list-value", "nested-lists-and-dicts"],
)
def test_hydrate_secrets_resolves_references_without_mutating_input(
    config: dict,
    expected: dict,
) -> None:
    original = deepcopy(config)

    assert hydrate_secrets(config) == expected
    assert config == original


def test_hydrate_secrets_masks_resolved_list_values() -> None:
    result = hydrate_secrets({
        "tokens": ["secret_reference::PYAIRBYTE_HYDRATION_TEST_TOKEN"]
    })

    assert isinstance(result["tokens"][0], SecretString)
    assert str(result["tokens"][0]) == "offline-token"
    assert "offline-token" not in repr(result)


@pytest.mark.parametrize(
    "config",
    [
        {"token": "secret_reference::PYAIRBYTE_HYDRATION_TEST_MISSING"},
        {"tokens": ["secret_reference::PYAIRBYTE_HYDRATION_TEST_MISSING"]},
        {"tokens": [["secret_reference::PYAIRBYTE_HYDRATION_TEST_MISSING"]]},
    ],
    ids=["dict-value", "list-value", "nested-list-value"],
)
def test_hydrate_secrets_rejects_missing_references(config: dict) -> None:
    with pytest.raises(PyAirbyteSecretNotFoundError):
        hydrate_secrets(config)


def test_hydrate_secrets_preserves_non_secret_list_values() -> None:
    config = {
        "values": [None, False, 0, 2, "ordinary", "a secret_reference::literal", [], {}]
    }

    assert hydrate_secrets(config) == config
