# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Secret hydration for Airbyte connectors."""

from collections.abc import Iterator
from copy import deepcopy
from functools import lru_cache
from typing import Any, cast

import requests
import yaml

from airbyte.constants import AIRBYTE_OFFLINE_MODE, SECRETS_HYDRATION_PREFIX
from airbyte.exceptions import PyAirbyteInternalError
from airbyte.secrets.util import get_secret


GLOBAL_MASK_KEYS_URL = "https://connectors.airbyte.com/files/registries/v0/specs_secrets_mask.yaml"


def _hydrate_recursive(
    config: dict,
) -> None:
    """Recursively hydrate secrets in the given configuration dictionary.

    Args:
        config (dict): The configuration dictionary to hydrate.
    """
    for key, value in config.items():
        if isinstance(value, dict):
            _hydrate_recursive(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _hydrate_recursive(item)
        elif isinstance(value, str) and value.startswith(SECRETS_HYDRATION_PREFIX):
            # Assuming the value is a secret reference
            config[key] = get_secret(value)  # Replace with actual secret fetching logic


def hydrate_secrets(
    config: dict,
) -> dict:
    """Hydrate secrets in the given configuration dictionary.

    A new dictionary is created with secrets resolved, leaving the original unchanged.

    Args:
        config (dict): The configuration dictionary to hydrate.

    Returns:
        dict: The hydrated configuration dictionary with secrets resolved.
    """
    result: dict = deepcopy(config)
    _hydrate_recursive(result)
    return result


def deep_update(
    target: dict,
    source: dict,
) -> None:
    """Recursively update the target dictionary with values from the source dictionary.

    This is most often used to merge a non-secret configuration with a secret one, allowing
    users to keep non-secret config in source control while secrets are stored securely in
    a second configuration file or in environment variables.
    """
    for key, value in source.items():
        if isinstance(value, dict) and key in target and isinstance(target[key], dict):
            deep_update(target[key], value)
        else:
            target[key] = value


def _walk_dict(
    d: dict[str, Any],
    breadcrumb: list[str] | None = None,
) -> Iterator[tuple[list[str], Any]]:
    """Walk through a dictionary and yield paths to each leaf node.

    Yields tuples of (path, value) where path is a list of keys leading to the value.
    """
    breadcrumb = breadcrumb or []
    for key, val in d.items():
        new_path: list[str] = [*breadcrumb, key]
        if isinstance(val, dict):
            yield from _walk_dict(val, new_path)
        else:
            yield new_path, val


@lru_cache
def _get_global_secrets_mask() -> list[str]:
    """Get the list of properties to mask from the spec mask file."""
    if AIRBYTE_OFFLINE_MODE:
        # In offline mode, we cannot fetch the global mask keys.
        # We return an empty list to avoid masking any keys.
        return []

    response = requests.get(
        GLOBAL_MASK_KEYS_URL,
        allow_redirects=True,
    )
    if not response.ok:
        raise PyAirbyteInternalError(
            "Failed to parse spec mask.",
            log_text=response.content.decode("utf-8"),
        ) from None

    try:
        return cast("list[str]", yaml.safe_load(response.content)["properties"])

    except Exception as ex:
        raise PyAirbyteInternalError(
            "Failed to parse spec mask.",
            original_exception=ex,
        ) from None


def _get_connector_secrets_mask(
    spec_json_schema: dict[str, Any],
) -> list[str]:
    """Get the list of properties to mask from the connector spec."""
    result: list[str] = []
    for field_keys, field_value in _walk_dict(spec_json_schema):
        if isinstance(field_value, dict):
            is_secret: bool = any(
                (
                    field_value.get("writeOnly") is True,
                    field_value.get("format") == "password",
                    field_value.get("airbyte_secret") is True,
                )
            )
            if is_secret:
                result.append(field_keys[-1])

    return result


def detect_hardcoded_secrets(
    config: dict[str, Any],
    spec_json_schema: dict[str, Any] | None = None,
) -> list[list[str]]:
    """Detect hardcoded secrets in config.

    Apply this check before transmitting config over unsecured channels, to understand
    if the configuration contains hardcoded secrets.

    The recommended practice is to use environment variables or a secrets manager
    to store sensitive information, rather than hardcoding them in the configuration.

    To pass by reference, instead use the pattern `secret_reference::SECRET_NAME`,
    where `SECRET_NAME` is the name of the environment variable or a secret known
    by a registered secrets manager.
    """
    hardcoded_secrets: list[list[str]] = []
    secrets_mask: list[str] = (
        _get_global_secrets_mask()
        if spec_json_schema is None
        else _get_connector_secrets_mask(spec_json_schema)
    )

    for field_keys, field_value in _walk_dict(config):
        if (
            any(field_key in secrets_mask for field_key in field_keys)
            and isinstance(field_value, str)
            and not field_value.startswith(SECRETS_HYDRATION_PREFIX)
        ):
            hardcoded_secrets.append(field_keys)

    return hardcoded_secrets
