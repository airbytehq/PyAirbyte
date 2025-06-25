"""Secret hydration for Airbyte connectors."""

from copy import deepcopy
from typing import cast

from airbyte.secrets import get_secret


HYDRATION_PREFIX = "secret_reference::"


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
        elif isinstance(value, str) and value.startswith(HYDRATION_PREFIX):
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
    result: dict = cast("dict", deepcopy(config))
    _hydrate_recursive(result)
    return result


def deep_update(
    target: dict,
    source: dict,
) -> None:
    """Recursively update the target dictionary with values from the source dictionary."""
    for key, value in source.items():
        if isinstance(value, dict) and key in target and isinstance(target[key], dict):
            deep_update(target[key], value)
        else:
            target[key] = value
