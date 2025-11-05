# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Helper functions for fetching connector specs from the registry."""

from __future__ import annotations

import logging
from typing import Any, Literal

import jsonschema
import requests

from airbyte import exceptions as exc
from airbyte.sources.registry import get_connector_metadata
from airbyte.version import get_version


logger = logging.getLogger("airbyte")

_SPEC_URL_TEMPLATE = "https://connectors.airbyte.com/files/metadata/airbyte/{connector_name}/{version}/{platform}.json"
_DEFAULT_TIMEOUT = 10  # seconds


def get_connector_spec_from_registry(
    connector_name: str,
    *,
    version: str | None = None,
    platform: Literal["cloud", "oss"] = "oss",
    timeout: int = _DEFAULT_TIMEOUT,
) -> dict[str, Any] | None:
    """Fetch connector spec from the registry.

    Args:
        connector_name: Name of the connector (e.g., "source-faker")
        version: Version of the connector. If None, uses latest_available_version from metadata.
        platform: Platform to fetch spec for ("cloud" or "oss"). Defaults to "oss".
        timeout: Timeout in seconds for the HTTP request. Defaults to 10.

    Returns:
        The connector spec JSON schema (connectionSpecification), or None if not found.

    Raises:
        AirbyteConnectorNotRegisteredError: If the connector is not found in the registry.
    """
    if version is None:
        metadata = get_connector_metadata(connector_name)
        if metadata is None:
            raise exc.AirbyteConnectorNotRegisteredError(
                connector_name=connector_name,
                guidance="Ensure the connector name is correct and the registry is accessible.",
            )
        version = metadata.latest_available_version
        if version is None:
            logger.warning(
                f"No version found for connector '{connector_name}' in registry metadata."
            )
            return None

    url = _SPEC_URL_TEMPLATE.format(
        connector_name=connector_name,
        version=version,
        platform=platform,
    )

    try:
        response = requests.get(
            url,
            headers={"User-Agent": f"PyAirbyte/{get_version()}"},
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()

        if "spec" in data and "connectionSpecification" in data["spec"]:
            return data["spec"]["connectionSpecification"]
        logger.warning(
            f"Spec for connector '{connector_name}' (version {version}, platform {platform}) "
            f"does not contain 'spec.connectionSpecification'."
        )
        return None  # noqa: TRY300

    except requests.exceptions.Timeout:
        logger.warning(
            f"Timeout fetching spec for connector '{connector_name}' "
            f"(version {version}, platform {platform}) from registry."
        )
        return None
    except requests.exceptions.RequestException as ex:
        logger.warning(
            f"Failed to fetch spec for connector '{connector_name}' "
            f"(version {version}, platform {platform}) from registry: {ex}"
        )
        return None
    except Exception as ex:
        logger.warning(
            f"Unexpected error fetching spec for connector '{connector_name}' "
            f"(version {version}, platform {platform}) from registry: {ex}"
        )
        return None


def validate_connector_config_from_registry(
    connector_name: str,
    config: dict[str, Any],
    *,
    version: str | None = None,
    platform: Literal["cloud", "oss"] = "oss",
    timeout: int = _DEFAULT_TIMEOUT,
) -> tuple[bool, str | None]:
    """Validate connector config against spec from registry.

    Args:
        connector_name: Name of the connector (e.g., "source-faker")
        config: Configuration dictionary to validate
        version: Version of the connector. If None, uses latest_available_version from metadata.
        platform: Platform to fetch spec for ("cloud" or "oss"). Defaults to "oss".
        timeout: Timeout in seconds for the HTTP request. Defaults to 10.

    Returns:
        Tuple of (is_valid, error_message). If valid, error_message is None.
    """
    spec = get_connector_spec_from_registry(
        connector_name,
        version=version,
        platform=platform,
        timeout=timeout,
    )

    if spec is None:
        return False, (
            f"Could not fetch spec for connector '{connector_name}' from registry. "
            f"Validation cannot be performed without the spec."
        )

    try:
        jsonschema.validate(instance=config, schema=spec)
    except jsonschema.ValidationError as ex:
        return False, f"Configuration validation failed: {ex.message}"
    except Exception as ex:
        return False, f"Unexpected error during validation: {ex}"
    else:
        return True, None


__all__ = [
    "get_connector_spec_from_registry",
    "validate_connector_config_from_registry",
]
