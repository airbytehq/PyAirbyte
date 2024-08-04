# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Connectivity to the connector catalog registry."""

from __future__ import annotations

import json
import os
import warnings
from copy import copy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import requests

from airbyte import exceptions as exc
from airbyte.version import get_version


__cache: dict[str, ConnectorMetadata] | None = None


_REGISTRY_ENV_VAR = "AIRBYTE_LOCAL_REGISTRY"
_REGISTRY_URL = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"

_LOWCODE_LABEL = "cdk:low-code"

_LOWCODE_CONNECTORS_NEEDING_PYTHON: list[str] = [
    "source-adjust",
    "source-alpha-vantage",
    "source-amplitude",
    "source-apify-dataset",
    "source-asana",
    "source-avni",
    "source-aws-cloudtrail",
    "source-bamboo-hr",
    "source-braintree",
    "source-braze",
    "source-chargebee",
    "source-close-com",
    "source-commercetools",
    "source-facebook-pages",
    "source-fastbill",
    "source-freshdesk",
    "source-gitlab",
    "source-gnews",
    "source-greenhouse",
    "source-instagram",
    "source-instatus",
    "source-intercom",
    "source-iterable",
    "source-jina-ai-reader",
    "source-jira",
    "source-klaviyo",
    "source-mailchimp",
    "source-mixpanel",
    "source-monday",
    "source-my-hours",
    "source-notion",
    "source-okta",
    "source-orb",
    "source-outreach",
    "source-partnerstack",
    "source-paypal-transaction",
    "source-pinterest",
    "source-pipedrive",
    "source-pocket",
    "source-posthog",
    "source-prestashop",
    "source-public-apis",
    "source-qualaroo",
    "source-quickbooks",
    "source-railz",
    "source-recharge",
    "source-recurly",
    "source-retently",
    "source-rss",
    "source-salesloft",
    "source-slack",
    "source-surveymonkey",
    "source-tiktok-marketing",
    "source-the-guardian-api",
    "source-trello",
    "source-typeform",
    "source-xero",
    "source-younium",
    "source-zendesk-chat",
    "source-zendesk-sunshine",
    "source-zendesk-support",
    "source-zendesk-talk",
    "source-zenloop",
    "source-zoom",
]
_LOWCODE_CONNECTORS_FAILING_VALIDATION = [
    "source-amazon-ads",
]
# Connectors that return 404 or some other misc error.
_LOWCODE_CONNECTORS_404: list[str] = [
    "source-xkcd",
]
_LOWCODE_CONNECTORS_EXCLUDED: list[str] = [
    *_LOWCODE_CONNECTORS_FAILING_VALIDATION,
    *_LOWCODE_CONNECTORS_404,
    *_LOWCODE_CONNECTORS_NEEDING_PYTHON,
]


class InstallType(str, Enum):
    """The type of installation for a connector."""

    YAML = "yaml"
    PYTHON = "python"
    DOCKER = "docker"
    JAVA = "java"


class Language(str, Enum):
    """The language of a connector."""

    PYTHON = InstallType.PYTHON.value
    JAVA = InstallType.JAVA.value


@dataclass
class ConnectorMetadata:
    """Metadata for a connector."""

    name: str
    """Connector name. For example, "source-google-sheets"."""

    latest_available_version: str
    """The latest available version of the connector."""

    pypi_package_name: str | None
    """The name of the PyPI package for the connector, if it exists."""

    language: Language | None
    """The language of the connector."""

    install_types: set[InstallType]
    """The supported install types for the connector."""


def _get_registry_url() -> str:
    if _REGISTRY_ENV_VAR in os.environ:
        return str(os.environ.get(_REGISTRY_ENV_VAR))

    return _REGISTRY_URL


def _registry_entry_to_connector_metadata(entry: dict) -> ConnectorMetadata:
    name = entry["dockerRepository"].replace("airbyte/", "")
    language: Language | None = None
    if "language" in entry and entry["language"] is not None:
        try:
            language = Language(entry["language"])
        except Exception:
            warnings.warn(
                message=f"Invalid language for connector {name}: {entry['language']}",
                stacklevel=2,
            )
    remote_registries: dict = entry.get("remoteRegistries", {})
    pypi_registry: dict = remote_registries.get("pypi", {})
    pypi_package_name: str = pypi_registry.get("packageName", None)
    pypi_enabled: bool = pypi_registry.get("enabled", False)
    install_types: set[InstallType] = {
        x
        for x in [
            InstallType.DOCKER if entry.get("dockerImageTag") else None,
            InstallType.PYTHON if pypi_enabled else None,
            InstallType.JAVA if language == Language.JAVA else None,
            InstallType.YAML if _LOWCODE_LABEL in entry.get("tags", []) else None,
        ]
        if x
    }

    return ConnectorMetadata(
        name=name,
        latest_available_version=entry.get("dockerImageTag", None),
        pypi_package_name=pypi_package_name if pypi_enabled else None,
        language=language,
        install_types=install_types,
    )


def _get_registry_cache(*, force_refresh: bool = False) -> dict[str, ConnectorMetadata]:
    """Return the registry cache."""
    global __cache
    if __cache and not force_refresh:
        return __cache

    registry_url = _get_registry_url()
    if registry_url.startswith("http"):
        response = requests.get(
            registry_url, headers={"User-Agent": f"airbyte-lib-{get_version()}"}
        )
        response.raise_for_status()
        data = response.json()
    else:
        # Assume local file
        with Path(registry_url).open(encoding="utf-8") as f:
            data = json.load(f)

    new_cache: dict[str, ConnectorMetadata] = {}

    for connector in data["sources"]:
        connector_metadata = _registry_entry_to_connector_metadata(connector)
        new_cache[connector_metadata.name] = connector_metadata

    for connector in data["destinations"]:
        connector_metadata = _registry_entry_to_connector_metadata(connector)
        new_cache[connector_metadata.name] = connector_metadata

    if len(new_cache) == 0:
        raise exc.PyAirbyteInternalError(
            message="Connector registry is empty.",
            context={
                "registry_url": _get_registry_url(),
            },
        )

    __cache = new_cache
    return __cache


def get_connector_metadata(name: str) -> ConnectorMetadata:
    """Check the cache for the connector.

    If the cache is empty, populate by calling update_cache.
    """
    cache = copy(_get_registry_cache())
    if not cache:
        raise exc.PyAirbyteInternalError(
            message="Connector registry could not be loaded.",
            context={
                "registry_url": _get_registry_url(),
            },
        )
    if name not in cache:
        raise exc.AirbyteConnectorNotRegisteredError(
            connector_name=name,
            context={
                "registry_url": _get_registry_url(),
                "available_connectors": get_available_connectors(),
            },
        )
    return cache[name]


def get_available_connectors(install_type: InstallType | str = InstallType.PYTHON) -> list[str]:
    """Return a list of all available connectors.

    Connectors will be returned in alphabetical order, with the standard prefix "source-".
    """
    if not isinstance(install_type, InstallType):
        install_type = InstallType(install_type)

    if install_type == InstallType.PYTHON:
        return sorted(
            conn.name
            for conn in _get_registry_cache().values()
            if conn.pypi_package_name is not None
        )

    if install_type == InstallType.JAVA:
        warnings.warn(
            message="Java connectors are not yet supported.",
            stacklevel=2,
        )
        return sorted(
            conn.name for conn in _get_registry_cache().values() if conn.language == Language.JAVA
        )

    if install_type == InstallType.DOCKER:
        return sorted(conn.name for conn in _get_registry_cache().values())

    if install_type == InstallType.YAML:
        return sorted(
            conn.name
            for conn in _get_registry_cache().values()
            if InstallType.YAML in conn.install_types
            and conn.name not in _LOWCODE_CONNECTORS_EXCLUDED
        )

    # pragma: no cover  # Should never be reached.
    raise exc.PyAirbyteInputError(
        message="Invalid install type.",
        context={
            "install_type": install_type,
        },
    )
