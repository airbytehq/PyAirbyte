# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Connectivity to the connector catalog registry."""

from __future__ import annotations

import json
import logging
import os
import warnings
from copy import copy
from enum import Enum
from pathlib import Path
from typing import Any, cast

import requests
import yaml
from pydantic import BaseModel, Field
from typing_extensions import Self

from airbyte import exceptions as exc
from airbyte._registry_utils import fetch_registry_version_date, parse_changelog_html
from airbyte._util.meta import is_docker_installed
from airbyte.constants import AIRBYTE_OFFLINE_MODE
from airbyte.logs import warn_once
from airbyte.version import get_version


logger = logging.getLogger("airbyte")


__cache: dict[str, ConnectorMetadata] | None = None


_REGISTRY_ENV_VAR = "AIRBYTE_LOCAL_REGISTRY"
_REGISTRY_URL = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"

_PYTHON_LANGUAGE = "python"
_MANIFEST_ONLY_LANGUAGE = "manifest-only"

_PYTHON_LANGUAGE_TAG = f"language:{_PYTHON_LANGUAGE}"
_MANIFEST_ONLY_TAG = f"language:{_MANIFEST_ONLY_LANGUAGE}"

_DEFAULT_METADATA_URL = (
    "https://connectors.airbyte.com/files/metadata/airbyte/{source_name}/{version}/metadata.yaml"
)


class InstallType(str, Enum):
    """The type of installation for a connector."""

    YAML = "yaml"
    """Manifest-only connectors that can be run without Docker."""
    PYTHON = "python"
    """Python-based connectors available via PyPI."""
    DOCKER = "docker"
    """Docker-based connectors (returns all connectors for backward compatibility)."""
    JAVA = "java"
    """Java-based connectors."""

    INSTALLABLE = "installable"
    """Connectors installable in the current environment (environment-sensitive).

    Returns all connectors if Docker is installed, otherwise only Python and YAML.
    """
    ANY = "any"
    """All connectors in the registry (environment-independent)."""


class Language(str, Enum):
    """The language of a connector."""

    PYTHON = InstallType.PYTHON.value
    JAVA = InstallType.JAVA.value
    MANIFEST_ONLY = _MANIFEST_ONLY_LANGUAGE


class ConnectorMetadata(BaseModel):
    """Metadata for a connector."""

    name: str
    """Connector name. For example, "source-google-sheets"."""

    latest_available_version: str | None
    """The latest available version of the connector."""

    pypi_package_name: str | None
    """The name of the PyPI package for the connector, if it exists."""

    language: Language | None
    """The language of the connector."""

    install_types: set[InstallType]
    """The supported install types for the connector."""

    suggested_streams: list[str] | None = None
    """A list of suggested streams for the connector, if available."""

    @property
    def default_install_type(self) -> InstallType:
        """Return the default install type for the connector."""
        if self.language == Language.MANIFEST_ONLY and InstallType.YAML in self.install_types:
            return InstallType.YAML

        if InstallType.PYTHON in self.install_types:
            return InstallType.PYTHON

        # Else: Java or Docker
        return InstallType.DOCKER


def _get_registry_url() -> str:
    if _REGISTRY_ENV_VAR in os.environ:
        return str(os.environ.get(_REGISTRY_ENV_VAR))

    return _REGISTRY_URL


def _is_registry_disabled(url: str) -> bool:
    return url.upper() in {"0", "F", "FALSE"} or AIRBYTE_OFFLINE_MODE


def _registry_entry_to_connector_metadata(entry: dict) -> ConnectorMetadata:
    name = entry["dockerRepository"].replace("airbyte/", "")
    latest_version: str | None = entry.get("dockerImageTag")
    tags = entry.get("tags", [])
    language: Language | None = None

    if "language" in entry and entry["language"] is not None:
        try:
            language = Language(entry["language"])
        except Exception:
            warnings.warn(
                message=f"Invalid language for connector {name}: {entry['language']}",
                stacklevel=2,
            )
    if not language and _PYTHON_LANGUAGE_TAG in tags:
        language = Language.PYTHON
    if not language and _MANIFEST_ONLY_TAG in tags:
        language = Language.MANIFEST_ONLY

    remote_registries: dict = entry.get("remoteRegistries", {})
    pypi_registry: dict = remote_registries.get("pypi", {})
    pypi_package_name = cast(
        "str | None",
        pypi_registry.get("packageName", None),
    )
    pypi_enabled: bool = pypi_registry.get("enabled", False)
    install_types: set[InstallType] = {
        x
        for x in [
            InstallType.DOCKER,  # Always True
            InstallType.PYTHON if language == Language.PYTHON and pypi_enabled else None,
            InstallType.JAVA if language == Language.JAVA else None,
            InstallType.YAML if language == Language.MANIFEST_ONLY else None,
        ]
        if x
    }

    return ConnectorMetadata(
        name=name,
        latest_available_version=latest_version,
        pypi_package_name=pypi_package_name if pypi_enabled else None,
        language=language,
        install_types=install_types,
        suggested_streams=entry.get("suggestedStreams", {}).get("streams", None),
    )


def _get_registry_cache(
    *,
    force_refresh: bool = False,
) -> dict[str, ConnectorMetadata]:
    """Return the registry cache.

    Result is a mapping of connector name to ConnectorMetadata.
    """
    global __cache
    if __cache and not force_refresh:
        return __cache

    registry_url = _get_registry_url()

    if _is_registry_disabled(registry_url):
        return {}

    if registry_url.startswith("http"):
        response = requests.get(
            registry_url,
            headers={"User-Agent": f"PyAirbyte/{get_version()}"},
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
        # This isn't necessarily fatal, since users can bring their own
        # connector definitions.
        warn_once(
            message=f"Connector registry is empty: {registry_url}",
            with_stack=False,
        )

    __cache = new_cache
    return __cache


def get_connector_metadata(name: str) -> ConnectorMetadata | None:
    """Check the cache for the connector.

    If the cache is empty, populate by calling update_cache.
    """
    registry_url = _get_registry_url()

    if _is_registry_disabled(registry_url):
        return None

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


def get_available_connectors(
    install_type: InstallType | str | None = InstallType.INSTALLABLE,
) -> list[str]:
    """Return a list of all available connectors.

    Connectors will be returned in alphabetical order, with the standard prefix "source-".

    Args:
        install_type: The type of installation for the connector.
            Defaults to `InstallType.INSTALLABLE`.
    """
    if install_type is None or install_type == InstallType.INSTALLABLE:
        # Filter for installable connectors (default behavior).
        if is_docker_installed():
            logger.info("Docker is detected. Returning all connectors.")
            return sorted(_get_registry_cache().keys())

        logger.info("Docker was not detected. Returning only Python and Manifest-only connectors.")
        return sorted(
            [
                connector_name
                for connector_name, conn_info in _get_registry_cache().items()
                if conn_info.language in {Language.PYTHON, Language.MANIFEST_ONLY}
            ]
        )

    if not isinstance(install_type, InstallType):
        install_type = InstallType(install_type)

    if install_type == InstallType.PYTHON:
        return sorted(
            connector_name
            for connector_name, conn_info in _get_registry_cache().items()
            if conn_info.pypi_package_name is not None
        )

    if install_type == InstallType.JAVA:
        warnings.warn(
            message="Java connectors are not yet supported.",
            stacklevel=2,
        )
        return sorted(
            connector_name
            for connector_name, conn_info in _get_registry_cache().items()
            if conn_info.language == Language.JAVA
        )

    if install_type in {InstallType.DOCKER, InstallType.ANY}:
        return sorted(_get_registry_cache().keys())

    if install_type == InstallType.YAML:
        return sorted(
            conn.name
            for conn in _get_registry_cache().values()
            if InstallType.YAML in conn.install_types
        )

    # pragma: no cover  # Should never be reached.
    raise exc.PyAirbyteInputError(
        message="Invalid install type.",
        context={
            "install_type": install_type,
        },
    )


class ConnectorVersionInfo(BaseModel):
    """Information about a specific connector version."""

    version: str
    release_date: str | None = None
    docker_image_url: str
    changelog_url: str
    pr_url: str | None = None
    pr_title: str | None = None
    parsing_errors: list[str] = Field(default_factory=list)


class ApiDocsUrl(BaseModel):
    """API documentation URL information."""

    title: str
    url: str
    source: str
    doc_type: str = Field(default="other", alias="type")
    requires_login: bool = Field(default=False, alias="requiresLogin")

    model_config = {"populate_by_name": True}

    @classmethod
    def from_metadata_docs_list(
        cls, docs: list[dict[str, Any]], *, source: str, context: str
    ) -> list[Self]:
        """Extract documentation URLs from a list of metadata documentation dictionaries.

        Args:
            docs: List of documentation dictionaries with 'title' and 'url' fields
            source: The source identifier for these documentation URLs
            context: Context string for error messages (e.g., "Metadata", "Registry")

        Returns:
            List of ApiDocsUrl objects extracted from the docs list

        Raises:
            PyAirbyteInternalError: If a documentation entry is missing required 'title' or
                'url' field
        """
        results: list[Self] = []
        for doc in docs:
            try:
                results.append(
                    cls(
                        title=doc["title"],
                        url=doc["url"],
                        source=source,
                        doc_type=doc.get("type", "other"),
                        requires_login=doc.get("requiresLogin", False),
                    )
                )
            except KeyError as e:
                raise exc.PyAirbyteInternalError(
                    message=f"{context} parsing error: missing required field in {doc}: {e}"
                ) from e
        return results

    @classmethod
    def from_metadata_dict(cls, metadata_data: dict[str, Any]) -> list[Self]:
        """Extract documentation URLs from parsed metadata.

        Args:
            metadata_data: The parsed metadata.yaml data as a dictionary

        Returns:
            List of ApiDocsUrl objects extracted from the metadata

        Raises:
            PyAirbyteInternalError: If a documentation entry is missing required 'title' or
                'url' field
        """
        data_section = metadata_data.get("data")
        if isinstance(data_section, dict):
            external_docs = data_section.get("externalDocumentationUrls")
            if isinstance(external_docs, list):
                return cls.from_metadata_docs_list(
                    external_docs, source="metadata_external_docs", context="Metadata"
                )
        return []


def _metadata_url_for(connector_name: str) -> str:
    """Get the expected URL of the metadata.yaml file for a connector.

    Args:
        connector_name: The canonical connector name (e.g., "source-facebook-marketing")

    Returns:
        The URL to the connector's metadata.yaml file
    """
    return _DEFAULT_METADATA_URL.format(
        source_name=connector_name,
        version="latest",
    )


def _fetch_metadata_dict(url: str) -> dict[str, Any]:
    """Fetch and parse a metadata.yaml file from a URL.

    Args:
        url: The URL to fetch the metadata from

    Returns:
        The parsed metadata as a dictionary, or empty dict if metadata not found (404)

    Raises:
        HTTPError: If the request fails with a non-404 status code
    """
    http_not_found = 404

    response = requests.get(url, timeout=10)
    if response.status_code == http_not_found:
        return {}

    response.raise_for_status()
    return yaml.safe_load(response.text) or {}


def _extract_docs_from_registry(connector_name: str) -> list[ApiDocsUrl]:
    """Extract documentation URLs from connector registry metadata.

    Args:
        connector_name: The canonical connector name (e.g., "source-facebook-marketing")

    Returns:
        List of ApiDocsUrl objects extracted from the registry

    Raises:
        PyAirbyteInternalError: If a documentation entry is missing required 'title' or 'url' field
    """
    registry_url = _get_registry_url()
    response = requests.get(registry_url, timeout=10)
    response.raise_for_status()
    registry_data = response.json()

    connector_list = registry_data.get("sources", []) + registry_data.get("destinations", [])
    connector_entry = None
    for entry in connector_list:
        if entry.get("dockerRepository", "").endswith(f"/{connector_name}"):
            connector_entry = entry
            break

    docs_urls = []

    if connector_entry and "documentationUrl" in connector_entry:
        docs_urls.append(
            ApiDocsUrl(
                title="Airbyte Documentation",
                url=connector_entry["documentationUrl"],
                source="registry",
                doc_type="internal",
            )
        )

    if connector_entry and "externalDocumentationUrls" in connector_entry:
        external_docs = connector_entry["externalDocumentationUrls"]
        if isinstance(external_docs, list):
            docs_urls.extend(
                ApiDocsUrl.from_metadata_docs_list(
                    external_docs, source="registry_external_docs", context="Registry"
                )
            )

    return docs_urls


def get_connector_docs_urls(connector_name: str) -> list[ApiDocsUrl]:
    """Get documentation URLs for a connector.

    This function retrieves documentation URLs for a connector from multiple sources:
    - Registry metadata (documentationUrl, externalDocumentationUrls)
    - Connector metadata.yaml file (data.externalDocumentationUrls)

    Args:
        connector_name: The canonical connector name (e.g., "source-facebook-marketing")

    Returns:
        List of ApiDocsUrl objects with documentation URLs.

    Raises:
        AirbyteConnectorNotRegisteredError: If the connector is not found in the registry.
    """
    if connector_name not in get_available_connectors(InstallType.ANY):
        raise exc.AirbyteConnectorNotRegisteredError(
            connector_name=connector_name,
            context={
                "registry_url": _get_registry_url(),
                "available_connectors": get_available_connectors(InstallType.ANY),
            },
        )

    docs_urls: list[ApiDocsUrl] = []

    registry_urls = _extract_docs_from_registry(connector_name)
    docs_urls.extend(registry_urls)

    metadata_url = _metadata_url_for(connector_name)
    metadata_data = _fetch_metadata_dict(metadata_url)
    metadata_urls = ApiDocsUrl.from_metadata_dict(metadata_data)
    docs_urls.extend(metadata_urls)

    return docs_urls


def get_connector_version_history(
    connector_name: str,
    *,
    num_versions_to_validate: int = 5,
    timeout: int = 30,
) -> list[ConnectorVersionInfo]:
    """Get version history for a connector.

    This function retrieves the version history for a connector by:
    1. Scraping the changelog HTML from docs.airbyte.com
    2. Parsing version information including PR URLs and titles
    3. Overriding release dates for the most recent N versions with accurate
       registry data

    Args:
        connector_name: Name of the connector (e.g., 'source-faker', 'destination-postgres')
        num_versions_to_validate: Number of most recent versions to override with
            registry release dates for accuracy. Defaults to 5.
        timeout: Timeout in seconds for the changelog fetch. Defaults to 30.

    Returns:
        List of ConnectorVersionInfo objects, sorted by most recent first.

    Raises:
        AirbyteConnectorNotRegisteredError: If the connector is not found in the registry.

    Example:
        >>> versions = get_connector_version_history("source-faker", num_versions_to_validate=3)
        >>> for v in versions[:5]:
        ...     print(f"{v.version}: {v.release_date}")
    """
    if connector_name not in get_available_connectors(InstallType.ANY):
        raise exc.AirbyteConnectorNotRegisteredError(
            connector_name=connector_name,
            context={
                "registry_url": _get_registry_url(),
                "available_connectors": get_available_connectors(InstallType.ANY),
            },
        )

    connector_type = "sources" if connector_name.startswith("source-") else "destinations"
    connector_short_name = connector_name.replace("source-", "").replace("destination-", "")

    changelog_url = f"https://docs.airbyte.com/integrations/{connector_type}/{connector_short_name}"

    try:
        response = requests.get(
            changelog_url,
            headers={"User-Agent": f"PyAirbyte/{get_version()}"},
            timeout=timeout,
        )
        response.raise_for_status()
        html_content = response.text
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch changelog for {connector_name}: {e}")
        return []

    version_dicts = parse_changelog_html(html_content, connector_name)

    if not version_dicts:
        logger.warning(f"No versions found in changelog for {connector_name}")
        return []

    versions = [ConnectorVersionInfo(**version_dict) for version_dict in version_dicts]

    for version_info in versions[:num_versions_to_validate]:
        registry_date = fetch_registry_version_date(connector_name, version_info.version)
        if registry_date:
            version_info.release_date = registry_date
            logger.debug(
                f"Updated release date for {connector_name} v{version_info.version} "
                f"from registry: {registry_date}"
            )

    return versions
