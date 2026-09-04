# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Validate deferred-auth placeholder configurations against registry schemas."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from airbyte._util.registry_spec import get_connector_spec_from_registry
from airbyte.cloud._deferred_auth import _stub_missing_secrets
from airbyte.registry import (
    InstallType,
    get_available_connectors,
    get_connector_metadata,
)


def _certified_source_names() -> list[str]:
    try:
        connector_names = get_available_connectors(InstallType.ANY)
        return [
            name
            for name in connector_names
            if name.startswith("source-")
            and (
                (metadata := get_connector_metadata(name)) is not None
                and metadata.support_level == "certified"
            )
        ]
    except Exception:
        return []


def _cached_spec(connector_name: str) -> dict[str, Any] | None:
    metadata = get_connector_metadata(connector_name)
    if metadata is None or metadata.latest_available_version is None:
        return None

    version = metadata.latest_available_version
    cache_path = (
        Path(tempfile.gettempdir())
        / "pyairbyte-registry-spec-cache"
        / f"{connector_name}-{version}.json"
    )
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass

    spec = get_connector_spec_from_registry(
        connector_name,
        version=version,
        platform="oss",
    )
    if spec is None:
        return None

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(spec), encoding="utf-8")
    return spec


EXPECTED_FAILURES: dict[str, str] = {
    "source-amazon-seller-partner": (
        "requires non-secret configuration not inferable from an empty config: "
        "aws_environment"
    ),
    "source-amplitude": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-azure-blob-storage": (
        "requires non-secret configuration not inferable from an empty config: streams"
    ),
    "source-chargebee": (
        "requires non-secret configuration not inferable from an empty config: site"
    ),
    "source-db2-enterprise": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-facebook-marketing": (
        "requires non-secret configuration not inferable from an empty config: "
        "account_ids"
    ),
    "source-file": (
        "requires non-secret configuration not inferable from an empty config: "
        "dataset_name"
    ),
    "source-freshdesk": (
        "requires non-secret configuration not inferable from an empty config: domain"
    ),
    "source-gcs": (
        "requires non-secret configuration not inferable from an empty config: streams"
    ),
    "source-github": "requires choosing a credentials oneOf branch",
    "source-gitlab": "requires choosing a credentials oneOf branch",
    "source-google-ads": (
        "requires non-secret configuration not inferable from an empty config: "
        "credentials.client_id"
    ),
    "source-google-analytics-data-api": (
        "requires non-secret configuration not inferable from an empty config: "
        "property_ids"
    ),
    "source-google-drive": (
        "requires non-secret configuration not inferable from an empty config: streams"
    ),
    "source-google-search-console": (
        "requires non-secret configuration not inferable from an empty config: "
        "site_urls"
    ),
    "source-google-sheets": (
        "requires non-secret configuration not inferable from an empty config: "
        "spreadsheet_id"
    ),
    "source-harvest": (
        "requires non-secret configuration not inferable from an empty config: "
        "replication_start_date"
    ),
    "source-hubspot": "requires choosing a credentials oneOf branch",
    "source-intercom": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-jira": "requires choosing a credentials oneOf branch",
    "source-linkedin-ads": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-mongodb-v2": "requires choosing a database_config oneOf branch",
    "source-mssql": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-mysql": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-netsuite-enterprise": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-oracle-enterprise": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-paypal-transaction": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-postgres": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-s3": (
        "requires non-secret configuration not inferable from an empty config: streams"
    ),
    "source-salesforce": (
        "requires non-secret configuration not inferable from an empty config: "
        "client_id"
    ),
    "source-sap-hana-enterprise": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-sendgrid": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-sentry": (
        "requires non-secret configuration not inferable from an empty config: "
        "organization"
    ),
    "source-service-now": (
        "requires non-secret configuration not inferable from an empty config: base_url"
    ),
    "source-sharepoint-enterprise": (
        "requires non-secret configuration not inferable from an empty config: streams"
    ),
    "source-sharepoint-lists-enterprise": (
        "requires non-secret configuration not inferable from an empty config: "
        "tenant_id"
    ),
    "source-shopify": (
        "requires non-secret configuration not inferable from an empty config: shop"
    ),
    "source-slack": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-snowflake": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-stripe": (
        "requires non-secret configuration not inferable from an empty config: "
        "account_id"
    ),
    "source-twilio": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-typeform": "requires choosing a credentials oneOf branch",
    "source-woocommerce": (
        "requires non-secret configuration not inferable from an empty config: shop"
    ),
    "source-workday": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-workday-rest": (
        "requires non-secret configuration not inferable from an empty config: host"
    ),
    "source-zendesk-chat": (
        "requires non-secret configuration not inferable from an empty config: "
        "start_date"
    ),
    "source-zendesk-support": (
        "requires non-secret configuration not inferable from an empty config: "
        "subdomain"
    ),
    "source-zendesk-talk": (
        "requires non-secret configuration not inferable from an empty config: "
        "subdomain"
    ),
}

_CERTIFIED_SOURCE_NAMES = _certified_source_names()
if not _CERTIFIED_SOURCE_NAMES:
    pytest.skip("registry unreachable", allow_module_level=True)


@pytest.mark.parametrize("connector_name", _CERTIFIED_SOURCE_NAMES)
def test_stubbed_config_passes_registry_schema(connector_name: str) -> None:
    if connector_name in EXPECTED_FAILURES:
        pytest.xfail(EXPECTED_FAILURES[connector_name])

    spec = _cached_spec(connector_name)
    if spec is None:
        pytest.skip(f"no spec available for {connector_name}")

    config = _stub_missing_secrets({}, spec)
    jsonschema.validate(instance=config, schema=spec)
