# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for the GSM secrets manager."""
from __future__ import annotations

from airbyte.secrets.google_gsm import GoogleGSMSecretManager


def test_get_gsm_secret(ci_secret_manager: GoogleGSMSecretManager) -> dict:
    assert ci_secret_manager.get_secret(
        "SECRET_DESTINATION_DUCKDB__MOTHERDUCK__CREDS",
    ).parse_json()


def test_get_gsm_secrets_with_filter(ci_secret_manager: GoogleGSMSecretManager) -> None:
    """Test fetching connector secrets."""
    secrets = ci_secret_manager.fetch_secrets(
        filter_string="labels.connector=source-bigquery",
    )
    assert secrets is not None
    secrets_list = list(secrets)
    assert len(secrets_list) > 0
    assert secrets_list[0].parse_json() is not None


def test_get_gsm_secrets_by_label(ci_secret_manager: GoogleGSMSecretManager) -> None:
    """Test fetching connector secrets."""
    secrets = ci_secret_manager.fetch_secrets_by_label(
        label_key="connector",
        label_value="source-salesforce",
    )
    assert secrets is not None
    secrets_list = list(secrets)
    assert len(secrets_list) > 0
    assert secrets_list[0].parse_json() is not None


def test_get_connector_secrets(ci_secret_manager: GoogleGSMSecretManager) -> None:
    """Test fetching connector secrets."""
    secrets = ci_secret_manager.fetch_connector_secrets(
        "source-salesforce"
    )
    assert secrets is not None
    secrets_list = list(secrets)
    assert len(secrets_list) > 0
    assert secrets_list[0].parse_json() is not None
