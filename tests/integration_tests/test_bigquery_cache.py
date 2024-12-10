# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests specific to BigQuery caches."""

from __future__ import annotations

import pytest

import airbyte as ab


@pytest.mark.requires_creds
def test_bigquery_props(
    new_bigquery_cache: ab.BigQueryCache,
) -> None:
    """Test that the BigQueryCache properties are set correctly."""
    # assert new_bigquery_cache.credentials_path.endswith(".json")
    assert new_bigquery_cache.dataset_name == new_bigquery_cache.schema_name, (
        "Dataset name should be the same as schema name."
    )
    assert new_bigquery_cache.schema_name != "airbyte_raw", (
        "Schema name should not be the default value."
    )

    assert new_bigquery_cache.get_database_name() == new_bigquery_cache.project_name, (
        "Database name should be the same as project name."
    )
