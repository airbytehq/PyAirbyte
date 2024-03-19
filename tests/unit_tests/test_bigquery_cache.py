# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which leverage the source-faker connector to test the framework end-to-end.

Since source-faker is included in dev dependencies, we can assume `source-faker` is installed
and available on PATH for the poetry-managed venv.
"""
from __future__ import annotations

import airbyte as ab


def test_bigquery_props(
    new_bigquery_cache: ab.BigQueryCache,
) -> None:
    """Test that the BigQueryCache properties are set correctly."""
    # assert new_bigquery_cache.credentials_path.endswith(".json")
    assert new_bigquery_cache.dataset_name == new_bigquery_cache.schema_name, \
        "Dataset name should be the same as schema name."
    assert new_bigquery_cache.schema_name != "airbyte_raw" \
        "Schema name should not be the default value."

    assert new_bigquery_cache.get_database_name() == new_bigquery_cache.project_name, \
        "Database name should be the same as project name."
