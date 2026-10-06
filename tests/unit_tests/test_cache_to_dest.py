# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Golden tests for cache-to-cloud-destination configuration dicts.

These freeze the exact wire JSON the removed Speakeasy `airbyte_api` destination
config models serialized to, so the dict-based replacements stay byte-identical.
"""

from __future__ import annotations

import pytest
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.snowflake import SnowflakeCache

from airbyte.caches._utils import _cache_to_dest


@pytest.mark.parametrize(
    ("cache", "expected"),
    [
        pytest.param(
            DuckDBCache.model_construct(db_path="/tmp/db.duckdb", schema_name="main"),
            {
                "destination_path": "/tmp/db.duckdb",
                "destinationType": "duckdb",
                "schema": "main",
            },
            id="duckdb",
        ),
        pytest.param(
            MotherDuckCache.model_construct(
                db_path="md:db", schema_name="main", api_key="token"
            ),
            {
                "destination_path": "md:db",
                "destinationType": "duckdb",
                "motherduck_api_key": "token",
                "schema": "main",
            },
            id="motherduck",
        ),
        pytest.param(
            PostgresCache.model_construct(
                database="db",
                host="host",
                username="user",
                password="pass",
                port=5432,
                schema_name="public",
            ),
            {
                "database": "db",
                "host": "host",
                "username": "user",
                "destinationType": "postgres",
                "disable_type_dedupe": False,
                "drop_cascade": False,
                "password": "pass",
                "port": 5432,
                "schema": "public",
                "ssl": False,
                "unconstrained_number": False,
            },
            id="postgres",
        ),
        pytest.param(
            SnowflakeCache.model_construct(
                account="acct",
                database="db",
                warehouse="wh",
                role="role",
                username="user",
                password="pass",
                schema_name="public",
            ),
            {
                "host": "acct.snowflakecomputing.com",
                "database": "DB",
                "schema": "PUBLIC",
                "warehouse": "wh",
                "role": "role",
                "username": "user",
                "credentials": {
                    "password": "pass",
                    "auth_type": "Username and Password",
                },
                "destinationType": "snowflake",
                "disable_type_dedupe": False,
                "retention_period_days": 1,
                "use_merge_for_upsert": False,
            },
            id="snowflake",
        ),
        pytest.param(
            BigQueryCache.model_construct(
                project_name="proj",
                dataset_name="ds",
                dataset_location="US",
                credentials_path=None,
            ),
            {
                "project_id": "proj",
                "dataset_id": "ds",
                "dataset_location": "US",
                "cdc_deletion_mode": "Hard delete",
                "credentials_json": None,
                "destinationType": "bigquery",
                "disable_type_dedupe": False,
                "loading_method": {"method": "Standard"},
            },
            id="bigquery",
        ),
    ],
)
def test_cache_to_destination_configuration_matches_speakeasy_wire_json(
    cache: object,
    expected: dict,
) -> None:
    assert _cache_to_dest.cache_to_destination_configuration(cache) == expected
