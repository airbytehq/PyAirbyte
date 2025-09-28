# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest_mock
from airbyte._processors.sql.postgres import PostgresConfig, PostgresSqlProcessor
from airbyte.caches.snowflake import SnowflakeConfig, SnowflakeSqlProcessor
from airbyte.secrets.base import SecretString
from airbyte.shared.catalog_providers import CatalogProvider
from airbyte.sources.util import get_source
from airbyte_protocol.models import ConfiguredAirbyteCatalog


def test_snowflake_cache_config_data_retention_time_in_days(
    mocker: pytest_mock.MockFixture,
):
    expected_cmd = """
        CREATE TABLE airbyte_raw."table_name" (
            col_name type
        )
        DATA_RETENTION_TIME_IN_DAYS = 1
        """

    def _execute_sql(cmd):
        global actual_cmd
        actual_cmd = cmd

    mocker.patch.object(SnowflakeSqlProcessor, "_execute_sql", side_effect=_execute_sql)
    config = _build_mocked_snowflake_processor(mocker, data_retention_time_in_days=1)
    config._create_table(table_name="table_name", column_definition_str="col_name type")

    assert actual_cmd == expected_cmd


def test_snowflake_cache_config_no_data_retention_time_in_days(
    mocker: pytest_mock.MockFixture,
):
    expected_cmd = """
        CREATE TABLE airbyte_raw."table_name" (
            col_name type
        )
        \n        """

    def _execute_sql(cmd):
        global actual_cmd
        actual_cmd = cmd

    mocker.patch.object(SnowflakeSqlProcessor, "_execute_sql", side_effect=_execute_sql)
    config = _build_mocked_snowflake_processor(mocker)
    config._create_table(table_name="table_name", column_definition_str="col_name type")

    assert actual_cmd == expected_cmd


def test_postgres_cache_config_generates_timestamps_with_timezone_settings():
    expected_sql = """
        CREATE TABLE airbyte_raw."products" (
            "id" BIGINT,
  "make" VARCHAR,
  "model" VARCHAR,
  "year" BIGINT,
  "price" DECIMAL(38, 9),
  "created_at" TIMESTAMP WITH TIME ZONE,
  "updated_at" TIMESTAMP WITH TIME ZONE,
  "_airbyte_raw_id" VARCHAR,
  "_airbyte_extracted_at" TIMESTAMP WITH TIME ZONE,
  "_airbyte_meta" JSON
        )
        \n        """
    source = get_source(
        name="source-faker",
        config={},
    )

    config = PostgresConfig(
        host="localhost",
        port=5432,
        username="postgres",
        password="postgres",
        database="postgres",
        schema_name="airbyte_raw",
    )

    processor = PostgresSqlProcessor(
        catalog_provider=CatalogProvider(source.configured_catalog),
        temp_dir=Path(),
        temp_file_cleanup=True,
        sql_config=config,
    )

    with (
        patch.object(processor, "_execute_sql") as _execute_sql_mock,
        patch.object(
            processor, "_table_exists", return_value=False
        ) as _table_exists_mock,
    ):
        processor._ensure_final_table_exists(
            stream_name="products",
        )
        _execute_sql_mock.assert_called_with(expected_sql)


def _build_mocked_snowflake_processor(
    mocker: pytest_mock.MockFixture, data_retention_time_in_days: Optional[int] = None
):
    sql_config = SnowflakeConfig(
        account="foo",
        username="foo",
        password=SecretString("foo"),
        warehouse="foo",
        database="foo",
        role="foo",
        data_retention_time_in_days=data_retention_time_in_days,
    )

    mocker.patch.object(
        SnowflakeSqlProcessor, "_ensure_schema_exists", return_value=None
    )
    return SnowflakeSqlProcessor(
        catalog_provider=CatalogProvider(ConfiguredAirbyteCatalog(streams=[])),
        temp_dir=Path(),
        temp_file_cleanup=True,
        sql_config=sql_config,
    )
