# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path
from typing import Optional
import pytest_mock
from airbyte.caches.snowflake import SnowflakeSqlProcessor, SnowflakeConfig
from airbyte_protocol.models import ConfiguredAirbyteCatalog
from airbyte.secrets.base import SecretString
from airbyte.shared.catalog_providers import CatalogProvider


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
