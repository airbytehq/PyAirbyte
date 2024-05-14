# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path

from airbyte.caches.base import CacheBase
from airbyte.caches.duckdb import DuckDBCache

UNIT_TEST_DB_PATH: Path = Path(".cache") / "unit_tests" / "test_db.duckdb"


def test_duck_db_cache_config_initialization():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.db_path == Path(UNIT_TEST_DB_PATH)
    assert config.schema_name == "test_schema"


def test_duck_db_cache_config_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.schema_name == "main"


def test_get_sql_alchemy_url():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_get_sql_alchemy_url_with_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_duck_db_cache_config_inheritance():
    assert issubclass(DuckDBCache, CacheBase)


def test_duck_db_cache_config_get_sql_alchemy_url():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_duck_db_cache_config_get_database_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.get_database_name() == "test_db"


def test_duck_db_cache_base_inheritance():
    assert issubclass(DuckDBCache, CacheBase)


def test_duck_db_cache_config_get_sql_alchemy_url_with_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_duck_db_cache_config_get_database_name_with_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.get_database_name() == "test_db"


def test_duck_db_cache_config_inheritance_from_sql_cache_config_base():
    assert issubclass(DuckDBCache, CacheBase)
