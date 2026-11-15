# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from sqlalchemy import create_engine, text


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


@pytest.mark.parametrize("max_chunk_size", [1, 2, 10])
@pytest.mark.parametrize(
    ("sql_type", "names"),
    [
        ("TEXT", [None, "Alice"]),
        ("TEXT", ["Alice", None]),
        ("TEXT", [None, None]),
        ("TEXT", []),
        ("INTEGER", [None, 1, 2]),
    ],
)
def test_arrow_dataset_nullable_columns(max_chunk_size, sql_type, names):
    engine = create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.execute(text(f"CREATE TABLE events (name {sql_type})"))
            for name in names:
                connection.execute(
                    text("INSERT INTO events VALUES (:name)"), {"name": name}
                )

        cache = SimpleNamespace(
            _read_processor=SimpleNamespace(get_sql_table_name=lambda _: "events"),
            get_sql_engine=lambda: engine,
            schema_name=None,
        )
        dataset = CacheBase.get_arrow_dataset(
            cache, "events", max_chunk_size=max_chunk_size
        )
        assert dataset.to_table().to_pylist() == [{"name": name} for name in names]
        if "Alice" in names:
            assert dataset.schema.field("name").type == pa.string()
        elif all(name is None for name in names):
            assert dataset.schema.field("name").type == pa.null()
    finally:
        engine.dispose()
