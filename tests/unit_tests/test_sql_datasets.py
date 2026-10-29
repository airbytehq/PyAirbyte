# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""SQL dataset conversion regressions using a real local cache."""

from pathlib import Path

from sqlalchemy import column, select, text

from airbyte.caches import DuckDBCache
from airbyte.datasets import CachedDataset, SQLDataset


def test_sql_dataset_arrow_promotes_nullable_chunks(tmp_path: Path) -> None:
    """A filtered first batch of nulls must not fix the schema to null types."""
    with DuckDBCache(db_path=tmp_path / "cache.duckdb", cache_dir=tmp_path) as cache:
        cache.execute_sql([
            "CREATE TABLE main.sample (id INTEGER, s VARCHAR, n INTEGER)",
            "INSERT INTO main.sample VALUES (1, 'first', 10), (2, NULL, NULL), (3, 'third', 30)",
        ])
        original = CachedDataset(cache, "sample", stream_configuration=False)
        assert original.to_arrow(max_chunk_size=1).count_rows() == 3

        query = select("*").select_from(text("main.sample")).where(column("id") >= 2)
        filtered = SQLDataset(cache, "sample", query, stream_configuration=False)
        expected = [{"id": 2, "s": None, "n": None}, {"id": 3, "s": "third", "n": 30}]
        assert list(filtered) == expected
        converted = filtered.to_arrow(max_chunk_size=1)
        assert converted.to_table().to_pylist() == expected
        assert [batch.num_rows for batch in converted.to_batches()] == [1, 1]
