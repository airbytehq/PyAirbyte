# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import patch

import airbyte as ab
import pandas as pd
import pytest
import ulid
from airbyte import datasets
from airbyte import exceptions as exc
from airbyte._future_cdk.sql_processor import SqlProcessorBase
from airbyte.caches import PostgresCache, SnowflakeCache
from airbyte.constants import AB_INTERNAL_COLUMNS
from airbyte.datasets import CachedDataset, LazyDataset, SQLDataset
from airbyte.executors.base import _get_bin_dir
from airbyte.results import ReadResult
from airbyte.sources import registry
from airbyte.version import get_version
from sqlalchemy import column, text


@pytest.fixture(scope="function", autouse=True)
def autouse_source_test_registry(source_test_registry):
    return


def pop_internal_columns_from_dataset(
    dataset: datasets.DatasetBase | list[dict[str, Any]],
    /,
) -> list[dict]:
    result: list[dict] = []
    for record in list(dataset):
        for internal_column in AB_INTERNAL_COLUMNS:
            if not isinstance(record, dict):
                record = dict(record)

            assert (
                internal_column in record
            ), f"Column '{internal_column}' should exist in stream data."
            assert (
                record[internal_column] is not None
            ), f"Column '{internal_column}' should not contain null values."

            record.pop(internal_column, None)

        result.append(record)

    return result


def pop_internal_columns_from_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for internal_column in AB_INTERNAL_COLUMNS:
        assert (
            internal_column in df.columns
        ), f"Column '{internal_column}' should exist in stream data."

        assert (
            df[internal_column].notnull().all()
        ), f"Column '{internal_column}' should not contain null values "

    return df.drop(columns=AB_INTERNAL_COLUMNS)


def assert_data_matches_cache(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
    cache: SqlProcessorBase,
    streams: list[str] = None,
) -> None:
    for stream_name in streams or expected_test_stream_data.keys():
        if len(cache[stream_name]) > 0:
            cache_df = pop_internal_columns_from_dataframe(
                cache[stream_name].to_pandas(),
            )
            pd.testing.assert_frame_equal(
                cache_df,
                pd.DataFrame(expected_test_stream_data[stream_name]),
                check_dtype=False,
            )
        else:
            # stream is empty
            assert len(expected_test_stream_data[stream_name]) == 0

    # validate that the cache doesn't contain any other streams
    if streams:
        assert len(list(cache.__iter__())) == len(streams)


@pytest.fixture(scope="module", autouse=True)
def autouse_source_test_installation(source_test_installation):
    return


@pytest.fixture
def source_test(source_test_env) -> ab.Source:
    return ab.get_source("source-test", config={"apiKey": "test"})


@pytest.fixture
def expected_test_stream_data() -> dict[str, list[dict[str, str | int]]]:
    return {
        "stream1": [
            {
                "column1": "value1",
                "column2": 1,
                "sometimes_object": '{"nested_column": "nested_value"}',
            },
            {
                "column1": "value2",
                "column2": 2,
                "sometimes_object": "string_value",
            },
        ],
        "stream2": [
            {
                "column1": "value1",
                "column2": 1,
                "empty_column": None,
                "big_number": 1234567890123456,
            },
        ],
        "always-empty-stream": [],
    }


def test_registry_get():
    metadata = registry.get_connector_metadata("source-test")
    assert metadata.name == "source-test"
    assert metadata.latest_available_version == "0.0.1"


def test_registry_list() -> None:
    assert registry.get_available_connectors() == ["source-test"]


def test_list_streams(expected_test_stream_data: dict[str, list[dict[str, str | int]]]):
    source = ab.get_source(
        "source-test", config={"apiKey": "test"}, install_if_missing=False
    )
    assert source.get_available_streams() == list(expected_test_stream_data.keys())


def test_invalid_config():
    source = ab.get_source(
        "source-test", config={"apiKey": 1234}, install_if_missing=False
    )
    with pytest.raises(exc.AirbyteConnectorCheckFailedError):
        source.check()


def test_ensure_installation_detection():
    """Assert that install isn't called, since the connector is already installed by the fixture."""
    with patch("airbyte.executors.VenvExecutor.install") as mock_venv_install, patch(
        "airbyte.sources.base.Source.install"
    ) as mock_source_install, patch(
        "airbyte.executors.VenvExecutor.ensure_installation"
    ) as mock_ensure_installed:
        source = ab.get_source(
            "source-test",
            config={"apiKey": 1234},
            pip_url="https://pypi.org/project/airbyte-not-found",
            install_if_missing=True,
        )
        assert mock_ensure_installed.call_count == 1
        assert not mock_venv_install.called
        assert not mock_source_install.called


def test_source_yaml_spec():
    source = ab.get_source(
        "source-test", config={"apiKey": 1234}, install_if_missing=False
    )
    assert source._yaml_spec.startswith("connectionSpecification:\n  $schema:")


def test_non_existing_connector():
    with pytest.raises(Exception):
        ab.get_source("source-not-existing", config={"apiKey": "abc"})


def test_non_enabled_connector():
    with pytest.raises(exc.AirbyteConnectorNotPyPiPublishedError):
        ab.get_source("source-non-published", config={"apiKey": "abc"})


@pytest.mark.parametrize(
    "latest_available_version, requested_version, raises",
    [
        ("0.0.1", "latest", False),
        ("0.0.1", "0.0.1", False),
        ("0.0.1", None, False),
        ("1.2.3", None, False),  # Don't raise if a version is not requested
        ("1.2.3", "latest", True),
        ("1.2.3", "1.2.3", True),
    ],
)
def test_version_enforcement(
    raises: bool,
    latest_available_version,
    requested_version,
):
    """ "
    Ensures version enforcement works as expected:
    * If no version is specified, the current version is accepted
    * If the version is specified as "latest", only the latest available version is accepted
    * If the version is specified as a semantic version, only the exact version is accepted

    In this test, the actually installed version is 0.0.1
    """
    patched_entry = registry.ConnectorMetadata(
        name="source-test",
        latest_available_version=latest_available_version,
        pypi_package_name="airbyte-source-test",
        language=registry.Language.PYTHON,
        install_types={registry.InstallType.PYTHON, registry.InstallType.DOCKER},
    )

    # We need to initialize the cache before we can patch it.
    _ = registry._get_registry_cache()
    with patch.dict(
        "airbyte.registry.__cache", {"source-test": patched_entry}, clear=False
    ):
        if raises:
            with pytest.raises(Exception):
                source = ab.get_source(
                    "source-test",
                    version=requested_version,
                    config={"apiKey": "abc"},
                    install_if_missing=False,
                )
                source.executor.ensure_installation(auto_fix=False)
        else:
            source = ab.get_source(
                "source-test",
                version=requested_version,
                config={"apiKey": "abc"},
                install_if_missing=False,
            )
            if requested_version:  # Don't raise if a version is not requested
                assert source.executor.get_installed_version(raise_on_error=True) == (
                    requested_version or latest_available_version
                ).replace("latest", latest_available_version)
            source.executor.ensure_installation(auto_fix=False)


def test_check():
    source = ab.get_source(
        "source-test",
        config={"apiKey": "test"},
        install_if_missing=False,
    )
    source.check()


def test_check_fail():
    source = ab.get_source("source-test", config={"apiKey": "wrong"})

    with pytest.raises(Exception):
        source.check()


def test_file_write_and_cleanup() -> None:
    """Ensure files are written to the correct location and cleaned up afterwards."""
    temp_dir_root = Path(tempfile.mkdtemp())
    temp_dir_1 = temp_dir_root / "cache_1"
    temp_dir_2 = temp_dir_root / "cache_2"

    cache_w_cleanup = ab.new_local_cache(cache_dir=temp_dir_1, cleanup=True)
    cache_wo_cleanup = ab.new_local_cache(cache_dir=temp_dir_2, cleanup=False)

    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    _ = source.read(cache_w_cleanup)
    _ = source.read(cache_wo_cleanup)

    # We expect all files to be cleaned up:
    assert (
        len(list(Path(temp_dir_1).glob("*.jsonl.gz"))) == 0
    ), "Expected files to be cleaned up"

    # There are three streams, but only two of them have data:
    assert (
        len(list(Path(temp_dir_2).glob("*.jsonl.gz"))) == 2
    ), "Expected files to exist"

    with suppress(Exception):
        shutil.rmtree(str(temp_dir_root))


def test_sync_to_duckdb(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    cache = ab.new_local_cache()

    result: ReadResult = source.read(cache)

    assert result.processed_records == 3
    assert_data_matches_cache(expected_test_stream_data, cache)


def test_read_result_mapping():
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()
    result: ReadResult = source.read(ab.new_local_cache())
    assert len(result) == 3
    assert isinstance(result, Mapping)
    assert "stream1" in result
    assert "stream2" in result
    assert "always-empty-stream" in result
    assert "stream3" not in result
    assert result.keys() == {"stream1", "stream2", "always-empty-stream"}


def test_dataset_list_and_len(expected_test_stream_data):
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    # Test the lazy dataset implementation
    lazy_dataset = source.get_records("stream1")
    # assert len(stream_1) == 2  # This is not supported by the lazy dataset
    lazy_dataset_list = list(lazy_dataset)
    # Make sure counts are correct
    assert len(list(lazy_dataset_list)) == 2

    # Test the cached dataset implementation
    result: ReadResult = source.read(ab.new_local_cache())
    stream_1 = result["stream1"]
    assert len(stream_1) == 2
    assert len(list(stream_1)) == 2

    assert isinstance(result, Mapping)
    assert "stream1" in result
    assert "stream2" in result
    assert "always-empty-stream" in result
    assert "stream3" not in result
    assert result.keys() == {"stream1", "stream2", "always-empty-stream"}


def test_read_from_cache(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    """
    Test that we can read from a cache that already has data (identifier by name)
    """
    cache_name = str(ulid.ULID())
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    cache = ab.new_local_cache(cache_name)

    source.read(cache)

    # Create a new cache pointing to the same duckdb file
    second_cache = ab.new_local_cache(cache_name)

    assert_data_matches_cache(expected_test_stream_data, second_cache)


def test_read_isolated_by_prefix(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    """
    Test that cache correctly isolates streams when different table prefixes are used
    """
    cache_name = str(ulid.ULID())
    db_path = Path(f"./.cache/{cache_name}.duckdb")
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()
    cache = ab.DuckDBCache(db_path=db_path, table_prefix="prefix_")

    source.read(cache)

    same_prefix_cache = ab.DuckDBCache(db_path=db_path, table_prefix="prefix_")
    different_prefix_cache = ab.DuckDBCache(
        db_path=db_path, table_prefix="different_prefix_"
    )
    no_prefix_cache = ab.DuckDBCache(db_path=db_path, table_prefix=None)

    # validate that the cache with the same prefix has the data as expected, while the other two are empty
    assert_data_matches_cache(expected_test_stream_data, same_prefix_cache)
    assert len(list(different_prefix_cache.__iter__())) == 0
    assert len(list(no_prefix_cache.__iter__())) == 0

    # read partial data into the other two caches
    source.select_streams(["stream1"])
    source.read(different_prefix_cache)
    source.read(no_prefix_cache)

    second_same_prefix_cache = ab.DuckDBCache(db_path=db_path, table_prefix="prefix_")
    second_different_prefix_cache = ab.DuckDBCache(
        db_path=db_path, table_prefix="different_prefix_"
    )
    second_no_prefix_cache = ab.DuckDBCache(db_path=db_path, table_prefix=None)

    # validate that the first cache still has full data, while the other two have partial data
    assert_data_matches_cache(expected_test_stream_data, second_same_prefix_cache)
    assert_data_matches_cache(
        expected_test_stream_data, second_different_prefix_cache, streams=["stream1"]
    )
    assert_data_matches_cache(
        expected_test_stream_data, second_no_prefix_cache, streams=["stream1"]
    )


def test_merge_streams_in_cache(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    """
    Test that we can extend a cache with new streams
    """
    cache_name = str(ulid.ULID())
    source = ab.get_source("source-test", config={"apiKey": "test"})
    cache = ab.new_local_cache(cache_name)

    source.select_streams(["stream1"])
    source.read(cache)

    # Assert that the cache only contains stream1
    with pytest.raises(KeyError):
        cache["stream2"]

    # Create a new cache with the same name
    second_cache = ab.new_local_cache(cache_name)
    source.select_streams(["stream2"])
    result = source.read(second_cache)

    third_cache = ab.new_local_cache(cache_name)
    source.select_streams(["always-empty-stream"])
    result = source.read(third_cache)

    # Assert that the read result only contains stream2
    with pytest.raises(KeyError):
        result["stream1"]
    with pytest.raises(KeyError):
        result["stream2"]

    assert_data_matches_cache(expected_test_stream_data, third_cache)


def test_read_result_as_list(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    cache = ab.new_local_cache()

    result: ReadResult = source.read(cache)
    stream_1_list = list(result["stream1"])
    stream_2_list = list(result["stream2"])
    always_empty_stream_list = list(result["always-empty-stream"])
    assert (
        pop_internal_columns_from_dataset(stream_1_list)
        == expected_test_stream_data["stream1"]
    )
    assert (
        pop_internal_columns_from_dataset(stream_2_list)
        == expected_test_stream_data["stream2"]
    )
    assert (
        pop_internal_columns_from_dataset(always_empty_stream_list)
        == expected_test_stream_data["always-empty-stream"]
    )


def test_get_records_result_as_list(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    source = ab.get_source("source-test", config={"apiKey": "test"})

    stream_1_list = list(source.get_records("stream1"))
    stream_2_list = list(source.get_records("stream2"))
    always_empty_stream_list = list(source.get_records("always-empty-stream"))
    assert (
        pop_internal_columns_from_dataset(stream_1_list)
        == expected_test_stream_data["stream1"]
    )
    assert (
        pop_internal_columns_from_dataset(stream_2_list)
        == expected_test_stream_data["stream2"]
    )
    assert (
        pop_internal_columns_from_dataset(always_empty_stream_list)
        == expected_test_stream_data["always-empty-stream"]
    )


def test_sync_with_merge_to_duckdb(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    """Test that the merge strategy works as expected.

    In this test, we sync the same data twice. If the data is not duplicated, we assume
    the merge was successful.

    # TODO: Add a check with a primary key to ensure that the merge strategy works as expected.
    """
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    cache = ab.new_local_cache()

    # Read twice to test merge strategy
    result: ReadResult = source.read(cache)
    result: ReadResult = source.read(cache)

    assert result.processed_records == 3
    for stream_name, expected_data in expected_test_stream_data.items():
        if len(cache[stream_name]) > 0:
            pd.testing.assert_frame_equal(
                pop_internal_columns_from_dataframe(result[stream_name].to_pandas()),
                pd.DataFrame(expected_data),
                check_dtype=False,
            )
        else:
            # stream is empty
            assert len(expected_test_stream_data[stream_name]) == 0


def test_cached_dataset(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
) -> None:
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    result: ReadResult = source.read(ab.new_local_cache())

    stream_name = "stream1"
    not_a_stream_name = "not_a_stream"

    # Check that the stream appears in mapping-like attributes
    assert stream_name in result
    assert stream_name in result.cache
    assert stream_name in result.cache.streams
    assert stream_name in result.streams

    stream_get_a: CachedDataset = result[stream_name]
    stream_get_b: CachedDataset = result.streams[stream_name]
    stream_get_c: CachedDataset = result.cache[stream_name]
    stream_get_d: CachedDataset = result.cache.streams[stream_name]

    # Check that each get method is syntactically equivalent

    assert isinstance(stream_get_a, CachedDataset)
    assert isinstance(stream_get_b, CachedDataset)
    assert isinstance(stream_get_c, CachedDataset)
    assert isinstance(stream_get_d, CachedDataset)

    assert stream_get_a == stream_get_b
    assert stream_get_b == stream_get_c
    assert stream_get_c == stream_get_d

    # Check that we can iterate over the stream

    list_from_iter_a = list(stream_get_a)
    list_from_iter_b = [row for row in stream_get_a]

    # Make sure that we get a key error if we try to access a stream that doesn't exist
    with pytest.raises(KeyError):
        result[not_a_stream_name]
    with pytest.raises(KeyError):
        result.streams[not_a_stream_name]
    with pytest.raises(KeyError):
        result.cache[not_a_stream_name]
    with pytest.raises(KeyError):
        result.cache.streams[not_a_stream_name]

    # Make sure we can use "result.streams.items()"
    for stream_name, cached_dataset in result.streams.items():
        assert isinstance(cached_dataset, CachedDataset)
        assert isinstance(stream_name, str)

        list_data = list(cached_dataset)
        assert (
            pop_internal_columns_from_dataset(list_data)
            == expected_test_stream_data[stream_name]
        )

    # Make sure we can use "result.cache.streams.items()"
    for stream_name, cached_dataset in result.cache.streams.items():
        assert isinstance(cached_dataset, CachedDataset)
        assert isinstance(stream_name, str)

        list_data = list(cached_dataset)
        assert (
            pop_internal_columns_from_dataset(list_data)
            == expected_test_stream_data[stream_name]
        )


def test_cached_dataset_filter():
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    result: ReadResult = source.read(ab.new_local_cache())

    stream_name = "stream1"

    # Check the many ways to add a filter:
    cached_dataset: CachedDataset = result[stream_name]
    filtered_dataset_a: SQLDataset = cached_dataset.with_filter("column2 == 1")
    filtered_dataset_b: SQLDataset = cached_dataset.with_filter(text("column2 == 1"))
    filtered_dataset_c: SQLDataset = cached_dataset.with_filter(column("column2") == 1)

    assert isinstance(cached_dataset, CachedDataset)
    all_records = list(cached_dataset)
    assert len(all_records) == 2

    for filtered_dataset, case in [
        (filtered_dataset_a, "a"),
        (filtered_dataset_b, "b"),
        (filtered_dataset_c, "c"),
    ]:
        assert isinstance(filtered_dataset, SQLDataset)

        # Check that we can iterate over each stream

        filtered_records: list[Mapping[str, Any]] = [row for row in filtered_dataset]

        # Check that the filter worked
        assert (
            len(filtered_records) == 1
        ), f"Case '{case}' had incorrect number of records."

        # Assert the stream name still matches
        assert (
            filtered_dataset.stream_name == stream_name
        ), f"Case '{case}' had incorrect stream name."

        # Check that chaining filters works
        chained_dataset = filtered_dataset.with_filter("column1 == 'value1'")
        chained_records = [row for row in chained_dataset]
        assert (
            len(chained_records) == 1
        ), f"Case '{case}' had incorrect number of records after chaining filters."


def test_lazy_dataset_from_source(
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
) -> None:
    source = ab.get_source("source-test", config={"apiKey": "test"})

    stream_name = "stream1"
    not_a_stream_name = "not_a_stream"

    lazy_dataset_a = source.get_records(stream_name)
    lazy_dataset_b = source.get_records(stream_name)

    assert isinstance(lazy_dataset_a, LazyDataset)

    # Check that we can iterate over the stream

    list_from_iter_a = list(lazy_dataset_a)
    list_from_iter_b = [row for row in lazy_dataset_b]

    assert pop_internal_columns_from_dataset(
        list_from_iter_a
    ) == pop_internal_columns_from_dataset(list_from_iter_b)

    # Make sure that we get a key error if we try to access a stream that doesn't exist
    with pytest.raises(exc.PyAirbyteInputError):
        source.get_records(not_a_stream_name)

    # Make sure we can iterate on all available streams
    for stream_name in source.get_available_streams():
        assert isinstance(stream_name, str)

        lazy_dataset: LazyDataset = source.get_records(stream_name)
        assert isinstance(lazy_dataset, LazyDataset)

        list_data = list(lazy_dataset)
        assert (
            pop_internal_columns_from_dataset(list_data)
            == expected_test_stream_data[stream_name]
        )


@pytest.mark.parametrize(
    "method_call",
    [
        pytest.param(lambda source: source.check(), id="check"),
        pytest.param(
            lambda source: list(source.get_records("stream1")), id="read_stream"
        ),
        pytest.param(lambda source: source.read(), id="read"),
    ],
)
def test_check_fail_on_missing_config(method_call):
    source = ab.get_source("source-test")

    with pytest.raises(exc.AirbyteConnectorConfigurationMissingError):
        method_call(source)


def test_sync_with_merge_to_postgres(
    new_postgres_cache: PostgresCache,
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    """Test that the merge strategy works as expected.

    In this test, we sync the same data twice. If the data is not duplicated, we assume
    the merge was successful.

    # TODO: Add a check with a primary key to ensure that the merge strategy works as expected.
    """
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    # Read twice to test merge strategy
    result: ReadResult = source.read(new_postgres_cache)
    result: ReadResult = source.read(new_postgres_cache)

    assert result.processed_records == 3
    for stream_name, expected_data in expected_test_stream_data.items():
        if len(new_postgres_cache[stream_name]) > 0:
            pd.testing.assert_frame_equal(
                result[stream_name].to_pandas(),
                pd.DataFrame(expected_data),
                check_dtype=False,
            )
        else:
            # stream is empty
            assert len(expected_test_stream_data[stream_name]) == 0


def test_airbyte_version() -> None:
    assert get_version()
    assert isinstance(get_version(), str)

    # Ensure the version is a valid semantic version (x.y.z or x.y.z.alpha0)
    assert 3 <= len(get_version().split(".")) <= 4


def test_sync_to_postgres(
    new_postgres_cache: PostgresCache,
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
) -> None:
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    result: ReadResult = source.read(new_postgres_cache)

    assert result.processed_records == 3
    for stream_name, expected_data in expected_test_stream_data.items():
        if len(new_postgres_cache[stream_name]) > 0:
            pd.testing.assert_frame_equal(
                pop_internal_columns_from_dataframe(result[stream_name].to_pandas()),
                pd.DataFrame(expected_data),
                check_dtype=False,
            )
        else:
            # stream is empty
            assert len(expected_test_stream_data[stream_name]) == 0


@pytest.mark.slow
@pytest.mark.requires_creds
def test_sync_to_snowflake(
    new_snowflake_cache: SnowflakeCache,
    expected_test_stream_data: dict[str, list[dict[str, str | int]]],
):
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    result: ReadResult = source.read(new_snowflake_cache)

    assert result.processed_records == 3
    for stream_name, expected_data in expected_test_stream_data.items():
        if len(new_snowflake_cache[stream_name]) > 0:
            pd.testing.assert_frame_equal(
                pop_internal_columns_from_dataframe(result[stream_name].to_pandas()),
                pd.DataFrame(expected_data),
                check_dtype=False,
            )
        else:
            # stream is empty
            assert len(expected_test_stream_data[stream_name]) == 0


def test_sync_limited_streams(expected_test_stream_data):
    source = ab.get_source("source-test", config={"apiKey": "test"})
    cache = ab.new_local_cache()

    source.select_streams(["stream2"])

    result = source.read(cache)

    assert result.processed_records == 1
    pd.testing.assert_frame_equal(
        pop_internal_columns_from_dataframe(result["stream2"].to_pandas()),
        pd.DataFrame(expected_test_stream_data["stream2"]),
        check_dtype=False,
    )


def test_read_stream_nonexisting():
    source = ab.get_source("source-test", config={"apiKey": "test"})

    with pytest.raises(Exception):
        list(source.get_records("non-existing"))


def test_failing_path_connector():
    with pytest.raises(Exception):
        ab.get_source("source-test", config={"apiKey": "test"}, use_local_install=True)


def test_succeeding_path_connector(monkeypatch):
    venv_bin_path = str(_get_bin_dir(Path(".venv-source-test")))

    # Add the bin directory to the PATH
    new_path = f"{venv_bin_path}{os.pathsep}{os.environ['PATH']}"

    # Patch the PATH env var to include the test venv bin folder
    monkeypatch.setenv("PATH", new_path)

    source = ab.get_source(
        "source-test",
        config={"apiKey": "test"},
        local_executable="source-test",
    )
    source.check()


def test_install_uninstall():
    with tempfile.TemporaryDirectory() as temp_dir:
        source = ab.get_source(
            "source-test",
            pip_url="./tests/integration_tests/fixtures/source-test",
            config={"apiKey": "test"},
            install_if_missing=False,
        )

        # Override the install root to avoid conflicts with the test fixture
        install_root = Path(temp_dir)
        source.executor.install_root = install_root

        # assert that the venv is gone
        assert not os.path.exists(install_root / ".venv-source-test")

        # use which to check if the executable is available
        assert shutil.which("source-test") is None

        # assert that the connector is not available
        with pytest.raises(Exception):
            source.check()

        source.install()

        assert os.path.exists(install_root / ".venv-source-test")
        assert os.path.exists(_get_bin_dir(install_root / ".venv-source-test"))

        source.check()

        source.uninstall()

        assert not os.path.exists(install_root / ".venv-source-test")
        assert not os.path.exists(_get_bin_dir(install_root / ".venv-source-test"))
