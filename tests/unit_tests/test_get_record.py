# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests for Source.get_record() and DeclarativeExecutor.fetch_record()."""

from __future__ import annotations

from unittest.mock import Mock, PropertyMock, patch

import pytest

from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream

from airbyte import exceptions as exc
from airbyte._executors.declarative import DeclarativeExecutor
from airbyte.sources.base import Source


@pytest.mark.parametrize(
    "stream_name,pk_value,expected_error",
    [
        pytest.param("users", "123", None, id="valid_stream_and_pk"),
        pytest.param(
            "nonexistent", "123", exc.AirbyteStreamNotFoundError, id="stream_not_found"
        ),
    ],
)
def test_declarative_executor_fetch_record_stream_validation(
    stream_name: str,
    pk_value: str,
    expected_error: type[Exception] | None,
) -> None:
    """Test stream validation in DeclarativeExecutor.fetch_record()."""
    manifest = {
        "streams": [
            {
                "name": "users",
                "retriever": {
                    "type": "SimpleRetriever",
                    "requester": {
                        "url_base": "https://api.example.com",
                        "path": "/users",
                    },
                    "record_selector": {"extractor": {"field_path": []}},
                },
            }
        ]
    }

    executor = DeclarativeExecutor(
        name="test-source",
        manifest=manifest,
    )

    mock_stream = Mock(spec=AbstractStream)
    mock_stream.name = "users"

    mock_retriever = Mock(spec=SimpleRetriever)
    mock_retriever.requester = Mock()
    mock_retriever.requester.get_path = Mock(return_value="/users")
    mock_retriever.requester.send_request = Mock(
        return_value=Mock(json=lambda: {"id": "123"})
    )
    mock_retriever._request_headers = Mock(return_value={})
    mock_retriever._request_params = Mock(return_value={})
    mock_retriever._request_body_data = Mock(return_value=None)
    mock_retriever._request_body_json = Mock(return_value=None)
    mock_retriever.record_selector = Mock()
    mock_retriever.record_selector.select_records = Mock(return_value=[{"id": "123"}])

    mock_stream.retriever = mock_retriever

    mock_streams = [mock_stream] if stream_name == "users" else []

    mock_declarative_source = Mock()
    mock_declarative_source.streams = Mock(return_value=mock_streams)

    if expected_error:
        with patch.object(
            type(executor), "declarative_source", new_callable=PropertyMock
        ) as mock_prop:
            mock_prop.return_value = mock_declarative_source
            with pytest.raises(expected_error):
                executor.fetch_record(stream_name, pk_value)
    else:
        with patch.object(
            type(executor), "declarative_source", new_callable=PropertyMock
        ) as mock_prop:
            mock_prop.return_value = mock_declarative_source
            result = executor.fetch_record(stream_name, pk_value)
            assert result == {"id": "123"}


@pytest.mark.parametrize(
    "primary_key,expected_result",
    [
        pytest.param([["id"]], ["id"], id="nested_single_field"),
        pytest.param(["id"], ["id"], id="flat_single_field"),
        pytest.param([["id"], ["org_id"]], ["id", "org_id"], id="nested_composite"),
        pytest.param([], [], id="no_primary_key"),
        pytest.param(None, [], id="none_primary_key"),
    ],
)
def test_source_get_stream_primary_key(
    primary_key: list | None,
    expected_result: list[str],
) -> None:
    """Test _get_stream_primary_key() handles various PK formats."""
    mock_executor = Mock()
    source = Source(
        executor=mock_executor,
        name="test-source",
        config={"api_key": "test"},
    )

    mock_stream = Mock()
    mock_stream.stream.name = "test_stream"
    mock_stream.primary_key = primary_key

    mock_catalog = Mock()
    mock_catalog.streams = [mock_stream]

    with patch.object(
        type(source), "configured_catalog", new_callable=PropertyMock
    ) as mock_prop:
        mock_prop.return_value = mock_catalog
        result = source._get_stream_primary_key("test_stream")
        assert result == expected_result


def test_source_get_stream_primary_key_stream_not_found() -> None:
    """Test _get_stream_primary_key() raises error for nonexistent stream."""
    mock_executor = Mock()
    source = Source(
        executor=mock_executor,
        name="test-source",
        config={"api_key": "test"},
    )

    mock_catalog = Mock()
    mock_catalog.streams = []

    with patch.object(
        type(source), "configured_catalog", new_callable=PropertyMock
    ) as mock_prop:
        mock_prop.return_value = mock_catalog
        with patch.object(source, "get_available_streams", return_value=[]):
            with pytest.raises(exc.AirbyteStreamNotFoundError):
                source._get_stream_primary_key("nonexistent_stream")


@pytest.mark.parametrize(
    "pk_value,primary_key_fields,expected_result,expected_error",
    [
        pytest.param("123", ["id"], "123", None, id="string_value"),
        pytest.param(123, ["id"], "123", None, id="int_value"),
        pytest.param({"id": "123"}, ["id"], "123", None, id="dict_with_correct_key"),
        pytest.param(
            {"wrong_key": "123"},
            ["id"],
            None,
            exc.PyAirbyteInputError,
            id="dict_with_wrong_key",
        ),
        pytest.param(
            {"id": "123", "extra": "456"},
            ["id"],
            None,
            exc.PyAirbyteInputError,
            id="dict_with_multiple_entries",
        ),
        pytest.param(
            "123",
            ["id", "org_id"],
            None,
            NotImplementedError,
            id="composite_primary_key",
        ),
        pytest.param("123", [], None, exc.PyAirbyteInputError, id="no_primary_key"),
    ],
)
def test_source_normalize_and_validate_pk_value(
    pk_value: any,
    primary_key_fields: list[str],
    expected_result: str | None,
    expected_error: type[Exception] | None,
) -> None:
    """Test _normalize_and_validate_pk_value() handles various input formats."""
    mock_executor = Mock()
    source = Source(
        executor=mock_executor,
        name="test-source",
        config={"api_key": "test"},
    )

    with patch.object(
        source, "_get_stream_primary_key", return_value=primary_key_fields
    ):
        if expected_error:
            with pytest.raises(expected_error):
                source._normalize_and_validate_pk_value("test_stream", pk_value)
        else:
            result = source._normalize_and_validate_pk_value("test_stream", pk_value)
            assert result == expected_result


def test_source_get_record_requires_declarative_executor() -> None:
    """Test get_record() raises NotImplementedError for non-declarative executors."""
    from airbyte._executors.python import VenvExecutor

    mock_executor = Mock(spec=VenvExecutor)
    source = Source(
        executor=mock_executor,
        name="test-source",
        config={"api_key": "test"},
    )

    with pytest.raises(
        NotImplementedError, match="only supported for declarative sources"
    ):
        source.get_record("test_stream", pk_value="123")


def test_source_get_record_calls_executor_fetch_record() -> None:
    """Test get_record() calls executor.fetch_record() with correct parameters."""
    mock_executor = Mock(spec=DeclarativeExecutor)
    mock_executor.fetch_record.return_value = {"id": "123", "name": "Test"}

    source = Source(
        executor=mock_executor,
        name="test-source",
        config={"api_key": "test"},
    )
    source._config_dict = {"api_key": "test"}

    with patch.object(source, "_normalize_and_validate_pk_value", return_value="123"):
        result = source.get_record("test_stream", pk_value="123")

    assert result == {"id": "123", "name": "Test"}
    mock_executor.fetch_record.assert_called_once_with(
        stream_name="test_stream",
        primary_key_value="123",
    )


@pytest.mark.parametrize(
    "pk_value",
    [
        pytest.param("123", id="string_pk"),
        pytest.param(123, id="int_pk"),
        pytest.param({"id": "123"}, id="dict_pk"),
    ],
)
def test_source_get_record_accepts_various_pk_formats(pk_value: any) -> None:
    """Test get_record() accepts various PK value formats."""
    mock_executor = Mock(spec=DeclarativeExecutor)
    mock_executor.fetch_record.return_value = {"id": "123", "name": "Test"}

    source = Source(
        executor=mock_executor,
        name="test-source",
        config={"api_key": "test"},
    )
    source._config_dict = {"api_key": "test"}

    with patch.object(source, "_normalize_and_validate_pk_value", return_value="123"):
        result = source.get_record("test_stream", pk_value=pk_value)

    assert result == {"id": "123", "name": "Test"}
    mock_executor.fetch_record.assert_called_once()
