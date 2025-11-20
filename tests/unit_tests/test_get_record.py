# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests for Source.get_record() and DeclarativeExecutor.fetch_record()."""

from __future__ import annotations

from typing import Any
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
    pk_value: Any,
    primary_key_fields: list[str],
    expected_result: str | None,
    expected_error: type[Exception] | None,
) -> None:
    """Test _normalize_and_validate_pk_value() handles various input formats."""
    from airbyte.shared.catalog_providers import CatalogProvider

    mock_executor = Mock()
    source = Source(
        executor=mock_executor,
        name="test-source",
        config={"api_key": "test"},
    )

    mock_catalog_provider = Mock(spec=CatalogProvider)
    mock_catalog_provider.get_primary_keys.return_value = primary_key_fields

    with patch.object(
        type(source), "catalog_provider", new_callable=PropertyMock
    ) as mock_provider_prop:
        mock_provider_prop.return_value = mock_catalog_provider

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
def test_source_get_record_accepts_various_pk_formats(pk_value: Any) -> None:
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
