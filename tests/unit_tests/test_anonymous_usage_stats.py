# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock

import airbyte as ab
import pytest
import responses
from airbyte._util import telemetry


@pytest.fixture(scope="function", autouse=True)
def autouse_source_test_registry(source_test_registry):
    return


@responses.activate
def test_telemetry_track(monkeypatch, source_test_registry):
    """Check that track is called and the correct data is sent."""
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)

    source_test = ab.get_source("source-test", install_if_missing=False)
    cache = ab.new_local_cache()

    # Add a response for the telemetry endpoint
    responses.add(responses.POST, "https://api.segment.io/v1/track", status=200)

    telemetry.send_telemetry(
        source=source_test._get_connector_runtime_info(),
        destination=None,
        cache=cache._get_writer_runtime_info(),
        state=telemetry.EventState.STARTED,
        number_of_records=0,
        event_type=telemetry.EventType.SYNC,
    )

    # Check that one request was made
    assert len(responses.calls) == 1

    # Parse the body of the first request as JSON
    body = json.loads(responses.calls[0].request.body)

    assert "properties" in body

    # Check that certain fields exist in 'properties' and are non-null
    for field in [
        "source",
        "cache",
        "state",
        "version",
        "python_version",
        "os",
        "application_hash",
    ]:
        assert body["properties"].get(field, None), (
            f"{field} is null in posted body: {body}"
        )

    assert body["properties"].get("source", {}).get("name") == "source-test", (
        f"field1 is null in posted body: {body}"
    )
    assert body["properties"].get("cache", {}).get("type") == "DuckDBCache", (
        f"field1 is null in posted body: {body}"
    )

    # Check for empty values:
    for field in body.keys():
        assert body[field], f"{field} is empty in posted body: {body}"


@pytest.mark.parametrize("do_not_track", ["1", "true", "t"])
@responses.activate
def test_do_not_track(
    monkeypatch,
    do_not_track,
    source_test_registry,
):
    """Check that track is called and the correct data is sent."""
    monkeypatch.setenv("DO_NOT_TRACK", do_not_track)

    source_test = ab.get_source("source-test", install_if_missing=False)
    cache = ab.new_local_cache()

    # Add a response for the telemetry endpoint
    responses.add(responses.POST, "https://api.segment.io/v1/track", status=200)
    responses.add(responses.GET, re.compile(".*"), status=200)

    telemetry.send_telemetry(
        source=source_test._get_connector_runtime_info(),
        destination=None,
        cache=cache._get_writer_runtime_info(),
        state=telemetry.EventState.STARTED,
        number_of_records=0,
        event_type=telemetry.EventType.SYNC,
    )

    # Check that zero requests were made, because DO_NOT_TRACK is set
    assert len(responses.calls) == 0


def test_setup_analytics_existing_file(monkeypatch):
    # Mock the environment variable and the analytics file
    monkeypatch.delenv(telemetry._ENV_ANALYTICS_ID, raising=False)
    monkeypatch.delenv(telemetry.DO_NOT_TRACK, raising=False)

    monkeypatch.setattr(Path, "exists", lambda x: True)
    monkeypatch.setattr(Path, "read_text", lambda x: "anonymous_user_id: test_id\n")
    assert telemetry._setup_analytics() == "test_id"


def test_setup_analytics_missing_file(monkeypatch):
    """Mock the environment variable and the missing analytics file."""
    monkeypatch.setenv(telemetry._ENV_ANALYTICS_ID, "test_id")
    monkeypatch.delenv(telemetry.DO_NOT_TRACK, raising=False)
    monkeypatch.setattr(Path, "exists", lambda x: False)

    mock_path = MagicMock()
    monkeypatch.setattr(Path, "write_text", mock_path)

    assert telemetry._setup_analytics() == "test_id"

    assert mock_path.call_count == 1


def test_setup_analytics_read_only_filesystem(monkeypatch, capfd):
    """Mock the environment variable and simulate a read-only filesystem."""
    monkeypatch.setenv(telemetry._ENV_ANALYTICS_ID, "test_id")
    monkeypatch.delenv(telemetry.DO_NOT_TRACK, raising=False)
    monkeypatch.setattr(Path, "exists", lambda x: False)

    mock_write_text = MagicMock(side_effect=PermissionError("Read-only filesystem"))
    monkeypatch.setattr(Path, "write_text", mock_write_text)

    # We should not raise an exception
    assert telemetry._setup_analytics() == "test_id"

    assert mock_write_text.call_count == 1

    # Capture print outputs
    captured = capfd.readouterr()

    # Validate print message
    assert "Read-only filesystem" not in captured.out


def test_setup_analytics_corrupt_file(monkeypatch):
    """Mock the environment variable and the missing analytics file."""
    monkeypatch.delenv(telemetry._ENV_ANALYTICS_ID, raising=False)
    monkeypatch.delenv(telemetry.DO_NOT_TRACK, raising=False)
    monkeypatch.setattr(Path, "exists", lambda x: True)
    monkeypatch.setattr(Path, "read_text", lambda x: "not-a-valid ::: yaml file\n")

    mock = MagicMock()
    monkeypatch.setattr(Path, "write_text", mock)

    assert telemetry._setup_analytics()

    assert mock.call_count == 1


def test_get_analytics_id(monkeypatch):
    # Mock the _ANALYTICS_ID variable
    monkeypatch.delenv(telemetry._ENV_ANALYTICS_ID, raising=False)
    monkeypatch.delenv(telemetry.DO_NOT_TRACK, raising=False)
    monkeypatch.setattr(telemetry, "_ANALYTICS_ID", "test_id")

    mock = MagicMock()
    monkeypatch.setattr(Path, "write_text", mock)

    assert telemetry._get_analytics_id() == "test_id"
