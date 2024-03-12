# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import itertools
from contextlib import nullcontext as does_not_raise
import json
import os
from pathlib import Path
import re
from unittest.mock import MagicMock, call, patch
from freezegun import freeze_time

import responses

import airbyte as ab
import pytest

from airbyte.version import get_version
import airbyte as ab
from airbyte._util import telemetry


@responses.activate
def test_telemetry_track(monkeypatch):
    """Check that track is called and the correct data is sent."""
    monkeypatch.delenv('DO_NOT_TRACK', raising=False)

    source_test = ab.get_source("source-test", install_if_missing=False)
    cache = ab.new_local_cache()

    # Add a response for the telemetry endpoint
    responses.add(responses.POST, 'https://api.segment.io/v1/track', status=200)

    telemetry.send_telemetry(
        source=source_test,
        cache=cache,
        state="started",
        number_of_records=0,
    )

    # Check that one request was made
    assert len(responses.calls) == 1

    # Parse the body of the first request as JSON
    body = json.loads(responses.calls[0].request.body)

    assert "properties" in body

    # Check that certain fields exist in 'properties' and are non-null
    for field in [
        "source", "cache", "state", "version", "python_version", "os", "application_hash"
    ]:
        assert body["properties"].get(field, None), f"{field} is null in posted body: {body}"

    assert body["properties"].get("source", {}).get("name") == "source-test", f"field1 is null in posted body: {body}"
    assert body["properties"].get("cache", {}).get("type") == "DuckDBCache", f"field1 is null in posted body: {body}"

    # Check for empty values:
    for field in body.keys():
        assert body[field], f"{field} is empty in posted body: {body}"




@pytest.mark.parametrize("do_not_track", ['1', 'true', 't'])
@responses.activate
def test_do_not_track(monkeypatch, do_not_track):
    """Check that track is called and the correct data is sent."""
    monkeypatch.setenv('DO_NOT_TRACK', do_not_track)

    source_test = ab.get_source("source-test", install_if_missing=False)
    cache = ab.new_local_cache()

    # Add a response for the telemetry endpoint
    responses.add(responses.POST, 'https://api.segment.io/v1/track', status=200)
    responses.add(responses.GET, re.compile('.*'), status=200)

    telemetry.send_telemetry(
        source=source_test,
        cache=cache,
        state="started",
        number_of_records=0,
    )

    # Check that zero requests were made, because DO_NOT_TRACK is set
    assert len(responses.calls) == 0


@pytest.mark.xfail(reason="This test is too brittle and should be rewritten.")
@freeze_time("2021-01-01T00:00:00.000000")
@patch.dict('os.environ', {'DO_NOT_TRACK': ''})
@responses.activate
@pytest.mark.parametrize(
    "raises, api_key, expected_state, expected_number_of_records, request_call_fails, extra_env, expected_flags, cache_type, number_of_records_read",
    [
        pytest.param(pytest.raises(Exception), "test_fail_during_sync", "failed", 1, False, {"CI": ""}, {}, "duckdb", None, id="fail_during_sync"),
        pytest.param(does_not_raise(), "test", "succeeded", 3, False, {"CI": ""}, {}, "duckdb", None, id="succeed_during_sync"),
        pytest.param(does_not_raise(), "test", "succeeded", 3, True, {"CI": ""}, {}, "duckdb", None,id="fail_request_without_propagating"),
        pytest.param(does_not_raise(), "test", "succeeded", 3, False, {"CI": ""}, {}, "duckdb", None,id="falsy_ci_flag"),
        pytest.param(does_not_raise(), "test", "succeeded", 3, False, {"CI": "true"}, {"CI": True}, "duckdb", None,id="truthy_ci_flag"),
        pytest.param(pytest.raises(Exception), "test_fail_during_sync", "failed", 1,  False, {"CI": ""}, {}, "streaming", 3, id="streaming_fail_during_sync"),
        pytest.param(does_not_raise(), "test", "succeeded", 2,  False, {"CI": ""}, {}, "streaming", 2, id="streaming_succeed"),
        pytest.param(does_not_raise(), "test", "succeeded", 1,  False, {"CI": ""}, {}, "streaming", 1, id="streaming_partial_read"),
    ],
)
def test_tracking(
    raises, api_key: str,
    expected_state: str,
    expected_number_of_records: int,
    request_call_fails: bool,
    extra_env: dict[str, str],
    expected_flags: dict[str, bool],
    cache_type: str,
    number_of_records_read: int
):
    """
    Test that the telemetry is sent when the sync is successful.
    This is done by mocking the requests.post method and checking that it is called with the right arguments.
    """
    source = ab.get_source("source-test", config={"apiKey": api_key})
    source.select_all_streams()

    cache = ab.new_local_cache()

    with patch.dict('os.environ', extra_env):
        with raises:
            if cache_type == "streaming":
                list(itertools.islice(source.get_records("stream1"), number_of_records_read))
            else:
                source.read(cache)

    mock_post.assert_has_calls([
        call(
            "https://api.segment.io/v1/track",
            auth=("cukeSffc0G6gFQehKDhhzSurDzVSZ2OP", ""),
            json={
                'anonymousId': 'airbyte-lib-user',
                'event': 'sync',
                'timestamp': '2021-01-01T00:00:00.000000',
                'properties': {
                    'session_id': '01HQA7CYZTT9S2S25397KJP49A',
                    'source': {
                        'name': 'source-test',
                        'executor_type': 'VenvExecutor',
                        'version': '0.0.1',
                    },
                    'cache': {'type': 'DuckDBCache'},
                    'state': "started",
                    'version': '0.0.0',
                    'python_version': '3.10.12 (CPython)',
                    'os': 'Darwin',
                    'application_hash': '46d4f7bf13805130b477f8691a3ba5b8786453474b1d5ecb06510d7ea72fe4c0',
                    'ip': '0.0.0.0',
                    'flags': {'CI': True},
                }
            }
        ),
        call(
            "https://api.segment.io/v1/track",
            auth=("cukeSffc0G6gFQehKDhhzSurDzVSZ2OP", ""),
            json={
                "anonymousId": "airbyte-lib-user",
                "event": "sync",
                "properties": {
                    "version": get_version(),
                    "source": {'name': 'source-test', 'version': '0.0.1', 'type': 'venv'},
                    "state": expected_state,
                    "number_of_records": expected_number_of_records,
                    "cache": {"type": cache_type},
                    "ip": "0.0.0.0",
                    "flags": expected_flags
                },
                "timestamp": "2021-01-01T00:00:00.000000",
            }
        )
    ])

def test_setup_analytics_existing_file(monkeypatch):
    # Mock the environment variable and the analytics file
    monkeypatch.setattr(Path, 'exists', lambda x: True)
    monkeypatch.setattr(Path, 'read_text', lambda x: "anonymous_user_id: test_id\n")
    assert telemetry._setup_analytics() == 'test_id'


def test_setup_analytics_missing_file(monkeypatch):
    """Mock the environment variable and the missing analytics file."""
    monkeypatch.setenv(telemetry._ENV_ANALYTICS_ID, 'test_id')
    monkeypatch.setattr(Path, 'exists', lambda x: False)

    mock_path = MagicMock()
    monkeypatch.setattr(Path, 'write_text', mock_path)

    assert telemetry._setup_analytics() == 'test_id'

    assert mock_path.call_count == 1


def test_setup_analytics_corrupt_file(monkeypatch):
    """Mock the environment variable and the missing analytics file."""
    monkeypatch.setattr(Path, 'exists', lambda x: True)
    monkeypatch.setattr(Path, 'read_text', lambda x: "not-a-valid ::: yaml file\n")

    mock = MagicMock()
    monkeypatch.setattr(Path, 'write_text', mock)

    assert telemetry._setup_analytics()

    assert mock.call_count == 1


def test_get_analytics_id(monkeypatch):
    # Mock the _ANALYTICS_ID variable
    monkeypatch.setattr(telemetry, '_ANALYTICS_ID', 'test_id')

    mock = MagicMock()
    monkeypatch.setattr(Path, 'write_text', mock)

    assert telemetry._get_analytics_id() == 'test_id'
