# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Exercise Cloud rate limiting through real HTTP transport without Cloud credentials."""

from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest.mock import patch

import pytest
import requests
from airbyte_api.errors import SDKError

from airbyte._util import api_util
from airbyte.secrets.base import SecretString


@contextmanager
def endpoint(
    statuses: list[int], retry_after: str | None = None
) -> Iterator[tuple[str, list[str]]]:
    """Serve a finite response sequence on localhost and record actual requests."""
    methods: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def respond(self) -> None:
            methods.append(self.command)
            status = statuses[min(len(methods) - 1, len(statuses) - 1)]
            if status == 0:
                self.close_connection = True
                return
            self.send_response(status)
            if retry_after is not None:
                self.send_header("Retry-After", retry_after)
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_GET = respond
        do_DELETE = respond
        do_POST = respond
        do_PATCH = respond

        def log_message(self, *_args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", methods
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def delete_destination(url: str) -> None:
    """Use the real PyAirbyte wrapper, SDK, session and adapter."""
    api_util.delete_destination(
        "00000000-0000-0000-0000-000000000001",
        destination_name="deleteme-local-test",
        api_root=url,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("test-token"),
    )


def test_delete_destination_recovers_from_rate_limit() -> None:
    with endpoint([429, 204], retry_after="0") as (url, methods):
        delete_destination(url)
    assert methods == ["DELETE", "DELETE"]


@pytest.mark.parametrize("bearer_auth", [True, False])
@pytest.mark.parametrize("method", ["GET", "DELETE"])
def test_both_authentication_modes_retry_safe_methods(
    bearer_auth: bool, method: str
) -> None:
    with endpoint([429, 204], retry_after="0") as (url, methods):
        instance = api_util.get_airbyte_server_instance(
            api_root=url,
            client_id=None if bearer_auth else SecretString("client"),
            client_secret=None if bearer_auth else SecretString("secret"),
            bearer_token=SecretString("token") if bearer_auth else None,
        )
        # Exercise the configured transport without invoking an OAuth token endpoint.
        with instance.sdk_configuration.client as client:
            response = client.request(method, url)
        assert response.status_code == 204
    assert methods == [method, method]


def test_exhausted_rate_limit_preserves_sdk_error() -> None:
    with patch("urllib3.util.retry.time.sleep"), endpoint([429]) as (url, methods):
        with pytest.raises(SDKError) as exc_info:
            delete_destination(url)
    assert exc_info.value.status_code == 429
    assert methods == ["DELETE"] * 4


@pytest.mark.parametrize("status", [400, 500, 503])
def test_other_errors_are_not_retried(status: int) -> None:
    with endpoint([status, 204], retry_after="5") as (url, methods):
        with pytest.raises(SDKError) as exc_info:
            delete_destination(url)
    assert exc_info.value.status_code == status
    assert methods == ["DELETE"]


@pytest.mark.parametrize("method", ["POST", "PATCH"])
def test_non_idempotent_requests_are_not_replayed(method: str) -> None:
    with endpoint([429, 204], retry_after="0") as (url, methods):
        instance = api_util.get_airbyte_server_instance(
            api_root=url,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )
        with instance.sdk_configuration.client as client:
            assert client.request(method, url).status_code == 429
    assert methods == [method]


def test_retry_after_is_respected() -> None:
    with (
        patch("urllib3.util.retry.time.sleep") as sleep,
        endpoint([429, 204], "2") as (url, _),
    ):
        delete_destination(url)
    sleep.assert_called_once_with(2)


def test_rate_limit_backoff_without_retry_after() -> None:
    with (
        patch("urllib3.util.retry.time.sleep") as sleep,
        endpoint([429, 429, 204]) as (url, _),
    ):
        delete_destination(url)
    sleep.assert_called_once_with(2.0)


def test_interrupted_response_is_not_replayed() -> None:
    with endpoint([0, 204]) as (url, methods):
        with pytest.raises(requests.exceptions.ConnectionError):
            delete_destination(url)
    assert methods == ["DELETE"]
