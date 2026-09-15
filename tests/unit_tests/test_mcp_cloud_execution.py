# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Wire contracts for the Cloud MCP transport, using a real local HTTP server."""

from __future__ import annotations

import json
import socket
import threading
from collections import deque
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from airbyte.mcp import _cloud_execution
from airbyte.mcp._cloud_execution import CloudExecutionClient, CloudExecutionError
from airbyte.secrets import SecretString


ACTOR_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
WORKSPACE_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
BODY = {"entity": "items", "action": "list", "params": {}}
DOCS = {
    "metadata": {
        "id": f"connector-source:{ACTOR_ID}",
        "kind": "connector_source",
        "title": "Connector",
        "provenance": "yaml",
        "version": "1",
        "freshness": {"state": "live"},
    },
    "outline": [{"id": "actions.items.list", "title": "List"}],
}


class CloudServer:
    """Record requests and serve queued wire responses without transport mocks."""

    def __init__(self) -> None:
        self.responses: deque[tuple[int, bytes, str]] = deque()
        self.requests: list[dict[str, Any]] = []
        self.release = threading.Event()
        self.server: ThreadingHTTPServer
        self.url: str

    def respond(self, payload: Any, status: int = 200, mode: str = "normal") -> None:
        self.responses.append((status, json.dumps(payload).encode(), mode))


@pytest.fixture
def cloud_server() -> Iterator[CloudServer]:
    state = CloudServer()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.handle_request()

        def do_POST(self) -> None:
            self.handle_request()

        def log_message(self, format: str, *args: Any) -> None:
            pass

        def handle_request(self) -> None:
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            state.requests.append({
                "method": self.command,
                "path": self.path,
                "headers": dict(self.headers),
                "body": body,
            })
            status, content, mode = state.responses.popleft()
            if mode == "disconnect":
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Location", state.url + "/redirect-target")
            self.end_headers()
            if mode == "stall":
                state.release.wait(2)
                return
            try:
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError):
                pass

    state.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    state.url = f"http://127.0.0.1:{state.server.server_port}"
    thread = threading.Thread(target=state.server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        state.release.set()
        state.server.shutdown()
        state.server.server_close()
        thread.join()


@pytest.fixture
def client(cloud_server: CloudServer) -> CloudExecutionClient:
    return CloudExecutionClient(
        api_root=cloud_server.url + "/public/v1",
        config_api_root=cloud_server.url + "/api/v1/",
        bearer_token="secret-token",
    )


@pytest.mark.parametrize("data", [None, 0, False, "value", [], {}, [{"field": None}]])
def test_execute_preserves_json(
    cloud_server: CloudServer, client: CloudExecutionClient, data: Any
) -> None:
    cloud_server.respond({"data": data, "meta": {"opaque": {"cursor": None}}})
    result = client.execute_source(ACTOR_ID, BODY)
    assert result["data"] == data
    assert result["meta"] == {"opaque": {"cursor": None}}
    request = cloud_server.requests[0]
    assert request["path"] == f"/api/v1/sources/{ACTOR_ID}/execute"
    assert request["method"] == "POST"
    assert request["headers"]["authorization"] == "Bearer secret-token"
    assert json.loads(request["body"]) == BODY
    assert not any("organization" in key.lower() for key in request["headers"])


def test_destination_rows_are_positional(
    cloud_server: CloudServer, client: CloudExecutionClient
) -> None:
    cloud_server.respond({"data": {"data": [["1", None]], "columns": ["id", "id"]}})
    assert client.execute_destination(ACTOR_ID, BODY)["data"]["data"] == [["1", None]]
    assert (
        cloud_server.requests[0]["path"] == f"/api/v1/destinations/{ACTOR_ID}/execute"
    )


def test_docs_encoding_and_default_content(
    cloud_server: CloudServer, client: CloudExecutionClient
) -> None:
    cloud_server.respond(DOCS)
    section = "actions.items.list &more=/#雪"
    result = client.read_docs(WORKSPACE_ID, f"connector-source:{ACTOR_ID}", section)
    assert result["content"] == []
    request = cloud_server.requests[0]
    url = urlsplit(request["path"])
    assert url.path == f"/api/v1/workspaces/{WORKSPACE_ID}/skills/docs"
    assert parse_qs(url.query) == {
        "id": [f"connector-source:{ACTOR_ID}"],
        "section": [section],
    }
    assert request["body"] == b""


@pytest.mark.parametrize(
    "status", [301, 307, 308, 401, 403, 404, 408, 413, 422, 429, 502, 503, 504]
)
def test_failures_never_replay(
    cloud_server: CloudServer, client: CloudExecutionClient, status: int
) -> None:
    cloud_server.respond({"detail": "secret-token SELECT confidential"}, status)
    with pytest.raises(CloudExecutionError) as caught:
        client.execute_source(ACTOR_ID, BODY)
    assert caught.value.status_code == status
    assert "secret-token" not in str(caught.value)
    assert "confidential" not in str(caught.value)
    assert len(cloud_server.requests) == 1
    cloud_server.respond({"data": None})
    assert client.execute_source(ACTOR_ID, BODY) == {"data": None}
    assert len(cloud_server.requests) == 2


@pytest.mark.parametrize("mode", ["disconnect", "stall"])
def test_interruption_reports_unknown_outcome(
    cloud_server: CloudServer,
    client: CloudExecutionClient,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    monkeypatch.setattr(_cloud_execution, "_TIMEOUT", (0.1, 0.1))
    cloud_server.respond({"data": []}, mode=mode)
    with pytest.raises(CloudExecutionError, match="outcome is unknown"):
        client.execute_source(ACTOR_ID, BODY)
    assert len(cloud_server.requests) == 1
    cloud_server.respond({"data": []})
    assert client.execute_source(ACTOR_ID, BODY) == {"data": []}


@pytest.mark.parametrize(
    "payload", [[], {}, {"data": 1, "meta": None}, {"data": 1, "meta": []}]
)
def test_invalid_execute_envelope(
    cloud_server: CloudServer, client: CloudExecutionClient, payload: Any
) -> None:
    cloud_server.respond(payload)
    with pytest.raises(CloudExecutionError):
        client.execute_source(ACTOR_ID, BODY)


@pytest.mark.parametrize(
    "wire", [b"not json", b'{"data":"\xff"}', b'{"data": NaN}', b'{"data":']
)
def test_invalid_json(
    cloud_server: CloudServer, client: CloudExecutionClient, wire: bytes
) -> None:
    cloud_server.responses.append((200, wire, "normal"))
    with pytest.raises(CloudExecutionError, match="Invalid JSON"):
        client.execute_source(ACTOR_ID, BODY)


def test_response_cap(cloud_server: CloudServer, client: CloudExecutionClient) -> None:
    cloud_server.respond({"data": "x" * (1024 * 1024)})
    with pytest.raises(CloudExecutionError, match="Response exceeds"):
        client.execute_source(ACTOR_ID, BODY)


@pytest.mark.parametrize("actor", ["../x", "", "not-a-uuid"])
def test_path_rejected_before_io(
    cloud_server: CloudServer, client: CloudExecutionClient, actor: str
) -> None:
    with pytest.raises(CloudExecutionError, match="UUID"):
        client.execute_source(actor, BODY)
    assert not cloud_server.requests


def test_request_bounds_before_io(
    cloud_server: CloudServer, client: CloudExecutionClient
) -> None:
    with pytest.raises(CloudExecutionError, match="Request exceeds"):
        client.execute_source(ACTOR_ID, {"sql": "secret" * 12000})
    with pytest.raises(CloudExecutionError, match="query"):
        client.read_docs(WORKSPACE_ID, "x" * 2049)
    assert not cloud_server.requests


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"metadata": {}, "outline": []},
        {**DOCS, "content": None},
        {**DOCS, "outline": [{}]},
    ],
)
def test_docs_schema(
    cloud_server: CloudServer, client: CloudExecutionClient, payload: Any
) -> None:
    cloud_server.respond(payload)
    with pytest.raises(CloudExecutionError, match="documentation response"):
        client.read_docs(WORKSPACE_ID, "connector-source:" + ACTOR_ID)


def test_application_token_reused_for_discovery_and_execution(
    cloud_server: CloudServer,
) -> None:
    client = CloudExecutionClient(
        api_root=cloud_server.url + "/api/public/v1",
        client_id="app-id",
        client_secret="app-secret",
    )
    cloud_server.respond({"access_token": "app-token"})
    assert isinstance(client.bearer_token, SecretString)
    assert client.bearer_token == "app-token"
    cloud_server.respond({"data": None})
    client.execute_source(ACTOR_ID, BODY)
    assert len(cloud_server.requests) == 2
    assert cloud_server.requests[0]["path"] == "/api/public/v1/applications/token"
    assert json.loads(cloud_server.requests[0]["body"]) == {
        "client_id": "app-id",
        "client_secret": "app-secret",
    }
    assert "authorization" not in cloud_server.requests[0]["headers"]
    assert cloud_server.requests[1]["headers"]["authorization"] == "Bearer app-token"


def test_bearer_precedence(cloud_server: CloudServer) -> None:
    client = CloudExecutionClient(
        api_root=cloud_server.url + "/api/public/v1",
        bearer_token="chosen",
        client_id="unused",
        client_secret="unused",
    )
    assert client.bearer_token == "chosen"
    assert not cloud_server.requests


@pytest.mark.parametrize(
    "payload", [{}, {"access_token": ""}, {"access_token": 123}, {"access_token": None}]
)
def test_invalid_token(cloud_server: CloudServer, payload: Any) -> None:
    client = CloudExecutionClient(
        api_root=cloud_server.url + "/api/public/v1",
        client_id="id",
        client_secret="secret",
    )
    cloud_server.respond(payload)
    with pytest.raises(CloudExecutionError, match="Invalid token"):
        _ = client.bearer_token


@pytest.mark.parametrize(
    "status,mode", [(307, "normal"), (308, "normal"), (401, "normal"), (200, "stall")]
)
def test_token_failures_do_not_replay(
    cloud_server: CloudServer, monkeypatch: pytest.MonkeyPatch, status: int, mode: str
) -> None:
    monkeypatch.setattr(_cloud_execution, "_TIMEOUT", (0.1, 0.1))
    client = CloudExecutionClient(
        api_root=cloud_server.url + "/api/public/v1",
        client_id="id",
        client_secret="app-secret",
    )
    cloud_server.respond({"detail": "app-secret"}, status, mode)
    with pytest.raises(CloudExecutionError) as caught:
        _ = client.bearer_token
    assert "app-secret" not in str(caught.value)
    assert len(cloud_server.requests) == 1


def test_execution_401_never_reexchanges_token(cloud_server: CloudServer) -> None:
    client = CloudExecutionClient(
        api_root=cloud_server.url + "/api/public/v1",
        client_id="id",
        client_secret="secret",
    )
    cloud_server.respond({"access_token": "app-token"})
    cloud_server.respond({"detail": "denied"}, 401)
    with pytest.raises(CloudExecutionError):
        client.execute_source(ACTOR_ID, BODY)
    assert len(cloud_server.requests) == 2
    assert (
        sum(
            request["path"].endswith("/applications/token")
            for request in cloud_server.requests
        )
        == 1
    )
    assert client.bearer_token == "app-token"
    assert len(cloud_server.requests) == 2


@pytest.mark.parametrize("secret", [None, "", SecretString(""), " "])
def test_missing_credentials_make_no_requests(
    cloud_server: CloudServer, secret: str | None
) -> None:
    client = CloudExecutionClient(
        api_root=cloud_server.url + "/api/public/v1",
        bearer_token=secret,
        client_id="id",
        client_secret=secret,
    )
    with pytest.raises(CloudExecutionError, match="credentials"):
        _ = client.bearer_token
    assert not cloud_server.requests


def test_docs_response_cap(
    cloud_server: CloudServer, client: CloudExecutionClient
) -> None:
    cloud_server.respond({
        **DOCS,
        "content": [{"type": "paragraph", "text": "x" * (1024 * 1024)}],
    })
    with pytest.raises(CloudExecutionError, match="Response exceeds"):
        client.read_docs(WORKSPACE_ID, "connector-source:" + ACTOR_ID)


def test_config_root_environment_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRBYTE_CLOUD_CONFIG_API_URL", "https://config.example/api/v1/")
    client = CloudExecutionClient(
        api_root="https://public.example/v1", bearer_token="token"
    )
    assert client.config_api_root == "https://config.example/api/v1"


def test_invalid_request_json(
    cloud_server: CloudServer, client: CloudExecutionClient
) -> None:
    with pytest.raises(CloudExecutionError, match="Invalid JSON request"):
        client.execute_source(ACTOR_ID, {"params": {"number": float("nan")}})
    assert not cloud_server.requests


@pytest.mark.parametrize(
    "case", ["success", "denied", "malformed", "oversized", "timeout"]
)
def test_response_closed_on_every_path(
    cloud_server: CloudServer,
    client: CloudExecutionClient,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    responses: list[requests.Response] = []
    original_request = requests.Session.request

    def capture_response(
        session: requests.Session, *args: Any, **kwargs: Any
    ) -> requests.Response:
        response = original_request(session, *args, **kwargs)
        responses.append(response)
        return response

    monkeypatch.setattr(requests.Session, "request", capture_response)
    monkeypatch.setattr(_cloud_execution, "_TIMEOUT", (0.1, 0.1))
    if case == "denied":
        cloud_server.respond({"detail": "denied"}, 403)
    elif case == "malformed":
        cloud_server.responses.append((200, b"broken", "normal"))
    elif case == "oversized":
        cloud_server.respond({"data": "x" * (1024 * 1024)})
    else:
        cloud_server.respond(
            {"data": None}, mode="stall" if case == "timeout" else "normal"
        )
    if case == "success":
        client.execute_source(ACTOR_ID, BODY)
    else:
        with pytest.raises(CloudExecutionError):
            client.execute_source(ACTOR_ID, BODY)
    assert len(responses) == 1
    assert responses[0].raw.closed


@pytest.mark.parametrize("application_credentials", [False, True])
def test_proxy_and_ca_settings_survive_without_netrc_authentication(
    cloud_server: CloudServer,
    monkeypatch: pytest.MonkeyPatch,
    application_credentials: bool,
) -> None:
    # The fixture acts as a real HTTP proxy for a hostname that cannot resolve.
    monkeypatch.setenv("HTTP_PROXY", cloud_server.url)
    monkeypatch.setenv("http_proxy", cloud_server.url)
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/configured/company-ca.pem")
    monkeypatch.setattr(
        requests.sessions, "get_netrc_auth", lambda url: ("wrong", "wrong")
    )
    observed: list[Any] = []
    original_send = requests.Session.send

    def send(
        session: requests.Session, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:
        observed.append(kwargs["verify"])
        return original_send(session, request, **kwargs)

    monkeypatch.setattr(requests.Session, "send", send)
    credentials = (
        {"client_id": "app", "client_secret": "secret"}
        if application_credentials
        else {"bearer_token": "chosen-token"}
    )
    transport = CloudExecutionClient(
        api_root="http://cloud.invalid/public/v1",
        config_api_root="http://cloud.invalid/api/v1",
        **credentials,
    )
    if application_credentials:
        cloud_server.respond({"access_token": "chosen-token"})
    cloud_server.respond({"data": None})
    assert transport.execute_source(ACTOR_ID, BODY) == {"data": None}
    assert (
        cloud_server.requests[-1]["path"]
        == f"http://cloud.invalid/api/v1/sources/{ACTOR_ID}/execute"
    )
    assert (
        cloud_server.requests[-1]["headers"]["authorization"] == "Bearer chosen-token"
    )
    if application_credentials:
        assert "authorization" not in cloud_server.requests[0]["headers"]
    assert observed == ["/configured/company-ca.pem"] * len(cloud_server.requests)
