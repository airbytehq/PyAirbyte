# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Credential file helpers for Airbyte Cloud authentication."""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import socket
import sys
import threading
import webbrowser
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from os import environ
from pathlib import Path
from queue import Empty, Queue
from typing import TextIO
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import yaml

from airbyte._util.api_util import get_bearer_token, status_ok
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_ORGANIZATION_ID_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.secrets.base import SecretString


CREDENTIALS_FILE_PATH = Path("~/.airbyte/credentials").expanduser()
CLIENT_ID_ENV_VAR = "AIRBYTE_CLIENT_ID"
CLIENT_SECRET_ENV_VAR = "AIRBYTE_CLIENT_SECRET"
WORKSPACE_ID_ENV_VAR = "AIRBYTE_WORKSPACE_ID"
ORGANIZATION_ID_ENV_VAR = "AIRBYTE_ORGANIZATION_ID"
PUBLIC_API_ROOT_ENV_VAR = "AIRBYTE_API_ROOT"
BEARER_TOKEN_ENV_VAR = "AIRBYTE_BEARER_TOKEN"
CONFIG_API_ROOT_ENV_VAR = "AIRBYTE_CONFIG_API_ROOT"
KEYCLOAK_BASE_URL = "https://cloud.airbyte.com/auth/realms/_airbyte-cloud-users"
KEYCLOAK_CLIENT_ID = "sonar-webapp"
KEYCLOAK_SCOPES = "openid email profile offline_access"
CALLBACK_PATH = "/callback"
KEYCLOAK_CALLBACK_HOST = "127.0.0.1"
AIRBYTE_AI_API_HOST = "https://api.airbyte.ai"
AIRBYTE_AI_API_ROOT = f"{AIRBYTE_AI_API_HOST}/api/v1"
DEFAULT_BROWSER_LOGIN_TIMEOUT_SECONDS = 180


@dataclass(frozen=True)
class CloudLoginResult:
    """Result of a successful Cloud login."""

    credentials_file_path: Path
    airbyte_api_root: str
    config_api_root: str
    organization_id: str | None = None


@dataclass(frozen=True)
class CloudCredentials:
    """Resolved credentials and API roots for Airbyte control-plane APIs."""

    client_id: SecretString | None
    client_secret: SecretString | None
    bearer_token: SecretString | None
    public_api_root: str
    config_api_root: str | None
    workspace_id: str | None = None
    organization_id: str | None = None


def _as_string_mapping(parsed: object) -> dict[str, str]:
    """Return a string-only mapping from parsed YAML content."""
    if not isinstance(parsed, dict):
        return {}

    result: dict[str, str] = {}
    for key, value in parsed.items():
        if isinstance(key, str) and value is not None:
            result[key] = str(value)

    return result


def _first_value(*values: str | None) -> str | None:
    """Return the first non-empty string value."""
    for value in values:
        if value:
            return value
    return None


def _env_value(*names: str) -> str | None:
    """Return the first available environment variable value."""
    for name in names:
        value = environ.get(name)
        if value:
            return value
    return None


def read_credentials_file(
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> dict[str, str]:
    """Read Airbyte credentials from a YAML credentials file."""
    if not credentials_file_path.exists():
        return {}

    try:
        content = credentials_file_path.read_text(encoding="utf-8").strip()
        parsed = yaml.safe_load(content) if content else {}
    except (OSError, yaml.YAMLError):
        return {}

    return _as_string_mapping(parsed)


def resolve_cloud_credentials(
    *,
    workspace_id: str | None = None,
    organization_id: str | None = None,
    client_id: str | SecretString | None = None,
    client_secret: str | SecretString | None = None,
    bearer_token: str | SecretString | None = None,
    public_api_root: str | None = None,
    config_api_root: str | None = None,
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> CloudCredentials:
    """Resolve Airbyte Cloud credentials from inputs, env vars, and credentials file."""
    credentials_file = read_credentials_file(credentials_file_path)
    resolved_bearer_token = _first_value(
        str(bearer_token) if bearer_token is not None else None,
        _env_value(BEARER_TOKEN_ENV_VAR, CLOUD_BEARER_TOKEN_ENV_VAR),
        credentials_file.get("bearer_token"),
    )
    resolved_client_id = _first_value(
        str(client_id) if client_id is not None else None,
        _env_value(CLIENT_ID_ENV_VAR, CLOUD_CLIENT_ID_ENV_VAR),
        credentials_file.get("client_id"),
    )
    resolved_client_secret = _first_value(
        str(client_secret) if client_secret is not None else None,
        _env_value(CLIENT_SECRET_ENV_VAR, CLOUD_CLIENT_SECRET_ENV_VAR),
        credentials_file.get("client_secret"),
    )

    if resolved_bearer_token and (resolved_client_id or resolved_client_secret):
        resolved_client_id = None
        resolved_client_secret = None
    elif bool(resolved_client_id) != bool(resolved_client_secret):
        raise PyAirbyteInputError(
            message="Client ID and client secret are both required.",
            guidance="Provide both client ID and client secret, or use a bearer token.",
        )
    elif not resolved_bearer_token and not resolved_client_id:
        raise PyAirbyteInputError(
            message="No Airbyte credentials found.",
            guidance=(
                "Set Airbyte Cloud credentials in environment variables or "
                f"create a credentials file at {credentials_file_path}."
            ),
        )

    return CloudCredentials(
        client_id=SecretString(resolved_client_id) if resolved_client_id else None,
        client_secret=SecretString(resolved_client_secret) if resolved_client_secret else None,
        bearer_token=SecretString(resolved_bearer_token) if resolved_bearer_token else None,
        public_api_root=_first_value(
            public_api_root,
            _env_value(PUBLIC_API_ROOT_ENV_VAR, CLOUD_API_ROOT_ENV_VAR),
            credentials_file.get("airbyte_api_root"),
            credentials_file.get("public_api_root"),
            credentials_file.get("api_url"),
        )
        or CLOUD_API_ROOT,
        config_api_root=_first_value(
            config_api_root,
            _env_value(CONFIG_API_ROOT_ENV_VAR, CLOUD_CONFIG_API_ROOT_ENV_VAR),
            credentials_file.get("config_api_root"),
        ),
        workspace_id=_first_value(
            workspace_id,
            _env_value(WORKSPACE_ID_ENV_VAR, CLOUD_WORKSPACE_ID_ENV_VAR),
            credentials_file.get("workspace_id"),
        ),
        organization_id=_first_value(
            organization_id,
            _env_value(ORGANIZATION_ID_ENV_VAR, CLOUD_ORGANIZATION_ID_ENV_VAR),
            credentials_file.get("organization_id"),
        ),
    )


def write_credentials_file(
    credentials: dict[str, str],
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> None:
    """Write Airbyte credentials to a YAML credentials file."""
    credentials_file_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_file_path.write_text(
        yaml.safe_dump(dict(credentials), sort_keys=True),
        encoding="utf-8",
    )
    credentials_file_path.chmod(0o600)


@dataclass(frozen=True)
class _OAuthCallbackResult:
    """OAuth callback result captured by the loopback server."""

    code: str | None = None
    state: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class _OrganizationItem:
    """Organization item returned by the bootstrap API."""

    id: str
    name: str


class _OAuthCallbackHTTPServer(HTTPServer):
    """Loopback OAuth server with a typed result queue."""

    result_queue: Queue[_OAuthCallbackResult]


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Handle one browser OAuth redirect."""

    def do_GET(self) -> None:  # noqa: N802
        """Capture the OAuth callback and render a browser result page."""
        server = self.server
        if not isinstance(server, _OAuthCallbackHTTPServer):
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        parsed_url = urlparse(self.path)
        if parsed_url.path != CALLBACK_PATH:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        query = parse_qs(parsed_url.query)
        error = _first_query_value(query, "error")
        error_description = _first_query_value(query, "error_description")
        if error:
            message = f"{error}: {error_description}" if error_description else error
            self._write_html(
                status=HTTPStatus.OK,
                body=(
                    "<!doctype html><html><body><h1>Login failed</h1>"
                    f"<p>{message}</p><p>You can close this window.</p></body></html>"
                ),
            )
            server.result_queue.put(_OAuthCallbackResult(error=message))
            return

        code = _first_query_value(query, "code")
        state = _first_query_value(query, "state")
        if not code or not state:
            self._write_html(
                status=HTTPStatus.BAD_REQUEST,
                body=(
                    "<!doctype html><html><body><h1>Login failed</h1>"
                    "<p>Missing code or state in callback.</p>"
                    "<p>You can close this window.</p></body></html>"
                ),
            )
            server.result_queue.put(
                _OAuthCallbackResult(error="Callback missing code or state.")
            )
            return

        self._write_html(
            status=HTTPStatus.OK,
            body=(
                "<!doctype html><html><body><h1>Login successful</h1>"
                "<p>You can close this window and return to the terminal.</p>"
                "</body></html>"
            ),
        )
        server.result_queue.put(_OAuthCallbackResult(code=code, state=state))

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Suppress default loopback request logging."""

    def _write_html(self, *, status: HTTPStatus, body: str) -> None:
        """Write a minimal HTML response."""
        self.send_response(status.value)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))


def _validate_client_credentials(
    *,
    client_id: str | None,
    client_secret: str | None,
) -> tuple[str, str]:
    """Validate and return client credentials for non-interactive login."""
    if not client_id or not client_secret:
        raise PyAirbyteInputError(
            message="Client ID and client secret are both required.",
            guidance="Provide both `--client-id` and `--client-secret`.",
        )

    return client_id, client_secret


def _resolve_login_roots(
    *,
    airbyte_api_root: str | None,
    config_api_root: str | None,
) -> tuple[str, str]:
    """Resolve Cloud or self-managed API roots for login."""
    if airbyte_api_root is not None or config_api_root is not None:
        if airbyte_api_root is not None and config_api_root is not None:
            return airbyte_api_root, config_api_root

        missing_roots: list[str] = []
        if not airbyte_api_root:
            missing_roots.append("airbyte_api_root")
        if not config_api_root:
            missing_roots.append("config_api_root")
        raise PyAirbyteInputError(
            message="Self-managed login requires both API roots.",
            context={"missing": ", ".join(missing_roots)},
            guidance="Provide both `--public-api-root` and `--config-api-root`.",
        )

    return CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT


def _base64_urlsafe_no_padding(raw_value: bytes) -> str:
    """Return base64 URL-safe text without padding."""
    return base64.urlsafe_b64encode(raw_value).rstrip(b"=").decode("ascii")


def _generate_code_verifier() -> str:
    """Generate a PKCE verifier."""
    return _base64_urlsafe_no_padding(secrets.token_bytes(32))


def _code_challenge(verifier: str) -> str:
    """Return the S256 PKCE challenge for a verifier."""
    return _base64_urlsafe_no_padding(hashlib.sha256(verifier.encode()).digest())


def _generate_state() -> str:
    """Generate an OAuth state value."""
    return _base64_urlsafe_no_padding(secrets.token_bytes(16))


def _first_query_value(query: Mapping[str, Sequence[str]], key: str) -> str | None:
    """Return the first query value for a key."""
    values = query.get(key)
    if not values:
        return None
    return values[0]


def _start_loopback_server() -> tuple[_OAuthCallbackHTTPServer, threading.Thread]:
    """Start the one-shot loopback callback server."""
    server = _OAuthCallbackHTTPServer(
        (KEYCLOAK_CALLBACK_HOST, 0),
        _OAuthCallbackHandler,
        bind_and_activate=False,
    )
    server.result_queue = Queue(maxsize=1)
    server.address_family = socket.AF_INET
    server.server_bind()
    server.server_activate()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _build_authorize_url(
    *,
    keycloak_base_url: str,
    redirect_uri: str,
    state: str,
    code_challenge: str,
) -> str:
    """Build the Keycloak authorization URL."""
    query = urlencode(
        {
            "response_type": "code",
            "client_id": KEYCLOAK_CLIENT_ID,
            "redirect_uri": redirect_uri,
            "scope": KEYCLOAK_SCOPES,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
    )
    return f"{keycloak_base_url.rstrip('/')}/protocol/openid-connect/auth?{query}"


def _json_object(response: requests.Response, *, operation: str) -> dict[str, object]:
    """Decode a response body as a JSON object."""
    try:
        parsed: object = response.json()
    except requests.JSONDecodeError as ex:
        raise AirbyteError(
            message=f"{operation} returned invalid JSON.",
            context={"status_code": response.status_code},
        ) from ex

    if not isinstance(parsed, dict):
        raise AirbyteError(
            message=f"{operation} returned an unexpected response shape.",
            context={"status_code": response.status_code},
        )

    return {str(key): value for key, value in parsed.items() if isinstance(key, str)}


def _raise_for_response_status(response: requests.Response, *, operation: str) -> None:
    """Raise a PyAirbyte API error for an unsuccessful response."""
    if status_ok(response.status_code):
        return

    context: dict[str, object] = {"status_code": response.status_code}
    if response.request is not None:
        context["url"] = response.request.url
    raise AirbyteError(message=f"{operation} failed.", context=context)


def _required_string(data: Mapping[str, object], key: str, *, operation: str) -> str:
    """Return a required string field from JSON data."""
    value = data.get(key)
    if isinstance(value, str) and value:
        return value
    raise AirbyteError(
        message=f"{operation} did not return `{key}`.",
    )


def _exchange_code_for_keycloak_token(
    *,
    session: requests.Session,
    keycloak_base_url: str,
    code: str,
    code_verifier: str,
    redirect_uri: str,
    timeout_seconds: int,
) -> str:
    """Exchange an authorization code for a transient Keycloak access token."""
    token_url = f"{keycloak_base_url.rstrip('/')}/protocol/openid-connect/token"
    response = session.post(
        token_url,
        headers={"Accept": "application/json"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": KEYCLOAK_CLIENT_ID,
            "code_verifier": code_verifier,
        },
        timeout=timeout_seconds,
    )
    parsed = _json_object(response, operation="Keycloak token exchange")
    if response.status_code != HTTPStatus.OK.value or parsed.get("error"):
        error = parsed.get("error")
        error_description = parsed.get("error_description")
        context: dict[str, object] = {"status_code": response.status_code}
        if isinstance(error, str):
            context["error"] = error
        if isinstance(error_description, str):
            context["error_description"] = error_description
        raise AirbyteError(message="Keycloak token exchange failed.", context=context)

    return _required_string(parsed, "access_token", operation="Keycloak token exchange")


def _bootstrap_request(
    *,
    session: requests.Session,
    method: str,
    url: str,
    access_token: str,
    operation: str,
    organization_id: str | None = None,
    timeout_seconds: int,
) -> dict[str, object]:
    """Call an Airbyte Cloud bootstrap endpoint."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "PyAirbyte Client",
        "X-ADP-Agent-CLI": "pyairbyte",
    }
    if organization_id:
        headers["X-Organization-Id"] = organization_id

    response = session.request(method, url, headers=headers, timeout=timeout_seconds)
    _raise_for_response_status(response, operation=operation)
    return _json_object(response, operation=operation)


def _api_url(airbyte_api_root: str, path: str) -> str:
    """Build an Airbyte API URL."""
    return f"{airbyte_api_root.rstrip('/')}{path}"


def _parse_organizations(data: Mapping[str, object]) -> list[_OrganizationItem]:
    """Parse bootstrap organization list response."""
    organizations = data.get("organizations")
    if not isinstance(organizations, list):
        raise AirbyteError(message="Organization list response did not include organizations.")

    result: list[_OrganizationItem] = []
    for organization in organizations:
        if not isinstance(organization, Mapping):
            raise AirbyteError(message="Organization list response included an invalid item.")
        organization_id = organization.get("id")
        organization_name = organization.get("organization_name")
        if not isinstance(organization_id, str) or not organization_id:
            raise AirbyteError(message="Organization list response included an item without `id`.")
        result.append(
            _OrganizationItem(
                id=organization_id,
                name=organization_name if isinstance(organization_name, str) else "",
            )
        )
    return result


def _organization_hint(organizations: Sequence[_OrganizationItem]) -> str:
    """Build a hint listing available organization choices."""
    lines = ["Re-run with `--organization-id <uuid>`. Available organizations:"]
    for organization in organizations:
        name = f"{organization.name} " if organization.name else ""
        lines.append(f"- {name}({organization.id})")
    return "\n".join(lines)


def _choose_organization(
    *,
    organizations: Sequence[_OrganizationItem],
    input_stream: TextIO,
    error_stream: TextIO,
    stdin_is_tty: bool,
) -> str:
    """Choose an organization from the bootstrap API response."""
    if not organizations:
        raise PyAirbyteInputError(
            message="No organizations found for this Airbyte Cloud account.",
            guidance="Complete onboarding at https://app.airbyte.ai and retry login.",
        )
    if len(organizations) == 1:
        return organizations[0].id
    if not stdin_is_tty:
        raise PyAirbyteInputError(
            message="This account belongs to multiple organizations.",
            guidance=_organization_hint(organizations),
        )

    print("You belong to multiple organizations. Choose one:", file=error_stream)
    for index, organization in enumerate(organizations, start=1):
        name = organization.name or organization.id
        print(f"  {index}) {name} ({organization.id})", file=error_stream)
    print(f"Enter choice [1-{len(organizations)}]: ", end="", file=error_stream)
    raw_choice = input_stream.readline().strip()
    try:
        choice = int(raw_choice)
    except ValueError as ex:
        raise PyAirbyteInputError(message="Organization choice must be a number.") from ex
    if choice < 1 or choice > len(organizations):
        raise PyAirbyteInputError(message="Organization choice is out of range.")
    return organizations[choice - 1].id


def _bootstrap_airbyte_credentials(
    *,
    session: requests.Session,
    access_token: str,
    organization_id: str | None,
    airbyte_api_root: str,
    timeout_seconds: int,
    input_stream: TextIO,
    error_stream: TextIO,
    stdin_is_tty: bool,
) -> tuple[str, str, str]:
    """Bootstrap Airbyte client credentials from a Keycloak access token."""
    enrollment = _bootstrap_request(
        session=session,
        method="GET",
        url=_api_url(airbyte_api_root, "/internal/account/enrollment-status"),
        access_token=access_token,
        operation="Airbyte Cloud enrollment-status request",
        timeout_seconds=timeout_seconds,
    )
    if enrollment.get("is_enrolled") is False:
        raise PyAirbyteInputError(
            message="This Airbyte Cloud account is not enrolled.",
            guidance="Complete onboarding at https://app.airbyte.ai and retry login.",
        )
    initial_organization_id = _required_string(
        enrollment,
        "organization_id",
        operation="Airbyte Cloud enrollment-status request",
    )

    chosen_organization_id = organization_id
    if not chosen_organization_id:
        organizations_response = _bootstrap_request(
            session=session,
            method="GET",
            url=_api_url(airbyte_api_root, "/internal/account/organizations"),
            access_token=access_token,
            organization_id=initial_organization_id,
            operation="Airbyte Cloud organizations request",
            timeout_seconds=timeout_seconds,
        )
        chosen_organization_id = _choose_organization(
            organizations=_parse_organizations(organizations_response),
            input_stream=input_stream,
            error_stream=error_stream,
            stdin_is_tty=stdin_is_tty,
        )

    application = _bootstrap_request(
        session=session,
        method="POST",
        url=_api_url(airbyte_api_root, "/internal/account/applications"),
        access_token=access_token,
        organization_id=chosen_organization_id,
        operation="Airbyte Cloud applications request",
        timeout_seconds=timeout_seconds,
    )
    client_id = _required_string(
        application,
        "client_id",
        operation="Airbyte Cloud applications request",
    )
    client_secret = _required_string(
        application,
        "client_secret",
        operation="Airbyte Cloud applications request",
    )
    return client_id, client_secret, chosen_organization_id


def login_with_browser(  # noqa: PLR0913, PLR0914
    *,
    organization_id: str | None = None,
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
    keycloak_base_url: str = KEYCLOAK_BASE_URL,
    airbyte_api_root: str = AIRBYTE_AI_API_ROOT,
    config_api_root: str = CLOUD_CONFIG_API_ROOT,
    timeout_seconds: int = DEFAULT_BROWSER_LOGIN_TIMEOUT_SECONDS,
    open_url: Callable[[str], object] | None = None,
    input_stream: TextIO | None = None,
    error_stream: TextIO | None = None,
    stdin_is_tty: bool | None = None,
    session: requests.Session | None = None,
) -> CloudLoginResult:
    """Log in with browser-based PKCE and persist Airbyte credentials."""
    input_stream = input_stream or sys.stdin
    error_stream = error_stream or sys.stderr
    stdin_is_tty = input_stream.isatty() if stdin_is_tty is None else stdin_is_tty
    session = session or requests.Session()
    open_url = open_url or webbrowser.open

    code_verifier = _generate_code_verifier()
    state = _generate_state()
    server, thread = _start_loopback_server()
    try:
        port = server.server_address[1]
        if not isinstance(port, int):
            raise PyAirbyteInputError(message="Loopback callback server port is invalid.")
        redirect_uri = f"http://{KEYCLOAK_CALLBACK_HOST}:{port}{CALLBACK_PATH}"
        authorize_url = _build_authorize_url(
            keycloak_base_url=keycloak_base_url,
            redirect_uri=redirect_uri,
            state=state,
            code_challenge=_code_challenge(code_verifier),
        )
        try:
            open_result = open_url(authorize_url)
        except Exception as ex:
            print(
                f"Could not open a browser automatically ({ex}).\n"
                f"Paste this URL into a browser:\n  {authorize_url}",
                file=error_stream,
            )
        else:
            if open_result is False:
                print(
                    f"Paste this URL into a browser:\n  {authorize_url}",
                    file=error_stream,
                )

        try:
            callback = server.result_queue.get(timeout=timeout_seconds)
        except Empty as ex:
            raise PyAirbyteInputError(
                message="Timed out waiting for browser login callback.",
                guidance="Retry `airbyte-cloud login` and complete login in the browser.",
            ) from ex
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    if callback.error:
        raise PyAirbyteInputError(message="Interactive Airbyte Cloud login failed.")
    if not callback.code or not callback.state:
        raise PyAirbyteInputError(message="Interactive Airbyte Cloud login did not return a code.")
    if not hmac.compare_digest(state, callback.state):
        raise PyAirbyteInputError(message="OAuth state mismatch.")

    access_token = _exchange_code_for_keycloak_token(
        session=session,
        keycloak_base_url=keycloak_base_url,
        code=callback.code,
        code_verifier=code_verifier,
        redirect_uri=redirect_uri,
        timeout_seconds=timeout_seconds,
    )
    client_id, client_secret, selected_organization_id = _bootstrap_airbyte_credentials(
        session=session,
        access_token=access_token,
        organization_id=organization_id,
        airbyte_api_root=airbyte_api_root,
        timeout_seconds=timeout_seconds,
        input_stream=input_stream,
        error_stream=error_stream,
        stdin_is_tty=stdin_is_tty,
    )

    credentials = read_credentials_file(credentials_file_path)
    credentials.pop("bearer_token", None)
    credentials.update(
        {
            "airbyte_api_root": airbyte_api_root,
            "client_id": client_id,
            "client_secret": client_secret,
            "config_api_root": config_api_root,
            "organization_id": selected_organization_id,
        }
    )
    write_credentials_file(credentials, credentials_file_path)

    return CloudLoginResult(
        credentials_file_path=credentials_file_path,
        airbyte_api_root=airbyte_api_root,
        config_api_root=config_api_root,
        organization_id=selected_organization_id,
    )


def login_with_client_credentials(
    *,
    client_id: str | None = None,
    client_secret: str | None = None,
    airbyte_api_root: str | None = None,
    config_api_root: str | None = None,
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> CloudLoginResult:
    """Log in using client credentials and persist a bearer token."""
    resolved_client_id, resolved_client_secret = _validate_client_credentials(
        client_id=client_id,
        client_secret=client_secret,
    )
    resolved_airbyte_api_root, resolved_config_api_root = _resolve_login_roots(
        airbyte_api_root=airbyte_api_root,
        config_api_root=config_api_root,
    )
    bearer_token = get_bearer_token(
        client_id=SecretString(resolved_client_id),
        client_secret=SecretString(resolved_client_secret),
        api_root=resolved_airbyte_api_root,
    )

    credentials = read_credentials_file(credentials_file_path)
    credentials.pop("client_id", None)
    credentials.pop("client_secret", None)
    credentials.update(
        {
            "airbyte_api_root": resolved_airbyte_api_root,
            "bearer_token": str(bearer_token),
            "config_api_root": resolved_config_api_root,
        }
    )
    write_credentials_file(credentials, credentials_file_path)

    return CloudLoginResult(
        credentials_file_path=credentials_file_path,
        airbyte_api_root=resolved_airbyte_api_root,
        config_api_root=resolved_config_api_root,
    )


def logout(
    *,
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> None:
    """Remove locally stored Airbyte credentials."""
    if credentials_file_path.exists():
        credentials_file_path.unlink()
