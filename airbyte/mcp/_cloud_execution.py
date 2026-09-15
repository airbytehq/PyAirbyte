# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Request-scoped, bounded transport for Cloud execution and skill documentation."""

from __future__ import annotations

import json
from typing import Any, Literal
from urllib.parse import urlencode
from uuid import UUID

import requests
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airbyte._util.api_util import get_config_api_root
from airbyte.exceptions import AirbyteError
from airbyte.secrets import SecretString


_TIMEOUT = (5, 45)
_RESPONSE_LIMIT = 1024 * 1024
_REQUEST_LIMIT = 65536
_TOKEN_LIMIT = 65536
_QUERY_LIMIT = 2048


class CloudExecutionError(AirbyteError):
    """A Cloud failure containing only safe operation and status information."""

    def __init__(self, operation: str, detail: str, status_code: int | None = None) -> None:
        """Exclude remote response bodies and request details from errors."""
        self.status_code = status_code
        self.operation = operation
        super().__init__(
            message=f"Cloud {operation}: {detail}",
            context={"operation": operation, "status_code": status_code},
        )


class _DocsModel(BaseModel):
    model_config = ConfigDict(strict=True)


class _Freshness(_DocsModel):
    state: Literal["static", "live", "degraded", "unknown"] = "live"
    checked_at: str | None = None


class _Metadata(_DocsModel):
    id: str
    kind: Literal["connector_source"]
    title: str
    provenance: str
    version: str
    freshness: _Freshness
    summary: str | None = None
    content_revision: str | None = None
    tags: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class _Outline(_DocsModel):
    id: str
    title: str
    summary: str | None = None
    available: bool = True


class _Content(_DocsModel):
    type: Literal["heading", "paragraph", "list", "code", "table"]
    text: str | None = None
    level: int | None = None
    items: list[str] | None = None
    language: str | None = None
    code: str | None = None
    headers: list[str] | None = None
    rows: list[list[str]] | None = None


class _Docs(_DocsModel):
    metadata: _Metadata
    outline: list[_Outline]
    section_id: str | None = None
    content: list[_Content] = Field(default_factory=list)


class CloudExecutionClient:
    """Use one credential set for discovery and execution within an MCP request."""

    def __init__(
        self,
        *,
        api_root: str,
        config_api_root: str | None = None,
        bearer_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        """Resolve roots without exchanging credentials or making HTTP requests."""
        self.api_root = api_root.rstrip("/")
        self.config_api_root = get_config_api_root(api_root, config_api_root=config_api_root)
        self._bearer_token = (
            SecretString(bearer_token)
            if bearer_token is not None and bearer_token.strip()
            else None
        )
        self._client_id = client_id
        self._client_secret = client_secret

    @property
    def bearer_token(self) -> SecretString:
        """Resolve once, giving an explicit bearer precedence over application credentials."""
        if self._bearer_token is None:
            if (
                not self._client_id
                or not self._client_id.strip()
                or not self._client_secret
                or not self._client_secret.strip()
            ):
                raise CloudExecutionError("authentication", "Cloud credentials are required.")
            payload = self._request(
                "token exchange",
                "POST",
                self.api_root + "/applications/token",
                body={"client_id": self._client_id, "client_secret": self._client_secret},
                response_limit=_TOKEN_LIMIT,
            )
            token = payload.get("access_token")
            if not isinstance(token, str) or not token.strip():
                raise CloudExecutionError("token exchange", "Invalid token response.")
            self._bearer_token = SecretString(token)
        return self._bearer_token

    def execute_source(self, source_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Dispatch a source read once and preserve its native data and metadata."""
        return self._execute("sources", source_id, body)

    def execute_destination(self, destination_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Dispatch a destination read once and preserve positional rows."""
        return self._execute("destinations", destination_id, body)

    def _execute(self, actor_kind: str, actor_id: str, body: dict[str, Any]) -> dict[str, Any]:
        actor_id = self._uuid(actor_id)
        payload = self._request(
            "execution",
            "POST",
            f"{self.config_api_root}/{actor_kind}/{actor_id}/execute",
            body=body,
            token=self.bearer_token,
        )
        if "data" not in payload or ("meta" in payload and not isinstance(payload["meta"], dict)):
            raise CloudExecutionError("execution", "Invalid execution response.")
        return payload

    def read_docs(
        self, workspace_id: str, skill_id: str, section: str | None = None
    ) -> dict[str, Any]:
        """Fetch the exact docs ID and section using encoded query parameters."""
        workspace_id = self._uuid(workspace_id)
        params = {"id": skill_id}
        if section is not None:
            params["section"] = section
        if (
            any(not value for value in params.values())
            or len(urlencode(params).encode()) > _QUERY_LIMIT
        ):
            raise CloudExecutionError("documentation", "Invalid documentation query.")
        payload = self._request(
            "documentation",
            "GET",
            f"{self.config_api_root}/workspaces/{workspace_id}/skills/docs",
            params=params,
            token=self.bearer_token,
        )
        try:
            _Docs.model_validate(payload)
        except ValidationError:
            raise CloudExecutionError("documentation", "Invalid documentation response.") from None
        payload.setdefault("content", [])
        return payload

    @staticmethod
    def _uuid(value: str) -> str:
        try:
            return str(UUID(value))
        except (ValueError, AttributeError, TypeError):
            raise CloudExecutionError(
                "request", "A valid Cloud resource UUID is required."
            ) from None

    @staticmethod
    def _request(
        operation: str,
        method: str,
        url: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        token: SecretString | None = None,
        response_limit: int = _RESPONSE_LIMIT,
    ) -> dict[str, Any]:
        headers = {"accept": "application/json"}
        encoded = None
        if body is not None:
            try:
                encoded = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
            except (TypeError, ValueError, UnicodeError):
                raise CloudExecutionError(operation, "Invalid JSON request.") from None
            if len(encoded) > _REQUEST_LIMIT:
                raise CloudExecutionError(operation, "Request exceeds the Cloud byte limit.")
            headers["content-type"] = "application/json"
        if token is not None:
            headers["authorization"] = f"Bearer {token}"
        try:
            # Explicit auth blocks netrc substitution while retaining proxy and CA settings.
            with requests.Session() as session:
                session.auth = lambda request: request
                with session.request(
                    method,
                    url,
                    headers=headers,
                    data=encoded,
                    params=params,
                    timeout=_TIMEOUT,
                    allow_redirects=False,
                    stream=True,
                ) as response:
                    if response.status_code != 200:  # noqa: PLR2004
                        raise CloudExecutionError(
                            operation, "Request failed.", status_code=response.status_code
                        )
                    chunks = bytearray()
                    for chunk in response.iter_content(chunk_size=8192):
                        chunks.extend(chunk)
                        if len(chunks) > response_limit:
                            raise CloudExecutionError(
                                operation, "Response exceeds the Cloud byte limit."
                            )
        except requests.RequestException:
            raise CloudExecutionError(
                operation,
                "Transport interrupted; remote outcome is unknown. No automatic retry was made.",
            ) from None
        try:
            payload = json.loads(chunks.decode("utf-8"), parse_constant=_reject_json_constant)
        except (ValueError, UnicodeError, RecursionError):
            raise CloudExecutionError(operation, "Invalid JSON response.") from None
        if not isinstance(payload, dict):
            raise CloudExecutionError(operation, "Expected a JSON object response.")
        return payload


def _reject_json_constant(value: str) -> None:
    raise ValueError(value)
