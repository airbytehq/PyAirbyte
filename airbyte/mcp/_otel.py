# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Hosted OpenTelemetry tracing registration and request-span filtering."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import os
import re
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit, urlunsplit

from fastmcp_extensions.otel._arg_digests import is_arg_key  # noqa: PLC2701
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import Event, ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import SpanKind, Status

from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT
from airbyte.mcp._error_handling import classify_mcp_tool_error, mcp_tool_error_reason


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from fastmcp import FastMCP
    from opentelemetry.util.types import AttributeValue
    from starlette.types import ASGIApp, Receive, Scope, Send


logger = logging.getLogger(__name__)
MCP_SESSION_ID_HEADER = "mcp-session-id"
_PROVIDER_OWNERSHIP_ERROR = (
    "Hosted MCP tracing requires exclusive ownership of the global tracer provider "
    "and requests instrumentation."
)
_UUID_PATTERN = r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
_SAFE_HTTP_URL = re.compile(
    rf"{re.escape(CLOUD_API_ROOT)}/(?:"
    rf"applications/token|organizations|jobs(?:/[0-9]{{1,19}})?|"
    rf"(?:connections|sources|destinations)(?:/{_UUID_PATTERN})?|"
    rf"workspaces(?:/{_UUID_PATTERN}(?:/definitions/declarative_sources"
    rf"(?:/{_UUID_PATTERN})?)?)?)|"
    rf"{re.escape(CLOUD_CONFIG_API_ROOT)}/(?:"
    r"(?:sources|destinations)/check_connection|"
    r"connector_builder_projects/(?:list|get_for_definition_id|get_with_manifest|update|update_testing_values)|"
    r"organizations/(?:list_by_user_id|get_organization_info)|"
    r"workspaces/(?:list_by_organization_id|list_by_user_id|get_organization_info|get)|"
    r"state/(?:get|create_or_update_safe)|web_backend/connections/(?:get|update)|"
    r"users/(?:get_by_auth_id|update)|permissions/list_by_user|jobs/get|"
    rf"(?:sources|destinations)/{_UUID_PATTERN}/(?:execute|search|search-status|enablement)|"
    rf"workspaces/{_UUID_PATTERN}/skills/docs)"
)
REDACTED_PLACEHOLDER = "[redacted by airbyte-mcp]"
_ARG_KEY_ENV = "AIRBYTE_MCP_TELEMETRY_HMAC_KEY"
_ARG_KEY_PATTERN = re.compile(r"[A-Za-z0-9_-]+")
_ARG_KEY_LENGTH = 32
_INSTALLED = False
_ARG_KEY_WARNING_EMITTED = False


def _env(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _flag(environ: Mapping[str, str] | None, name: str) -> bool:
    return _env(environ).get(name, "").strip().lower() in {"1", "true"}


def _tracing_backend(environ: Mapping[str, str] | None) -> str:
    environment = _env(environ)
    backend = environment.get("AIRBYTE_MCP_TRACING_BACKEND")
    if backend is None:
        vendor = environment.get("AIRBYTE_MCP_OTEL_VENDOR")
        if vendor is not None:
            return "datadog-otlp" if vendor.strip().lower() == "datadog" else "otel"
        return "datadog" if _flag(environ, "DD_LLMOBS_ENABLED") else "otel"
    backend = backend.strip().lower()
    if backend not in {"otel", "datadog-otlp", "datadog"}:
        raise ValueError("AIRBYTE_MCP_TRACING_BACKEND must be 'otel', 'datadog-otlp', or 'datadog'")
    return backend


def _arg_key() -> bytes | None:
    """Return the configured 32-byte unpadded base64url argument-hash key."""
    encoded = os.environ.get(_ARG_KEY_ENV)
    if encoded is None:
        return None
    if not _ARG_KEY_PATTERN.fullmatch(encoded):
        _warn_invalid_arg_key()
        return None
    try:
        value = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    except (binascii.Error, ValueError):
        _warn_invalid_arg_key()
        return None
    if len(value) != _ARG_KEY_LENGTH:
        _warn_invalid_arg_key()
        return None
    return value


def _warn_invalid_arg_key() -> None:
    global _ARG_KEY_WARNING_EMITTED
    if not _ARG_KEY_WARNING_EMITTED:
        logger.warning("%s is invalid; argument hashing is disabled", _ARG_KEY_ENV)
        _ARG_KEY_WARNING_EMITTED = True


def _trace_attributes() -> dict[str, str]:
    from airbyte._util.meta import (  # noqa: PLC0415
        get_cloud_api_analytic_source,
        get_declared_application_name,
    )

    attributes = {"analytic_source": get_cloud_api_analytic_source()}
    if application_name := get_declared_application_name():
        attributes["application_name"] = application_name
    return attributes


def _http_client_span_attributes(span: ReadableSpan) -> Mapping[str, AttributeValue] | None:
    """Keep bounded requests spans and remove payload, identity and URL secrets."""
    scope = span.instrumentation_scope
    if scope is None or scope.name != "opentelemetry.instrumentation.requests":
        return None
    attributes: dict[str, AttributeValue] = {
        key: value
        for key, value in (span.attributes or {}).items()
        if not key.startswith(("enduser.", "http.request.header.", "http.response.header."))
        and key
        not in {
            "user_agent.original",
            "url.query",
            "http.user_agent",
            "http.host",
            "server.address",
            "network.peer.address",
            "gen_ai.tool.call.arguments",
            "gen_ai.tool.call.result",
            "_dd.ml_obs.metadata",
        }
    }
    for key in ("http.url", "url.full"):
        if key in attributes:
            try:
                url = urlsplit(str(attributes[key]))
                clean_url = urlunsplit(
                    (url.scheme, url.netloc.rsplit("@", 1)[-1], url.path, "", "")
                )
            except ValueError:
                clean_url = ""
            attributes[key] = (
                clean_url if _SAFE_HTTP_URL.fullmatch(clean_url) else REDACTED_PLACEHOLDER
            )
    return attributes


def _exporter(
    backend: str, environ: Mapping[str, str] | None
) -> Literal["otlp"] | _DatadogMetadataExporter:
    environment = _env(environ)
    if backend != "datadog-otlp" or not (
        environment.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        or environment.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    ):
        return "otlp"
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # noqa: PLC0415
        OTLPSpanExporter,
    )

    return _DatadogMetadataExporter(OTLPSpanExporter())


class _DatadogMetadataExporter(SpanExporter):
    def __init__(self, exporter: SpanExporter) -> None:
        self._exporter = exporter

    @staticmethod
    def _rebuild(span: ReadableSpan) -> ReadableSpan:
        attributes = dict(span.attributes or {})
        attributes.pop("_dd.ml_obs.metadata", None)
        attributes.pop("gen_ai.tool.call.arguments", None)
        attributes.pop("gen_ai.tool.call.result", None)
        metadata = {
            key: attributes[f"airbyte.mcp.{key}"]
            for key in (
                "intent",
                "intent_present",
                "tool_module",
                "workspace_id",
                "organization_id",
                "scope_source",
                "error_type",
                "error.category",
                "error.fault",
                "error.reason",
                "error.cause_types",
                "upstream.status_code",
                "outcome",
                "auth_method",
                "mcp_protocol_version",
                "session_id",
                "agent.action",
                "agent.entity_type",
                "client_name",
                "client_version",
            )
            if f"airbyte.mcp.{key}" in attributes
        }
        metadata.update(
            {
                key.removeprefix("airbyte.mcp."): value
                for key, value in attributes.items()
                if is_arg_key("airbyte.mcp", key)
            }
        )
        if metadata:
            attributes["_dd.ml_obs.metadata"] = json.dumps(metadata)
        tool_arguments = {
            label: metadata[key]
            for label, key in (
                ("intent", "intent"),
                ("action", "agent.action"),
                ("entity_name", "agent.entity_type"),
            )
            if isinstance(metadata.get(key), str) and metadata[key]
        }
        if span.kind == SpanKind.SERVER and span.name.startswith("tools/call ") and tool_arguments:
            attributes["gen_ai.tool.call.arguments"] = json.dumps(tool_arguments)
        events = [
            Event(
                "exception",
                {"exception.type": (event.attributes or {}).get("exception.type", "")},
                event.timestamp,
            )
            for event in span.events
            if event.name == "exception"
        ]
        return ReadableSpan(
            name=span.name,
            context=span.context,
            parent=span.parent,
            resource=span.resource,
            attributes=attributes,
            events=events,
            links=span.links,
            kind=span.kind,
            status=Status(span.status.status_code),
            start_time=span.start_time,
            end_time=span.end_time,
            instrumentation_scope=span.instrumentation_scope,
        )

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            rebuilt = []
            for span in spans:
                try:
                    rebuilt.append(self._rebuild(span))
                except Exception:
                    logger.debug("Dropping span that could not be mapped to Datadog")
            return self._exporter.export(rebuilt) if rebuilt else SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        try:
            self._exporter.shutdown()
        except Exception:
            logger.debug("OpenTelemetry exporter shutdown failed")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        try:
            return self._exporter.force_flush(timeout_millis)
        except Exception:
            return False


def install(app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
    """Install hosted OpenTelemetry tracing after checking provider ownership."""
    global _INSTALLED
    if _INSTALLED:
        return
    backend = _tracing_backend(environ)
    if backend == "datadog":
        from airbyte.mcp._datadog import install as install_datadog  # noqa: PLC0415

        install_datadog(app, environ=environ)
        _INSTALLED = True
        return
    if not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider):
        raise RuntimeError(_PROVIDER_OWNERSHIP_ERROR)  # noqa: TRY004
    if RequestsInstrumentor().is_instrumented_by_opentelemetry:  # type: ignore[missing-attribute]
        raise RuntimeError(_PROVIDER_OWNERSHIP_ERROR)

    from fastmcp_extensions import (  # noqa: PLC0415
        TelemetryConfig,
        ToolTracingConfig,
        register_tool_call_telemetry,
    )

    register_tool_call_telemetry(
        app,
        TelemetryConfig(
            package_name="airbyte",
            tool_tracing=ToolTracingConfig(
                attribute_prefix="airbyte.mcp",
                attributes=_trace_attributes,
                shared_properties=(
                    "workspace_id",
                    "organization_id",
                    "scope_source",
                    "auth_method",
                ),
                capture_intent=_flag(environ, "AIRBYTE_MCP_INTENT_CAPTURE"),
                error_classifier=classify_mcp_tool_error,
                error_reason=mcp_tool_error_reason,
                other_spans=_http_client_span_attributes,
                arg_key=_arg_key,
                exporter=_exporter(backend, environ),
            ),
        ),
    )
    _INSTALLED = True

    if isinstance(trace.get_tracer_provider(), TracerProvider):
        try:
            RequestsInstrumentor().instrument(excluded_urls="api.segment.io")  # type: ignore[missing-attribute]
        except Exception:
            logger.debug("Optional OpenTelemetry setup failed")


def _reset_for_tests() -> None:
    global _INSTALLED, _ARG_KEY_WARNING_EMITTED
    _INSTALLED = False
    _ARG_KEY_WARNING_EMITTED = False


class SessionIdHeaderDigest:
    """Hash the unsigned client grouping key before MCP instrumentation sees it."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            from fastmcp_extensions.capability_tokens import (  # noqa: PLC0415
                DEFAULT_EXTENSIONS_HEADER,
                decode_capability_token,
            )

            key = MCP_SESSION_ID_HEADER.encode()
            ext_key = DEFAULT_EXTENSIONS_HEADER.lower().encode()
            headers: list[tuple[bytes, bytes]] = scope.get("headers") or []
            raw = next((value for name, value in headers if name.lower() == key), None)
            if raw is not None:
                from airbyte.mcp._telemetry import _SESSION_ID_STATE_KEY  # noqa: PLC0415

                scope.setdefault("state", {})[_SESSION_ID_STATE_KEY] = hashlib.sha256(
                    raw
                ).hexdigest()
                try:
                    extensions = decode_capability_token(raw.decode("latin-1"))
                except Exception:
                    logger.debug("Session extensions could not be decoded", exc_info=True)
                    extensions = set()
                new_headers = [
                    (
                        name,
                        hashlib.sha256(value).hexdigest().encode()
                        if name.lower() == key
                        else value,
                    )
                    for name, value in headers
                ]
                if extensions:
                    existing = b" ".join(
                        value for name, value in new_headers if name.lower() == ext_key
                    )
                    merged = (existing + b" " if existing else b"") + " ".join(
                        sorted(extensions)
                    ).encode()
                    new_headers = [
                        (name, value) for name, value in new_headers if name.lower() != ext_key
                    ]
                    new_headers.append((ext_key, merged))
                scope["headers"] = new_headers
        await self._app(scope, receive, send)
