# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Hosted MCP intent capture and fail-closed OpenTelemetry span export."""

# Hosted startup is the only caller; defer exporter and registration imports until needed.
# ruff: noqa: PLC0415

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import re
import sys
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from fastmcp.server.middleware import Middleware
from fastmcp_extensions import get_mcp_config
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import SpanKind, Status

from airbyte._direct_connectors.api_util import _AGENTS_API_ROOT
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_CONFIG_API_ROOT,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_WORKSPACE_ID,
)
from airbyte.version import get_version


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from fastmcp import FastMCP
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import Tool, ToolResult
    from mcp.types import CallToolRequestParams, ListToolsRequest
    from opentelemetry.context import Context
    from opentelemetry.sdk.trace import Span
    from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)
INTENT_ARG = "intent"
_LEGACY_TELEMETRY_ARG = "telemetry"
MCP_SESSION_ID_HEADER = "mcp-session-id"
INTENT_INSTRUCTIONS_SENTENCE = (
    " Tools may accept an optional `intent` string; if present, "
    "state in one sentence why you are calling the tool "
    "(never credentials, identifiers or data values)."
)
_PROVIDER_OWNERSHIP_ERROR = (
    "Hosted MCP tracing requires exclusive ownership of the global tracer provider "
    "and requests instrumentation."
)
_INTENT_SCHEMA = {
    "type": "string",
    "description": (
        "Briefly describe the wider task and why you chose this tool, in English. "
        "Omit argument values, personal information, and secrets."
    ),
}
_UUID_PATTERN = r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
_UUID_RE = re.compile(rf"\A{_UUID_PATTERN}\Z")
# Only literal routes and validated IDs may survive export. Keep these aligned with
# _util/api_util.py, _direct_connectors/api_util.py and their Public API SDK calls. Unknown
# routes (including registry and custom API roots) retain status, but redact the URL.
_SAFE_HTTP_URL = re.compile(
    rf"{re.escape(CLOUD_API_ROOT)}/(?:"
    rf"applications/token|organizations|jobs(?:/[0-9]{{1,19}})?|"
    rf"(?:connections|sources|destinations)(?:/{_UUID_PATTERN})?|"
    rf"workspaces(?:/{_UUID_PATTERN}(?:/definitions/declarative_sources"
    rf"(?:/{_UUID_PATTERN})?)?)?)|"
    rf"{re.escape(CLOUD_CONFIG_API_ROOT)}/(?:"
    r"(?:sources|destinations)/check_connection|"
    r"connector_builder_projects/(?:get_for_definition_id|get_with_manifest|update_testing_values)|"
    r"organizations/(?:list_by_user_id|get_organization_info)|"
    r"workspaces/(?:list_by_organization_id|get_organization_info|get)|"
    r"state/(?:get|create_or_update_safe)|web_backend/connections/(?:get|update)|"
    r"users/(?:get_by_auth_id|update)|permissions/list_by_user|jobs/get)|"
    rf"{re.escape(_AGENTS_API_ROOT)}/(?:workspaces(?:/{_UUID_PATTERN})?|"
    rf"integrations/connectors(?:/{_UUID_PATTERN}/(?:inspect|execute))?|skills(?:/docs)?)"
)
REDACTED_PLACEHOLDER = "[redacted by airbyte-mcp]"
_MAX_INTENT_LENGTH = 4096
_MAX_LATE_ATTRIBUTES = 4096
_INSTALLED = False
_ENVIRON: Mapping[str, str] | None = None
_TOOL_MODULES: dict[str, str] = {}
_TOOL_ANNOTATIONS: dict[str, dict[str, Any]] = {}
# Middleware runs outside FastMCP's span; a ContextVar survives trace-context extraction.
_INTENT_ATTRIBUTES: ContextVar[dict[str, str | bool] | None] = ContextVar(
    "mcp_intent", default=None
)
_LATE_ATTRIBUTES: dict[int, dict[str, str]] = {}


def _env(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _flag(environ: Mapping[str, str] | None, name: str) -> bool:
    return _env(environ).get(name, "").strip().lower() in {"1", "true"}


def install(app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
    """Install once in hosted mode; export only with an explicitly configured endpoint.

    `environ` is a test seam for enablement and the `AIRBYTE_MCP_*` controls only.
    The OTel SDK always reads exporter and resource configuration from process
    environment; production callers should omit `environ`. An existing global
    provider or requests instrumentation prevents safe redaction and therefore
    refuses hosted startup, including when this integration's endpoint is unset.
    """
    global _INSTALLED, _ENVIRON
    if _INSTALLED:
        return
    if not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider):
        raise RuntimeError(_PROVIDER_OWNERSHIP_ERROR)  # noqa: TRY004  # Conflicting process state, not an invalid argument type.
    if RequestsInstrumentor().is_instrumented_by_opentelemetry:  # type: ignore[missing-attribute]  # Instrumentor singleton is non-null.
        raise RuntimeError(_PROVIDER_OWNERSHIP_ERROR)
    environment = _env(environ)
    provider = None
    if environment.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or environment.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT"
    ):
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            _build_tool_maps()
            provider = _build_provider(OTLPSpanExporter())
            trace.set_tracer_provider(provider)
        except Exception:
            if provider is not None:
                provider.shutdown()
            provider = None
            logger.error("OpenTelemetry controls could not be installed; export disabled")  # noqa: TRY400  # Never log exporter credentials.
        else:
            # The SDK silently ignores a second setter call; never instrument an unsafe provider.
            if trace.get_tracer_provider() is not provider:
                provider.shutdown()
                raise RuntimeError(_PROVIDER_OWNERSHIP_ERROR)
    # Set the guard only once ownership is established, so a refused startup stays refused.
    _INSTALLED, _ENVIRON = True, environ
    app.add_middleware(IntentCaptureMiddleware(app, environ=environ))
    if _flag(
        environ, "AIRBYTE_MCP_INTENT_CAPTURE"
    ) and INTENT_INSTRUCTIONS_SENTENCE.strip() not in (app.instructions or ""):
        app.instructions = (app.instructions or "") + INTENT_INSTRUCTIONS_SENTENCE
    if provider is None:
        return
    try:
        RequestsInstrumentor().instrument(excluded_urls="api.segment.io")  # type: ignore[missing-attribute]  # Instrumentor singleton is non-null.
    except Exception:
        logger.debug("Optional OpenTelemetry setup failed")


def _build_provider(exporter: SpanExporter) -> TracerProvider:
    resource = Resource({"service.version": get_version()}).merge(Resource.create())
    provider = TracerProvider(resource=resource)
    try:
        provider.add_span_processor(IntentStampProcessor())
        provider.add_span_processor(BatchSpanProcessor(RedactingExporter(exporter)))
    except Exception:
        provider.shutdown()
        raise
    return provider


def _reset_for_tests() -> None:
    global _INSTALLED, _ENVIRON
    _INSTALLED, _ENVIRON = False, None
    _LATE_ATTRIBUTES.clear()
    _TOOL_MODULES.clear()
    _TOOL_ANNOTATIONS.clear()


def _build_tool_maps() -> None:
    from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701
    from fastmcp_extensions.tool_filters import ANNOTATION_MCP_MODULE

    for func, tool_annotations in _REGISTERED_TOOLS:
        name = getattr(func, "__name__", None)
        if name:
            _TOOL_MODULES[name] = str(tool_annotations.get(ANNOTATION_MCP_MODULE, ""))
            _TOOL_ANNOTATIONS[name] = dict(tool_annotations)


class IntentCaptureMiddleware(Middleware):
    """Advertise optional intent and carry it into FastMCP's existing tool span."""

    def __init__(self, app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
        """Retain the app to distinguish synthetic intent from real parameters."""
        self._app, self._environ = app, environ

    async def on_list_tools(
        self,
        context: MiddlewareContext[ListToolsRequest],
        call_next: CallNext[ListToolsRequest, Sequence[Tool]],
    ) -> Sequence[Tool]:
        """Return copied schemas, leaving required fields and declared intent intact."""
        tools = await call_next(context)
        if not _flag(self._environ, "AIRBYTE_MCP_INTENT_CAPTURE"):
            return tools
        try:
            result = []
            for tool in tools:
                parameters = copy.deepcopy(tool.parameters)
                parameters.setdefault("properties", {}).setdefault(
                    INTENT_ARG, copy.deepcopy(_INTENT_SCHEMA)
                )
                result.append(tool.model_copy(update={"parameters": parameters}))
        except Exception:
            logger.debug("Intent schema advertisement skipped")
            return tools
        else:
            return result

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Strip synthetic arguments and untrusted tracing context before dispatch."""
        attrs: dict[str, str | bool] = {}
        intent = None
        try:
            if context.fastmcp_context and context.fastmcp_context.request_context:
                meta = context.fastmcp_context.request_context.meta
                if meta is not None and meta.model_extra:
                    meta.model_extra.pop("traceparent", None)
                    meta.model_extra.pop("tracestate", None)
            args = dict(context.message.arguments or {})
            intent = args.get(INTENT_ARG)
            if INTENT_ARG in args or _LEGACY_TELEMETRY_ARG in args:
                tool = await self._app.get_tool(context.message.name)
                properties = tool.parameters.get("properties", {}) if tool is not None else {}
                if _LEGACY_TELEMETRY_ARG not in properties:
                    telemetry = args.pop(_LEGACY_TELEMETRY_ARG, None)
                    if INTENT_ARG not in args and isinstance(telemetry, dict):
                        intent = telemetry.get(INTENT_ARG)
                if INTENT_ARG not in properties:
                    args.pop(INTENT_ARG, None)
                context = context.copy(
                    message=context.message.model_copy(update={"arguments": args})
                )
            attrs = self._attributes(context, intent)
        except Exception:
            logger.debug("Intent attributes unavailable")
        token = _INTENT_ATTRIBUTES.set(attrs)
        try:
            return await call_next(context)
        finally:
            _INTENT_ATTRIBUTES.reset(token)

    @staticmethod
    def _attributes(
        context: MiddlewareContext[CallToolRequestParams],
        intent: object,
    ) -> dict[str, str | bool]:
        from fastmcp.server.dependencies import get_http_headers

        from airbyte._util.meta import get_cloud_api_analytic_source

        name = context.message.name
        intent = intent.strip() if isinstance(intent, str) else ""
        if len(intent) > _MAX_INTENT_LENGTH:
            marker = "...[truncated]"
            intent = intent[: _MAX_INTENT_LENGTH - len(marker)] + marker
        attrs: dict[str, str | bool] = {
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": name,
            "airbyte.mcp.intent_present": bool(intent),
            "airbyte.mcp.analytic_source": get_cloud_api_analytic_source(),
        }
        if intent:
            attrs["airbyte.mcp.intent"] = intent
        if name in _TOOL_MODULES:
            hints = _TOOL_ANNOTATIONS.get(name, {})
            attrs.update(
                {
                    "airbyte.mcp.tool_module": _TOOL_MODULES[name],
                    "airbyte.mcp.tool_mutating": not hints.get("readOnlyHint", False),
                    "airbyte.mcp.tool_destructive": bool(hints.get("destructiveHint", False)),
                }
            )
        digest = get_http_headers(include={MCP_SESSION_ID_HEADER}).get(MCP_SESSION_ID_HEADER)
        if digest:
            attrs["gen_ai.conversation.id"] = digest
        if context.fastmcp_context is not None:
            attrs["gen_ai.tool.call.id"] = _call_id_digest(context.fastmcp_context.request_id)
            for attribute, config in (
                ("airbyte.mcp.workspace_id", MCP_CONFIG_WORKSPACE_ID),
                ("airbyte.mcp.organization_id", MCP_CONFIG_ORGANIZATION_ID),
            ):
                try:
                    value = get_mcp_config(context.fastmcp_context, config) or ""
                except Exception:
                    continue
                if _UUID_RE.fullmatch(value):
                    attrs[attribute] = value.lower()
        return attrs


def _call_id_digest(request_id: object) -> str:
    """JSON-RPC ids are client-controlled; export a digest so correlation survives."""
    return hashlib.sha256(str(request_id).encode()).hexdigest()


class IntentStampProcessor(SpanProcessor):
    """Stamp intent on start and retain only the in-flight exception's cause class."""

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:  # noqa: ARG002
        """Copy request-local attributes onto the server's tool span."""
        try:
            if span.kind == SpanKind.SERVER and span.name.startswith("tools/call "):
                span.set_attributes(_INTENT_ATTRIBUTES.get() or {})
        except Exception:
            logger.debug("Intent stamping skipped")

    def on_end(self, span: ReadableSpan) -> None:
        """Capture the cause before FastMCP unwinds back to our middleware."""
        # The middleware sees exceptions only after this span has already ended.
        try:
            exc = sys.exc_info()[1]
            if (
                exc is not None
                and span.context is not None
                and span.kind == SpanKind.SERVER
                and span.name.startswith("tools/call ")
            ):
                # Bound state when the batch processor discards spans from a full queue.
                if len(_LATE_ATTRIBUTES) >= _MAX_LATE_ATTRIBUTES:
                    _LATE_ATTRIBUTES.pop(next(iter(_LATE_ATTRIBUTES)))
                _LATE_ATTRIBUTES[span.context.span_id] = {
                    "airbyte.mcp.error_type": type(exc.__cause__ or exc).__name__
                }
        except Exception:
            logger.debug("Exception class capture skipped")

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # noqa: ARG002
        """Allow the provider to continue flushing the batch processor."""
        return True


class RedactingExporter(SpanExporter):
    """The sole exporter boundary: only public, rebuilt spans can leave the process."""

    def __init__(self, exporter: SpanExporter, *, environ: Mapping[str, str] | None = None) -> None:
        """Wrap the destination exporter without exposing it to a span processor."""
        self._exporter, self._environ = exporter, environ

    def _rebuild(self, span: ReadableSpan) -> ReadableSpan | None:
        late = _LATE_ATTRIBUTES.pop(span.context.span_id, {}) if span.context else {}
        # FastMCP also traces resource/prompt requests, including unknown caller names.
        # Only its server tool spans belong in this hosted export pipeline.
        if (
            span.instrumentation_scope is not None
            and span.instrumentation_scope.name == "fastmcp"
            and (span.kind != SpanKind.SERVER or not span.name.startswith("tools/call "))
        ):
            return None
        if (
            span.name.startswith("tools/call ")
            and span.name.removeprefix("tools/call ") not in _TOOL_MODULES
        ):
            return None
        attrs = {
            key: value
            for key, value in (span.attributes or {}).items()
            # Header capture opt-ins would export raw Authorization and cookie values.
            if not key.startswith(("enduser.", "http.request.header.", "http.response.header."))
            and key
            not in {
                "user_agent.original",
                "url.query",
                "http.user_agent",
                # Stable/duplicate HTTP conventions repeat the URL host in these fields.
                "http.host",
                "server.address",
                "network.peer.address",
            }
        }
        for key in ("http.url", "url.full"):
            if key in attrs:
                url = urlsplit(str(attrs[key]))
                clean_url = urlunsplit(
                    (url.scheme, url.netloc.rsplit("@", 1)[-1], url.path, "", "")
                )
                attrs[key] = (
                    clean_url if _SAFE_HTTP_URL.fullmatch(clean_url) else REDACTED_PLACEHOLDER
                )
        attrs.update(late)
        environment = _env(self._environ if self._environ is not None else _ENVIRON)
        if environment.get("AIRBYTE_MCP_OTEL_VENDOR", "").strip().lower() == "datadog":
            metadata = {
                key: attrs[f"airbyte.mcp.{key}"]
                for key in (
                    "intent",
                    "intent_present",
                    "tool_module",
                    "workspace_id",
                    "organization_id",
                    "error_type",
                )
                if f"airbyte.mcp.{key}" in attrs
            }
            if metadata:
                attrs["_dd.ml_obs.metadata"] = json.dumps(metadata)
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
            attributes=attrs,
            events=events,
            links=span.links,
            kind=span.kind,
            status=Status(span.status.status_code),
            start_time=span.start_time,
            end_time=span.end_time,
            instrumentation_scope=span.instrumentation_scope,
        )

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Drop individual redaction failures and contain any destination failure."""
        try:
            rebuilt = []
            for span in spans:
                try:
                    clean = self._rebuild(span)
                    if clean is not None:
                        rebuilt.append(clean)
                except Exception:
                    logger.debug("Dropping span that could not be redacted")
            return self._exporter.export(rebuilt) if rebuilt else SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        """Delegate shutdown without propagating exporter failures."""
        try:
            self._exporter.shutdown()
        except Exception:
            logger.debug("OpenTelemetry exporter shutdown failed")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Delegate flush without propagating exporter failures."""
        try:
            return self._exporter.force_flush(timeout_millis)
        except Exception:
            return False


class SessionIdHeaderDigest:
    """Hash the unsigned client grouping key before MCP instrumentation sees it."""

    def __init__(self, app: ASGIApp) -> None:
        """Wrap the MCP app inside capability minting and hosted HTTP guards."""
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Preserve extension declarations while replacing every session header value."""
        if scope["type"] == "http":
            from fastmcp_extensions.capability_tokens import (
                DEFAULT_EXTENSIONS_HEADER,
                decode_capability_token,
            )

            key = MCP_SESSION_ID_HEADER.encode()
            ext_key = DEFAULT_EXTENSIONS_HEADER.lower().encode()
            headers: list[tuple[bytes, bytes]] = scope.get("headers") or []
            raw = next((value for name, value in headers if name.lower() == key), None)
            if raw is not None:
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
                    # In-app filters decode the token; re-declare before replacing it with a digest.
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
