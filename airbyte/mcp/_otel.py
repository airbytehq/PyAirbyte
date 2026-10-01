# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Hosted MCP intent capture and fail-closed OpenTelemetry span export."""

# Hosted startup is the only caller; defer exporter and registration imports until needed.
# ruff: noqa: PLC0415

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
import unicodedata
from contextlib import nullcontext
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from fastmcp.server.middleware import Middleware
from fastmcp.server.telemetry import _active_seam_span, seam_span  # noqa: PLC2701
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import SpanKind, Status, StatusCode

from airbyte._direct_connectors.models import ExternalApiReadOnlyAction, ExternalSearchType
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_CONFIG_API_ROOT,
)
from airbyte.mcp import _arg_trace
from airbyte.mcp._scope import current_call_scope, enrich_call_scope, scope_from_request
from airbyte.mcp._telemetry_key import load_master
from airbyte.version import get_version


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from fastmcp import FastMCP
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import Tool, ToolResult
    from mcp.server.context import ServerRequestContext
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
    r"connector_builder_projects/(?:list|get_for_definition_id|get_with_manifest|update_testing_values)|"
    r"organizations/(?:list_by_user_id|get_organization_info)|"
    r"workspaces/(?:list_by_organization_id|list_by_user_id|get_organization_info|get)|"
    r"state/(?:get|create_or_update_safe)|web_backend/connections/(?:get|update)|"
    r"users/(?:get_by_auth_id|update)|permissions/list_by_user|jobs/get|"
    rf"(?:sources|destinations)/{_UUID_PATTERN}/(?:execute|search|search-status|enablement)|"
    rf"workspaces/{_UUID_PATTERN}/skills/docs)"
)
REDACTED_PLACEHOLDER = "[redacted by airbyte-mcp]"
_MAX_INTENT_LENGTH = 4096
_MAX_LATE_ATTRIBUTES = 4096
_MAX_ENTITY_TYPE_LENGTH = 256
_ENTITY_TYPE_ACTIONS = {
    "execute_external_api_query": tuple(member.value for member in ExternalApiReadOnlyAction),
}
_INSTALLED = False
_ENVIRON: Mapping[str, str] | None = None
_TOOL_MODULES: dict[str, str] = {}
_TOOL_ANNOTATIONS: dict[str, dict[str, Any]] = {}
_TOOL_ARG_CLASSES: dict[str, dict[str, _arg_trace.ArgClass]] = {}
_TOOL_ERROR_STRINGS: dict[str, frozenset[str]] = {}
# Loaded once per process from the hosted-only secret; never exported or logged.
_ARG_MASTER: bytes | None = None
_APPROXIMATE_BUCKET_SECONDS = 1800
_AGENT_ACTION_VALUES: dict[str, dict[str, str]] = {
    "execute_external_api_query": {
        member.value: member.value for member in ExternalApiReadOnlyAction
    },
    "execute_external_sql_query": {"sql_select": "sql_select"},
    "execute_external_search_query": {
        f"search_{member.value}": f"search_{member.value}" for member in ExternalSearchType
    },
}
# Middleware runs outside FastMCP's span; a ContextVar survives trace-context extraction.
_INTENT_ATTRIBUTES: ContextVar[dict[str, str | bool | int] | None] = ContextVar(
    "mcp_intent", default=None
)
_LATE_ATTRIBUTES: dict[int, dict[str, str | bool | int]] = {}
_LATE_LOCK = threading.Lock()


def _env(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _flag(environ: Mapping[str, str] | None, name: str) -> bool:
    return _env(environ).get(name, "").strip().lower() in {"1", "true"}


def _tracing_backend(environ: Mapping[str, str] | None) -> str:
    environment = _env(environ)
    backend = environment.get("AIRBYTE_MCP_TRACING_BACKEND")
    if backend is None:
        # Explicit legacy settings retain their OTLP transport.
        vendor = environment.get("AIRBYTE_MCP_OTEL_VENDOR")
        if vendor is not None:
            return "datadog-otlp" if vendor.strip().lower() == "datadog" else "otel"
        return "datadog" if _flag(environ, "DD_LLMOBS_ENABLED") else "otel"
    backend = backend.strip().lower()
    if backend not in {"otel", "datadog-otlp", "datadog"}:
        raise ValueError("AIRBYTE_MCP_TRACING_BACKEND must be 'otel', 'datadog-otlp', or 'datadog'")
    return backend


def install(app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
    """Install one hosted backend; OTel exports only with a configured endpoint.

    `environ` is a test seam for enablement and the `AIRBYTE_MCP_*` controls only.
    The OTel SDK always reads exporter and resource configuration from process
    environment; production callers should omit `environ`. For OTel, an existing global
    provider or requests instrumentation prevents safe redaction and therefore
    refuses hosted startup, including when this integration's endpoint is unset.
    """
    global _INSTALLED, _ENVIRON
    if _INSTALLED:
        return
    backend = _tracing_backend(environ)
    if backend == "datadog":
        from airbyte.mcp._datadog import install as install_datadog

        install_datadog(app, environ=environ)
        _INSTALLED, _ENVIRON = True, environ
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

            _build_tool_maps(environ)
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
    global _ARG_MASTER
    _ARG_MASTER = None
    with _LATE_LOCK:
        _LATE_ATTRIBUTES.clear()
    _TOOL_MODULES.clear()
    _TOOL_ANNOTATIONS.clear()
    _TOOL_ARG_CLASSES.clear()
    _TOOL_ERROR_STRINGS.clear()


def _build_tool_maps(environ: Mapping[str, str] | None = None) -> None:
    """Index registered tools and load the argument-tracing key, once per install."""
    global _ARG_MASTER
    from fastmcp_extensions.annotations import ANNOTATION_MCP_MODULE
    from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701

    for func, tool_annotations in _REGISTERED_TOOLS:
        name = getattr(func, "__name__", None)
        if name:
            _TOOL_MODULES[name] = str(tool_annotations.get(ANNOTATION_MCP_MODULE, ""))
            _TOOL_ANNOTATIONS[name] = dict(tool_annotations)
            try:
                _TOOL_ARG_CLASSES[name] = _arg_trace.classify_tool(func)
                _TOOL_ERROR_STRINGS[name] = _arg_trace.literal_error_strings(func)
            except Exception as exc:
                # An unclassifiable tool keeps its other telemetry but gets no arg records.
                _TOOL_ARG_CLASSES.pop(name, None)
                logger.warning("Argument tracing disabled for one tool: %s", type(exc).__name__)
    _ARG_MASTER = load_master(_env(environ))
    if _ARG_MASTER is not None:
        logger.info("arg tracing key loaded, key_id=%s", _arg_trace.key_id(_ARG_MASTER))


class _StripMetaTraceContextMiddleware:
    """SDK-tier middleware dropping untrusted `_meta` tracing context.

    FastMCP 4 extracts `_meta.traceparent`/`tracestate` for span parenting in
    its seam span, which opens *above* the FastMCP middleware layer — so the
    pop in `IntentCaptureMiddleware` runs too late to prevent an untrusted
    client from parenting the server span onto its own trace. Inserted into
    `LowLevelServer.middleware` just ahead of `FastMCPServerMiddleware`.
    """

    async def __call__(
        self,
        ctx: ServerRequestContext[Any],
        call_next: Callable[[ServerRequestContext[Any]], Awaitable[Any]],
    ) -> Any:  # noqa: ANN401
        # The seam reads trace context from `ctx.params["_meta"]` (lifted into
        # FastMCPRequestContext.meta inside the seam); `ctx.meta` only carries
        # `progress_token`, so strip on the raw params block.
        params = getattr(ctx, "params", None)
        meta = params.get("_meta") if isinstance(params, dict) else None
        if isinstance(meta, dict):
            meta.pop("traceparent", None)
            meta.pop("tracestate", None)
        return await call_next(ctx)


def _install_meta_trace_context_middleware(app: FastMCP) -> None:
    """Insert `_StripMetaTraceContextMiddleware` ahead of FastMCP's seam span."""
    from fastmcp.server.low_level import FastMCPServerMiddleware

    low_level_middleware = app._mcp_server.middleware  # noqa: SLF001
    index = next(
        (
            i
            for i, middleware in enumerate(low_level_middleware)
            if isinstance(middleware, FastMCPServerMiddleware)
        ),
        None,
    )
    if index is None:
        raise RuntimeError(
            "FastMCPServerMiddleware not found; cannot install _meta trace-context stripping"
        )
    low_level_middleware.insert(index, _StripMetaTraceContextMiddleware())


def _client_label(value: object) -> str | None:
    """Bound reported application labels; this is not arbitrary-text sanitization."""
    if not isinstance(value, str) or any(
        unicodedata.category(char).startswith("C") for char in value
    ):
        return None
    return value.strip()[:256] or None


class IntentCaptureMiddleware(Middleware):
    """Advertise optional intent and carry it into FastMCP's existing tool span."""

    def __init__(self, app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
        """Retain the app to distinguish synthetic intent from real parameters."""
        self._app, self._environ = app, environ
        _install_meta_trace_context_middleware(app)

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
        attrs: dict[str, str | bool | int] = {}
        try:
            if context.fastmcp_context and context.fastmcp_context.request_context:
                meta = context.fastmcp_context.request_context.meta
                if meta:
                    meta.pop("traceparent", None)
                    meta.pop("tracestate", None)
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
        return await self._trace_call(context, call_next, attrs)

    async def _trace_call(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
        attrs: dict[str, str | bool | int],
    ) -> ToolResult:
        """Record an OTel call after common argument preparation."""
        # HTTP already owns a seam span above middleware. Nested/in-process calls
        # need their own seam, kept alive until we inspect the result. FastMCP
        # enriches that same span, avoiding a second span or an ended-span race.
        nested = _INTENT_ATTRIBUTES.get() is not None
        token = _INTENT_ATTRIBUTES.set(attrs)
        try:
            with (
                seam_span("tools/call", self._app.name)
                if nested or _active_seam_span.get() is None
                else nullcontext(trace.get_current_span())
            ) as span:
                try:
                    try:
                        span.set_attributes(attrs)
                    except Exception:
                        logger.debug("Intent stamping skipped")
                    result = await call_next(context)
                    try:
                        span.set_attributes(result_error_like_attributes(context, result))
                    except Exception:
                        logger.debug("Result classification skipped")
                    try:
                        span.set_attribute("airbyte.mcp.outcome", "success")
                        if result.is_error and context.message.name in _TOOL_MODULES:
                            # Never read error content: only a fixed category leaves here.
                            span.set_status(Status(StatusCode.ERROR))
                            span.set_attributes(
                                {
                                    "airbyte.mcp.error_type": "ToolError",
                                    "error.type": "ToolError",
                                    "airbyte.mcp.outcome": "tool_error",
                                }
                            )
                    except Exception:
                        logger.debug("Tool error status capture skipped")
                except BaseException as exc:
                    # HTTP converts exceptions into protocol errors before its
                    # seam ends, so retain the original class at this boundary.
                    try:
                        span.set_status(Status(StatusCode.ERROR))
                        _record_late_attributes(_exception_attributes(exc))
                    except Exception:
                        logger.debug("Exception class capture skipped")
                    raise
                else:
                    return result
                finally:
                    try:
                        if not isinstance(sys.exc_info()[1], asyncio.CancelledError):
                            await enrich_call_scope(context.fastmcp_context)
                    except asyncio.CancelledError as exc:
                        span.set_status(Status(StatusCode.ERROR))
                        _record_late_attributes(_exception_attributes(exc))
                        raise
                    finally:
                        _record_default_workspace()
        finally:
            _INTENT_ATTRIBUTES.reset(token)

    @staticmethod
    def _attributes(
        context: MiddlewareContext[CallToolRequestParams],
        intent: object,
    ) -> dict[str, str | bool | int]:
        from airbyte._util.meta import get_cloud_api_analytic_source

        name = context.message.name
        intent = intent.strip() if isinstance(intent, str) else ""
        if len(intent) > _MAX_INTENT_LENGTH:
            marker = "...[truncated]"
            intent = intent[: _MAX_INTENT_LENGTH - len(marker)] + marker
        tool_start = time.time()
        attrs: dict[str, str | bool | int] = {
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": name,
            "airbyte.mcp.intent_present": bool(intent),
            "airbyte.mcp.analytic_source": get_cloud_api_analytic_source(),
        }
        attrs.update(_request_trace_attributes())
        if intent:
            attrs["airbyte.mcp.intent"] = intent
        arguments = context.message.arguments or {}
        if name == "execute_external_sql_query":
            action = "sql_select"
        elif name == "execute_external_search_query":
            search_type = arguments.get("search_type", ExternalSearchType.HYBRID.value)
            action = f"search_{search_type}" if isinstance(search_type, str) else None
        else:
            action = arguments.get("action", ExternalApiReadOnlyAction.LIST.value)
        if isinstance(action, str):
            canonical_action = _AGENT_ACTION_VALUES.get(name, {}).get(action)
            if canonical_action is not None:
                attrs["airbyte.mcp.agent.action"] = canonical_action
        if isinstance(action, str) and action in _ENTITY_TYPE_ACTIONS.get(name, ()):
            entity_type = arguments.get("entity_type")
            if (
                isinstance(entity_type, str)
                and entity_type
                and entity_type.isprintable()
                and entity_type == entity_type.strip()
            ):
                attrs["airbyte.mcp.agent.entity_type"] = entity_type[
                    :_MAX_ENTITY_TYPE_LENGTH
                ].rstrip()
        if name in _TOOL_MODULES:
            hints = _TOOL_ANNOTATIONS.get(name, {})
            attrs.update(
                {
                    "airbyte.mcp.tool_module": _TOOL_MODULES[name],
                    "airbyte.mcp.tool_mutating": not hints.get("readOnlyHint", False),
                    "airbyte.mcp.tool_destructive": bool(hints.get("destructiveHint", False)),
                }
            )
        if context.fastmcp_context is not None:
            attrs["gen_ai.tool.call.id"] = _call_id_digest(context.fastmcp_context.request_id)
        scope = current_call_scope() or scope_from_request(context)
        attrs.update(
            {
                attribute: value
                for attribute, value in (
                    ("airbyte.mcp.workspace_id", scope.workspace_id),
                    ("airbyte.mcp.organization_id", scope.organization_id),
                    ("airbyte.mcp.scope_source", scope.scope_source),
                )
                if value
            }
        )
        attrs.update(_arg_trace_attributes(name, arguments, tool_start))
        return attrs


def _verified_principal() -> str | None:
    """Return `iss + NUL + subject` of the verified access token, else `None`.

    Never reads headers or unverified claims; the value is neither logged nor exported.
    """
    from fastmcp.server.dependencies import get_access_token

    from airbyte._util.api_util import get_user_id_from_bearer_token
    from airbyte.exceptions import PyAirbyteInputError
    from airbyte.secrets.base import SecretString

    token = get_access_token()
    if token is None:
        return None
    issuer = token.claims.get("iss")
    try:
        subject = get_user_id_from_bearer_token(SecretString(token.token))
    except PyAirbyteInputError:
        return None
    if not (isinstance(issuer, str) and issuer and isinstance(subject, str) and subject):
        return None
    if "\x00" in issuer or "\x00" in subject:
        return None
    return f"{issuer}\x00{subject}"


def _arg_keys(
    master: bytes | None, tool_start: float
) -> tuple[_arg_trace.ArgKeys | None, str, str]:
    """Return `(keys, arg_tracing, arg_key_scope)`; fails closed without key or principal."""
    from airbyte.mcp._telemetry import current_grouping_id, request_properties

    if master is None:
        return None, "no_key", "none"
    principal = _verified_principal()
    if principal is None:
        return None, "no_scope", "none"
    grouping = current_grouping_id()
    if grouping.kind != "none" and grouping.raw_digest:
        keys = _arg_trace.keys_for(master, grouping.kind, principal, grouping.raw_digest)
        return keys, "ok", grouping.kind
    properties = request_properties()
    client_name = _client_label(properties.get("mcp_client_name")) or ""
    client_version = _client_label(properties.get("mcp_client_version")) or ""
    major = re.match(r"[0-9]+", client_version)
    keys = _arg_trace.approximate_keys_for(
        master,
        principal,
        client_name,
        major.group() if major else "",
        int(tool_start // _APPROXIMATE_BUCKET_SECONDS),
    )
    return keys, "ok", "approximate"


def _arg_trace_attributes(
    name: str, arguments: Mapping[str, object], tool_start: float
) -> dict[str, str | bool | int]:
    """Return argument records and tracing state for a registered tool; never raises."""
    classes = _TOOL_ARG_CLASSES.get(name)
    if classes is None:
        return {}
    try:
        keys, tracing, key_scope = _arg_keys(_ARG_MASTER, tool_start)
        attrs: dict[str, str | bool | int] = dict(
            _arg_trace.build_records(name, arguments, classes, keys)
        )
        attrs[_arg_trace.TRACING_KEY] = tracing
        attrs[_arg_trace.KEY_SCOPE_KEY] = key_scope
        if keys is not None:
            attrs[_arg_trace.SCOPE_ID_KEY] = _arg_trace.scope_id(keys.k_eq)
    except Exception as exc:
        logger.debug("Argument tracing failed: %s", type(exc).__name__)
        return {_arg_trace.TRACING_KEY: "error", _arg_trace.KEY_SCOPE_KEY: "none"}
    return attrs


def result_error_like_attributes(
    context: MiddlewareContext[CallToolRequestParams], result: ToolResult
) -> dict[str, bool]:
    """Classify a registered tool result by its declared `Literal` error strings only."""
    name = context.message.name
    if name not in _TOOL_ARG_CLASSES:
        return {}
    errors = _TOOL_ERROR_STRINGS.get(name, frozenset())
    return {
        _arg_trace.RESULT_ERROR_LIKE_KEY: not result.is_error
        and _arg_trace.is_error_like(result, errors)
    }


def record_tool_span_attributes(attrs: Mapping[str, str | bool | int]) -> None:
    """Attach attributes from inside a tool to its root span on either backend; never raises."""
    try:
        current = _INTENT_ATTRIBUTES.get()
        if current is not None:
            current.update(attrs)
        _record_late_attributes(dict(attrs))
    except Exception as exc:
        logger.debug("Tool span attributes skipped: %s", type(exc).__name__)


def _request_trace_attributes() -> dict[str, str]:
    """Reuse analytics context, exporting only bounded labels and the session digest."""
    from fastmcp.server.dependencies import get_http_headers, get_http_request

    from airbyte.mcp._telemetry import _SESSION_ID_STATE_KEY, request_properties

    attrs: dict[str, str] = {}
    try:
        properties = request_properties()
        for field, key in (
            ("client_name", "mcp_client_name"),
            ("client_version", "mcp_client_version"),
            ("mcp_protocol_version", "mcp_protocol_version"),
        ):
            if value := _client_label(properties.get(key)):
                attrs[f"airbyte.mcp.{field}"] = value
        if properties.get("auth_method") in {"bearer", "client_credentials", "none"}:
            attrs["airbyte.mcp.auth_method"] = str(properties["auth_method"])
        session = properties.get("session_id")
        if properties.get("transport") == "stdio" and isinstance(session, str):
            try:
                get_http_request()
            except RuntimeError:
                session = hashlib.sha256(session.encode()).hexdigest()
            else:
                # A manually hosted app may not set PyAirbyte's hosted-mode flag.
                # Its process-wide stdio ID must not group unrelated HTTP clients.
                session = None
        if isinstance(session, str) and re.fullmatch(r"[0-9a-f]{64}", session):
            attrs["airbyte.mcp.session_id"] = session
    except Exception:
        logger.debug("Request trace attributes unavailable")
    # Only request state proves that a wrapper hashed the header. Its shape alone
    # cannot distinguish a digest from a caller-supplied hexadecimal token.
    try:
        headers = get_http_headers(include={MCP_SESSION_ID_HEADER, "mcp-protocol-version"})
        if "airbyte.mcp.session_id" not in attrs:
            session = get_http_request().scope.get("state", {}).get(_SESSION_ID_STATE_KEY)
            if isinstance(session, str) and re.fullmatch(r"[0-9a-f]{64}", session):
                attrs["airbyte.mcp.session_id"] = session
            elif raw_session := headers.get(MCP_SESSION_ID_HEADER):
                attrs["airbyte.mcp.session_id"] = hashlib.sha256(
                    raw_session.encode("latin-1")
                ).hexdigest()
        if "airbyte.mcp.mcp_protocol_version" not in attrs and (
            protocol := _client_label(headers.get("mcp-protocol-version"))
        ):
            attrs["airbyte.mcp.mcp_protocol_version"] = protocol
    except Exception:
        logger.debug("Request header trace attributes unavailable")
    if session := attrs.get("airbyte.mcp.session_id"):
        attrs["gen_ai.conversation.id"] = session
    return attrs


def _exception_attributes(error: BaseException) -> dict[str, str]:
    """Classify a failure without reading its potentially sensitive text."""
    return {
        "airbyte.mcp.outcome": "cancelled"
        if isinstance(error, asyncio.CancelledError)
        else "exception",
        "airbyte.mcp.error_type": type(error.__cause__ or error).__name__,
        "error.type": type(error.__cause__ or error).__name__,
    }


def _record_default_workspace() -> None:
    """Attach the scope only known once the tool has run: its default workspace or org."""
    call_scope = current_call_scope()
    scope = call_scope.resolved() if call_scope is not None else None
    if scope is None:
        return
    late_attributes = {
        f"airbyte.mcp.{key}": value
        for key, value in scope.to_properties().items()
        if value is not None
    }
    if late_attributes:
        try:
            _record_late_attributes(late_attributes)
        except Exception:
            logger.debug("Default workspace capture skipped")


def _record_late_attributes(attributes: Mapping[str, str | bool | int]) -> None:
    """Attach attributes to the in-flight span at export, after it has ended."""
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if not (span.is_recording() and span_context.is_valid):
        return
    with _LATE_LOCK:
        if span_context.span_id not in _LATE_ATTRIBUTES and len(_LATE_ATTRIBUTES) >= (
            _MAX_LATE_ATTRIBUTES
        ):
            _LATE_ATTRIBUTES.pop(next(iter(_LATE_ATTRIBUTES)))
        _LATE_ATTRIBUTES.setdefault(span_context.span_id, {}).update(attributes)


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
                with _LATE_LOCK:
                    if len(_LATE_ATTRIBUTES) >= _MAX_LATE_ATTRIBUTES:
                        _LATE_ATTRIBUTES.pop(next(iter(_LATE_ATTRIBUTES)))
                    _LATE_ATTRIBUTES.setdefault(span.context.span_id, {}).update(
                        _exception_attributes(exc)
                    )
        except Exception:
            logger.debug("Exception class capture skipped")

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # noqa: ARG002
        """Allow the provider to continue flushing the batch processor."""
        return True


def _validate_arg_attributes(attrs: dict[str, Any], root_tool: str | None) -> None:
    """Remove every argument-tracing key; re-add only validated keys on root tool spans."""
    _arg_trace.merge_tool_flats(attrs)
    arg_attrs = {key: attrs.pop(key) for key in list(attrs) if _arg_trace.is_new_key(key)}
    if root_tool is None or root_tool not in _TOOL_ARG_CLASSES:
        return
    accepted, dropped = _arg_trace.validate(root_tool, arg_attrs, _TOOL_ARG_CLASSES)
    attrs.update(accepted)
    if dropped:
        attrs[_arg_trace.DROPPED_KEY] = dropped


class RedactingExporter(SpanExporter):
    """The sole exporter boundary: only validated, rebuilt spans can leave the process."""

    def __init__(self, exporter: SpanExporter, *, environ: Mapping[str, str] | None = None) -> None:
        """Wrap the destination exporter without exposing it to a span processor."""
        self._exporter, self._environ = exporter, environ

    def _rebuild(self, span: ReadableSpan) -> ReadableSpan | None:
        with _LATE_LOCK:
            late = _LATE_ATTRIBUTES.pop(span.context.span_id, {}) if span.context else {}
        # FastMCP also traces resource/prompt requests, including unknown caller
        # names, and the `mcp` SDK emits its own client/session spans. Only the
        # server tool spans belong in this hosted export pipeline.
        if (
            span.instrumentation_scope is not None
            and span.instrumentation_scope.name in {"fastmcp", "mcp-python-sdk"}
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
                # Rebuild Datadog Input from approved metadata, never raw tool data.
                "gen_ai.tool.call.arguments",
                "gen_ai.tool.call.result",
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
        if "mcp.session.id" in attrs:
            # FastMCP can copy the raw header on apps without our HTTP wrapper.
            attrs.pop("mcp.session.id")
            if session := attrs.get("airbyte.mcp.session_id"):
                attrs["mcp.session.id"] = session
        entity_type = attrs.pop("airbyte.mcp.agent.entity_type", None)
        action = attrs.pop("airbyte.mcp.agent.action", None)
        tool_name = span.name.removeprefix("tools/call ")
        root_tool_span = (
            span.kind == SpanKind.SERVER
            and span.parent is None
            and span.name.startswith("tools/call ")
            and tool_name in _TOOL_MODULES
        )
        _validate_arg_attributes(attrs, tool_name if root_tool_span else None)
        if root_tool_span and isinstance(action, str):
            canonical_action = _AGENT_ACTION_VALUES.get(tool_name, {}).get(action)
            if canonical_action is not None:
                attrs["airbyte.mcp.agent.action"] = canonical_action
        if not root_tool_span or tool_name not in _ENTITY_TYPE_ACTIONS:
            entity_type = None
        if (
            isinstance(entity_type, str)
            and entity_type
            and entity_type.isprintable()
            and entity_type == entity_type.strip()
        ):
            attrs["airbyte.mcp.agent.entity_type"] = entity_type[:_MAX_ENTITY_TYPE_LENGTH].rstrip()
        for key in ("client_name", "client_version"):
            value = _client_label(attrs.pop(f"airbyte.mcp.{key}", None))
            if (
                value is not None
                and span.kind == SpanKind.SERVER
                and span.name.startswith("tools/call ")
                and tool_name in _TOOL_MODULES
            ):
                attrs[f"airbyte.mcp.{key}"] = value
        attrs.pop("_dd.ml_obs.metadata", None)
        environment = _env(self._environ if self._environ is not None else _ENVIRON)
        if _tracing_backend(environment) == "datadog-otlp":
            metadata = {
                key: attrs[f"airbyte.mcp.{key}"]
                for key in (
                    "intent",
                    "intent_present",
                    "tool_module",
                    "workspace_id",
                    "organization_id",
                    "scope_source",
                    "error_type",
                    "outcome",
                    "auth_method",
                    "mcp_protocol_version",
                    "session_id",
                    "agent.action",
                    "agent.entity_type",
                    "client_name",
                    "client_version",
                    *sorted(_arg_trace.LLMOBS_ALLOWED),
                )
                if f"airbyte.mcp.{key}" in attrs
            }
            if metadata:
                attrs["_dd.ml_obs.metadata"] = json.dumps(metadata)
            tool_input = {
                label: metadata[key]
                for label, key in (
                    ("intent", "intent"),
                    ("action", "agent.action"),
                    ("entity_name", "agent.entity_type"),
                )
                if isinstance(metadata.get(key), str) and metadata[key]
            }
            if (
                span.kind == SpanKind.SERVER
                and span.name.startswith("tools/call ")
                and tool_name in _TOOL_MODULES
                and tool_input
            ):
                attrs["gen_ai.tool.call.arguments"] = json.dumps(tool_input)
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
                from airbyte.mcp._telemetry import _SESSION_ID_STATE_KEY

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
