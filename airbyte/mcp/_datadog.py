# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Hosted MCP observability controls; `ddtrace.auto` owns instrumentation and enablement.

`airbyte.mcp.http_main` imports `ddtrace.auto` when `DD_API_KEY` is set. The LLM
Observability processor and APM sanitizer must both register before the optional
annotations are installed. Every ddtrace import here is lazy so `import airbyte`
never loads ddtrace and the hosted server runs without a Datadog configuration.
"""

# Lazy imports keep ddtrace optional and defer registration dependencies to hosted startup.
# ruff: noqa: PLC0415

from __future__ import annotations

import copy
import hashlib
import logging
import os
import re
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

from fastmcp.server.middleware import Middleware
from fastmcp_extensions import get_mcp_config

from airbyte.agents._api_util import _AGENTS_API_ROOT
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_CONFIG_API_ROOT,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_WORKSPACE_ID,
)


if TYPE_CHECKING:
    from collections.abc import Mapping

    from ddtrace.llmobs import LLMObsSpan
    from ddtrace.trace import Span, TraceFilter
    from fastmcp import FastMCP
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import ToolResult
    from mcp.types import CallToolRequestParams, ListToolsResult
    from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

REDACTED_PLACEHOLDER = "[redacted by airbyte-mcp]"
TELEMETRY_ARG = "telemetry"
LLMOBS_STRUCT_KEY = "_llmobs"
MCP_SESSION_ID_HEADER = "mcp-session-id"
SEGMENT_URL_PREFIX = "https://api.segment.io/"
STRIPPED_SPAN_TAGS = (
    "error.message",
    "error.msg",
    "error.stack",
    "http.useragent",
    "http.referrer_hostname",  # ddtrace 4.15.1 captures Referer even with header tracing off.
    # ddtrace 4.15.1 also captures these regardless of header-tracing/AppSec settings.
    "http.request.headers.x-datadog-security-test",
    "http.request.headers.x-datadog-endpoint-scan",
)
INTENT_INSTRUCTIONS_SENTENCE = (
    " Tools may accept an optional `telemetry.intent` string; if present, "
    "state in one sentence why you are calling the tool "
    "(never credentials, identifiers or data values)."
)
_UUID_PATTERN = r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
_UUID_RE = re.compile(rf"\A{_UUID_PATTERN}\Z")
# Only literal routes and validated IDs may survive export. Keep these aligned with
# _util/api_util.py, agents/_api_util.py and their Public API SDK calls. Unknown
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
    r"users/(?:get_by_auth_id|update)|permissions/list_by_user)|"
    rf"{re.escape(_AGENTS_API_ROOT)}/(?:workspaces(?:/{_UUID_PATTERN})?|"
    rf"integrations/connectors(?:/{_UUID_PATTERN}/(?:inspect|execute))?|skills(?:/docs)?)|"
    rf"{re.escape(SEGMENT_URL_PREFIX)}v1/track"
)
_HTTP_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})
_HTTP_HOST_TAGS = ("out.host", "server.address", "peer.hostname", "network.destination.name")
_TRUTHY = frozenset({"1", "true"})
_INSTALLED = False
_SHIM_INSTALLED = False
_TOOL_MODULES: dict[str, str] = {}
_TOOL_ANNOTATIONS: dict[str, dict[str, Any]] = {}


def _flag(environ: Mapping[str, str] | None, name: str) -> bool:
    environment = os.environ if environ is None else environ
    return environment.get(name, "").strip().lower() in _TRUTHY


def install(app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
    """Install once for the hosted entrypoint, disabling export if redaction fails."""
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    try:
        app.add_middleware(TelemetryArgumentStripMiddleware(app))
    except Exception:
        logger.debug("Telemetry argument strip could not be installed", exc_info=True)
    if not _flag(environ, "DD_LLMOBS_ENABLED"):
        return
    try:
        import ddtrace
        from ddtrace.llmobs import LLMObs
        from ddtrace.trace import tracer
    except ImportError:
        logger.debug("ddtrace not importable; Datadog observability not installed", exc_info=True)
        return
    try:
        LLMObs.register_processor(redact_llmobs_span)
        was_enabled = tracer.enabled
        # configure replaces user processors and recreates the writer in ddtrace 4.15.1.
        # ddtrace accepts this structural TraceFilter without requiring an eager base import.
        tracer.configure(trace_processors=[cast("TraceFilter", ApmExportSanitizer())])
        tracer.enabled = was_enabled  # configure also unconditionally enables the tracer.
    except Exception:
        logger.error(
            "Datadog redaction controls failed to install; disabling export", exc_info=True
        )
        try:
            LLMObs.disable()
        except Exception:
            logger.debug("LLM Observability shutdown failed", exc_info=True)
        finally:
            tracer.enabled = False
        return
    for step in (
        lambda: _set_version_if_unset(ddtrace.config),
        _install_tools_list_schema_shim,
        _build_tool_maps,
        lambda: app.add_middleware(DatadogToolSpanMiddleware()),
        lambda: _add_instructions_sentence(app)
        if _flag(environ, "DD_MCP_CAPTURE_INTENT")
        else None,
    ):
        try:
            step()
        except Exception:
            logger.debug("Datadog observability step failed", exc_info=True)


def _set_version_if_unset(config: Any) -> None:  # noqa: ANN401  # ddtrace's dynamic public config.
    if not config.version:
        from airbyte.version import get_version

        config.version = get_version()


def _add_instructions_sentence(app: FastMCP) -> None:
    if INTENT_INSTRUCTIONS_SENTENCE.strip() not in (app.instructions or ""):
        app.instructions = (app.instructions or "") + INTENT_INSTRUCTIONS_SENTENCE


def _build_tool_maps() -> None:
    # Registration records are private in fastmcp-extensions; avoid per-call tool resolution.
    from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701
    from fastmcp_extensions.tool_filters import ANNOTATION_MCP_MODULE

    for func, tool_annotations in _REGISTERED_TOOLS:
        name = getattr(func, "__name__", None)
        if name:
            _TOOL_MODULES[name] = str(tool_annotations.get(ANNOTATION_MCP_MODULE, ""))
            _TOOL_ANNOTATIONS[name] = dict(tool_annotations)


def _install_tools_list_schema_shim() -> None:
    global _SHIM_INSTALLED
    if _SHIM_INSTALLED:
        return
    # Private integration hook: ddtrace issue #20414 / PR #20446. Keep the deep copy
    # after the required-field bug is fixed upstream: injection still mutates schemas.
    from ddtrace.llmobs._integrations.mcp import MCPIntegration  # noqa: PLC2701

    if not hasattr(MCPIntegration, "inject_tools_list_response"):
        logger.debug("MCPIntegration.inject_tools_list_response missing; shim skipped")
        return
    original_inject = MCPIntegration.inject_tools_list_response

    def fixed_inject(self: MCPIntegration, response: ListToolsResult) -> None:
        for tool in response.tools:
            tool.inputSchema = copy.deepcopy(tool.inputSchema)
        before = {
            tool.name: list(tool.inputSchema.get("required", []))
            for tool in response.tools
            if TELEMETRY_ARG not in tool.inputSchema.get("properties", {})
        }
        original_inject(self, response)
        for tool in response.tools:
            if tool.name in before:
                tool.inputSchema["required"] = before[tool.name]

    MCPIntegration.inject_tools_list_response = fixed_inject
    _SHIM_INSTALLED = True


class TelemetryArgumentStripMiddleware(Middleware):
    """Accept cached advertised telemetry after observability has been disabled."""

    def __init__(self, app: FastMCP) -> None:
        """Keep the app for checking whether telemetry is a real tool parameter."""
        self._app = app

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Remove only synthetic telemetry, leaving unknown tools untouched."""
        try:
            args = context.message.arguments or {}
            if TELEMETRY_ARG in args:
                tool = await self._app.get_tool(context.message.name)
                if tool is not None and TELEMETRY_ARG not in tool.parameters.get("properties", {}):
                    context = context.copy(
                        message=context.message.model_copy(
                            update={
                                "arguments": {k: v for k, v in args.items() if k != TELEMETRY_ARG}
                            }
                        )
                    )
        except Exception:
            logger.debug("Telemetry argument strip skipped", exc_info=True)
        return await call_next(context)


class DatadogToolSpanMiddleware(Middleware):
    """Annotate the existing ddtrace tool span without adding a workflow root."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Add bounded event tags and the cause class while preserving tool behavior."""
        try:
            from ddtrace.llmobs import LLMObs
            from ddtrace.trace import tracer

            span = tracer.current_span()
        except Exception:
            logger.debug("Datadog tool span unavailable", exc_info=True)
            return await call_next(context)
        if span is None or span.name != "mcp.tools/call":
            return await call_next(context)

        try:
            LLMObs.annotate(span=span, tags=self._tags(context, span))
        except Exception:
            # ponytail: 4.15.1 raises after applying tags, before MCP sets kind at respond().
            logger.debug("LLMObs.annotate raised (expected pre-kind)", exc_info=True)
        try:
            return await call_next(context)
        except Exception as exc:
            cause = exc.__cause__ or exc
            try:
                LLMObs.annotate(span=span, tags={"airbyte_error_type": type(cause).__name__})
            except Exception:
                logger.debug("LLMObs.annotate (error) raised", exc_info=True)
            raise

    @staticmethod
    def _tags(context: MiddlewareContext[CallToolRequestParams], span: Span) -> dict[str, str]:
        from fastmcp.server.dependencies import get_http_headers

        from airbyte._util.meta import get_cloud_api_analytic_source

        name = context.message.name
        tags: dict[str, str] = {}
        if name in _TOOL_MODULES:
            tags["airbyte_tool_module"] = _TOOL_MODULES[name]
            hints = _TOOL_ANNOTATIONS.get(name, {})
            tags["airbyte_tool_mutating"] = str(not hints.get("readOnlyHint", False)).lower()
            tags["airbyte_tool_destructive"] = str(
                bool(hints.get("destructiveHint", False))
            ).lower()
        tags["airbyte_analytic_source"] = get_cloud_api_analytic_source()
        try:
            # Private ddtrace struct read; omit on failure and use @meta.intent:* in the UI.
            struct = span._get_struct_tag(LLMOBS_STRUCT_KEY) or {}  # noqa: SLF001
            tags["intent_present"] = str(bool((struct.get("meta") or {}).get("intent"))).lower()
        except Exception:
            logger.debug("intent_present unavailable", exc_info=True)
        digest = get_http_headers(include={MCP_SESSION_ID_HEADER}).get(MCP_SESSION_ID_HEADER)
        if digest:
            tags["session_id"] = digest  # SessionIdHeaderDigest has already hashed the bytes.
        if context.fastmcp_context is not None:
            for tag, cfg in (
                ("airbyte_workspace_id", MCP_CONFIG_WORKSPACE_ID),
                ("airbyte_organization_id", MCP_CONFIG_ORGANIZATION_ID),
            ):
                value = get_mcp_config(context.fastmcp_context, cfg) or ""
                if _UUID_RE.match(value):
                    tags[tag] = value.lower()
        return tags


def redact_llmobs_span(span: LLMObsSpan) -> LLMObsSpan | None:
    """Replace input/output or drop the event; raising would export the original."""
    try:
        if span.get_tag("mcp_method") == "initialize":
            return None
        # Unregistered names are arbitrary caller text, not server-owned tool names.
        if (
            span.get_tag("mcp_method") == "tools/call"
            and span.get_tag("mcp_tool") not in _TOOL_MODULES
        ):
            return None
        span.input = [{"role": "", "content": REDACTED_PLACEHOLDER}]
        span.output = [{"role": "", "content": REDACTED_PLACEHOLDER}]
    except Exception:
        logger.debug("redact_llmobs_span failed; dropping event", exc_info=True)
        return None
    else:
        return span


class ApmExportSanitizer:
    """Implement ddtrace's TraceFilter protocol without importing it at module load."""

    def process_trace(self, trace: list[Span]) -> list[Span] | None:
        """Drop Segment roots and strip sensitive tags from every remaining span."""
        try:
            for span in trace:
                if span.parent_id is None:
                    url = span.get_tag("http.url") or ""
                    if span.name == "requests.request" and url.startswith(SEGMENT_URL_PREFIX):
                        return None
                    break
            for span in trace:
                for key in STRIPPED_SPAN_TAGS:
                    span.remove_tag(key)
                if span.name == "requests.request":
                    _sanitize_http_target(span)
                elif span.name == "starlette.request":
                    _sanitize_inbound_http_target(span)
        except Exception:
            logger.debug("ApmExportSanitizer failed; dropping trace", exc_info=True)
            return None
        else:
            return trace


def _sanitize_inbound_http_target(span: Span) -> None:
    """Keep server-owned routes instead of raw inbound hosts, paths, and methods."""
    for key in ("http.url", "http.query.string", "http.target", *_HTTP_HOST_TAGS):
        span.remove_tag(key)
    method = span.get_tag("http.method") or ""
    method = method if method in _HTTP_METHODS else "HTTP"
    span.set_tag("http.method", method)
    # ddtrace 4.15.1 composes this tag from Route/Mount.path, never path parameter values.
    route = span.get_tag("http.route") or REDACTED_PLACEHOLDER
    span.resource = f"{method} {route}"


def _sanitize_http_target(span: Span) -> None:
    """Retain only known HTTP routes; tool arguments can otherwise leak through IDs."""
    url = (span.get_tag("http.url") or "").split("?", 1)[0].split("#", 1)[0]
    method = span.get_tag("http.method") or ""
    method = method if method in _HTTP_METHODS else "HTTP"
    span.set_tag("http.method", method)
    # Rewrite every host alias from validated data rather than trusting their original values.
    for key in _HTTP_HOST_TAGS:
        span.remove_tag(key)
    span.remove_tag("http.query.string")
    if _SAFE_HTTP_URL.fullmatch(url):
        target = urlsplit(url)
        span.set_tag("http.url", url)
        span.set_tag("out.host", target.hostname)
        span.resource = f"{method} {target.path}"
    else:
        span.set_tag("http.url", REDACTED_PLACEHOLDER)
        span.resource = f"{method} {REDACTED_PLACEHOLDER}"


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
