# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Native Datadog MCP spans without raw tool payloads."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import re
import sys
import unicodedata
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from fastmcp.server.middleware import Middleware
from fastmcp.telemetry import suppress_fastmcp_telemetry
from fastmcp_extensions.otel._arg_digests import ArgTracer, is_arg_key  # noqa: PLC2701
from fastmcp_extensions.otel.middleware import arg_trace_attributes
from mcp.types import (
    CallToolResult,
    InitializeRequest,
    InitializeResult,
    ListToolsRequest,
    ListToolsResult,
)
from opentelemetry.instrumentation.requests import RequestsInstrumentor

from airbyte.mcp._otel import _arg_key, _env, _flag
from airbyte.mcp._scope import current_call_scope, scope_from_request
from airbyte.mcp._trace_attributes import agent_action_attributes
from airbyte.version import get_version


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from ddtrace.llmobs import LLMObsSpan
    from ddtrace.trace import Span
    from fastmcp import FastMCP
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import Tool, ToolResult
    from mcp.server.context import ServerRequestContext
    from mcp.types import CallToolRequestParams

logger = logging.getLogger(__name__)
REDACTED = "[REDACTED]"
INTENT_ARG = "intent"
_LEGACY_TELEMETRY_ARG = "telemetry"
MCP_SESSION_ID_HEADER = "mcp-session-id"
_MAX_INTENT_LENGTH = 4096
INTENT_INSTRUCTIONS_SENTENCE = (
    " Tools may accept an optional `intent` string; if present, "
    "state in one sentence why you are calling the tool "
    "(never credentials, identifiers or data values)."
)
_INTENT_SCHEMA = {
    "type": "string",
    "description": (
        "Briefly describe the wider task and why you chose this tool, in English. "
        "Omit argument values, personal information, and secrets."
    ),
}
_INTENT_ATTRIBUTES: ContextVar[dict[str, str | bool] | None] = ContextVar(
    "mcp_intent", default=None
)
_TOOL_MODULES: dict[str, str] = {}
_TOOL_ANNOTATIONS: dict[str, dict[str, Any]] = {}
_ARG_PREFIX = "airbyte.mcp"
_LLMOBS_ARG_KEYS = frozenset({"arg_hash_status", "arg_key_scope"})
# Same classification and validation as the OTel backend; intent is exported separately.
_ARG_TRACER = ArgTracer(_ARG_PREFIX, key=_arg_key, skip=(INTENT_ARG,))


def _build_tool_maps() -> None:
    from fastmcp_extensions.annotations import ANNOTATION_MCP_MODULE  # noqa: PLC0415
    from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC0415, PLC2701

    for func, tool_annotations in _REGISTERED_TOOLS:
        name = getattr(func, "__name__", None)
        if name:
            _TOOL_MODULES[name] = str(tool_annotations.get(ANNOTATION_MCP_MODULE, ""))
            _TOOL_ANNOTATIONS[name] = dict(tool_annotations)


def _install_meta_trace_context_middleware(app: FastMCP) -> None:
    from fastmcp.server.low_level import FastMCPServerMiddleware  # noqa: PLC0415

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
    if not isinstance(value, str) or any(
        unicodedata.category(char).startswith("C") for char in value
    ):
        return None
    return value.strip()[:256] or None


def _call_id_digest(request_id: object) -> str:
    return hashlib.sha256(str(request_id).encode()).hexdigest()


def _request_trace_attributes() -> dict[str, str]:
    from fastmcp.server.dependencies import (  # noqa: PLC0415
        get_http_headers,
        get_http_request,
    )

    from airbyte.mcp._telemetry import (  # noqa: PLC0415
        _SESSION_ID_STATE_KEY,
        request_properties,
    )

    attrs: dict[str, str] = {}
    try:
        properties = request_properties()
        for field, key in (
            ("application_name", "application_name"),
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
                session = None
        if isinstance(session, str) and re.fullmatch(r"[0-9a-f]{64}", session):
            attrs["airbyte.mcp.session_id"] = session
    except Exception:
        logger.debug("Request trace attributes unavailable")
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
    error_type = type(error.__cause__ or error).__name__
    return {
        "airbyte.mcp.outcome": "cancelled"
        if isinstance(error, asyncio.CancelledError)
        else "exception",
        "airbyte.mcp.error_type": error_type,
        "error.type": error_type,
    }


class _StripMetaTraceContextMiddleware:
    async def __call__(
        self,
        ctx: ServerRequestContext[Any],
        call_next: Callable[[ServerRequestContext[Any]], Awaitable[Any]],
    ) -> Any:  # noqa: ANN401
        params = getattr(ctx, "params", None)
        meta = params.get("_meta") if isinstance(params, dict) else None
        if isinstance(meta, dict):
            meta.pop("traceparent", None)
            meta.pop("tracestate", None)
        return await call_next(ctx)


class IntentCaptureMiddleware(Middleware):
    def __init__(self, app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
        self._app, self._environ = app, environ
        _install_meta_trace_context_middleware(app)

    async def on_list_tools(
        self,
        context: MiddlewareContext[ListToolsRequest],
        call_next: CallNext[ListToolsRequest, Sequence[Tool]],
    ) -> Sequence[Tool]:
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
        return result

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        attrs: dict[str, str | bool] = {}
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
        attrs: dict[str, str | bool],  # noqa: ARG002
    ) -> ToolResult:
        return await call_next(context)

    @staticmethod
    def _attributes(
        context: MiddlewareContext[CallToolRequestParams],
        intent: object,
    ) -> dict[str, str | bool]:
        from airbyte._util.meta import get_cloud_api_analytic_source  # noqa: PLC0415

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
        attrs.update(_request_trace_attributes())
        if intent:
            attrs["airbyte.mcp.intent"] = intent
        attrs.update(
            {
                f"airbyte.mcp.{key}": value
                for key, value in agent_action_attributes(
                    name, context.message.arguments or {}
                ).items()
            }
        )
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
        return attrs


def redact_tool_span(span: LLMObsSpan) -> LLMObsSpan:
    """Rebuild server tool Input from approved metadata; omit all tool output."""
    from ddtrace.llmobs.types import Message  # noqa: PLC0415

    if span.get_tag("mcp_tool_kind") == "client":
        span.input = [Message(content=REDACTED, role="")]
        span.output = [Message(content=REDACTED, role="")]
        return span
    tool_name = span.get_tag("mcp_tool")
    if tool_name is None:
        return span
    arguments = {
        label: span.metadata[key]
        for label, key in (
            ("intent", "intent"),
            ("action", "agent.action"),
            ("entity_type", "agent.entity_type"),
        )
        if isinstance(span.metadata.get(key), str) and span.metadata[key]
    }
    # Keep the MCP envelope understood by existing deployment processors, but
    # never parse or copy the original request (including unknown future fields).
    request = {"method": "tools/call", "params": {"name": tool_name, "arguments": arguments}}
    span.input = [Message(content=json.dumps(request, sort_keys=True), role="")]
    span.output = [Message(content=REDACTED, role="")]
    return span


def install(app: FastMCP, *, environ: Mapping[str, str] | None = None) -> None:
    """Reuse the deployment's native Datadog tracer instead of installing OTel."""
    import mcp  # noqa: PLC0415

    try:
        from ddtrace.llmobs import LLMObs  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("Native Datadog tracing requires the 'airbyte[datadog]' extra.") from exc
    if RequestsInstrumentor().is_instrumented_by_opentelemetry:  # type: ignore[missing-attribute]
        raise RuntimeError(
            "Native Datadog tracing cannot share active OTel requests instrumentation."
        )
    if getattr(mcp, "__datadog_patch", False):
        raise RuntimeError(
            "Disable Datadog's automatic MCP integration with DD_TRACE_MCP_ENABLED=false "
            "before selecting PyAirbyte's native Datadog backend."
        )
    if not LLMObs.enabled:
        LLMObs.enable(integrations_enabled=False)
    if not LLMObs.enabled:
        raise RuntimeError("Native Datadog tracing requires LLM Observability to be enabled.")
    LLMObs.register_processor(redact_tool_span)
    _build_tool_maps()
    app.add_middleware(_DatadogIntentMiddleware(app, environ=environ))
    # The SDK boundary sees the complete response, including exceptions converted
    # to isError results, and surrounds FastMCP's own seam span.
    from fastmcp.server.low_level import FastMCPServerMiddleware  # noqa: PLC0415

    middleware = app._mcp_server.middleware  # noqa: SLF001
    index = next(
        i for i, item in enumerate(middleware) if isinstance(item, FastMCPServerMiddleware)
    )
    middleware.insert(index, _DatadogRequestMiddleware(environ=environ))
    if _flag(environ, "AIRBYTE_MCP_INTENT_CAPTURE") and (
        INTENT_INSTRUCTIONS_SENTENCE.strip() not in (app.instructions or "")
    ):
        app.instructions = (app.instructions or "") + INTENT_INSTRUCTIONS_SENTENCE


class _DatadogIntentMiddleware(IntentCaptureMiddleware):
    async def _trace_call(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
        attrs: dict[str, str | bool],
    ) -> ToolResult:
        """Reuse common intent/action extraction without creating an OTel span."""
        from ddtrace import tracer  # noqa: PLC0415

        # Native SDK spans describe protocol calls. An in-process nested tool
        # must not overwrite the request span's identity or handled error class.
        nested = _INTENT_ATTRIBUTES.get() is not None
        token = _INTENT_ATTRIBUTES.set(attrs)
        try:
            span = None if nested else tracer.current_span()
        except Exception:
            span = None
        try:
            tool = (
                await self._app.get_tool(context.message.name)
                if span is not None and context.message.name in _TOOL_MODULES
                else None
            )
            if tool is not None:
                attrs.update(
                    arg_trace_attributes(
                        _ARG_TRACER,
                        context.message.name,
                        tool,
                        context.message.arguments,
                        session_digest=attrs.get("airbyte.mcp.session_id"),
                        client_name=attrs.get("airbyte.mcp.client_name"),
                        client_version=attrs.get("airbyte.mcp.client_version"),
                    )
                )
        except Exception:
            logger.debug("Datadog argument tracing unavailable")
        try:
            return await call_next(context)
        except BaseException as exc:
            attrs.update(_exception_attributes(exc))
            raise
        finally:
            _INTENT_ATTRIBUTES.reset(token)
            try:
                from airbyte.mcp._scope import (  # noqa: PLC0415
                    current_call_scope,
                    enrich_call_scope,
                )

                try:
                    if not isinstance(sys.exc_info()[1], asyncio.CancelledError):
                        await enrich_call_scope(context.fastmcp_context)
                except asyncio.CancelledError as exc:
                    attrs.update(_exception_attributes(exc))
                    raise
                finally:
                    if span is not None:
                        scope = current_call_scope()
                        if scope is not None:
                            attrs.update(
                                {
                                    f"airbyte.mcp.{key}": value
                                    for key, value in scope.resolved().to_properties().items()
                                    if value is not None
                                }
                            )
                        _annotate_attributes(span, attrs)
            except Exception:
                logger.debug("Datadog tool attributes unavailable")


def _annotate_attributes(span: Span, source: Mapping[str, str | bool]) -> None:
    """Keep native APM attributes and LLM metadata consistent.

    Argument records pass the OTel backend's export validation: ints become
    metrics, and only the tracing state reaches LLM metadata.
    """
    from ddtrace.llmobs import LLMObs  # noqa: PLC0415

    attrs = dict(source)
    arg_attrs = {key: attrs.pop(key) for key in list(attrs) if is_arg_key(_ARG_PREFIX, key)}
    tool = attrs.get("gen_ai.tool.name")
    accepted = (
        _ARG_TRACER.revalidate(tool, arg_attrs)
        if arg_attrs and isinstance(tool, str) and tool in _TOOL_MODULES
        else {}
    )
    span.set_tags({key: str(value) for key, value in attrs.items()})
    for key, value in accepted.items():
        if isinstance(value, int):
            span.set_metric(key, value)
        else:
            span.set_tag(key, value)
    metadata: dict[str, object] = {
        key.removeprefix("airbyte.mcp."): value
        for key, value in attrs.items()
        if key.startswith("airbyte.mcp.")
    }
    metadata.update(
        {
            short: value
            for key, value in accepted.items()
            if (short := key.removeprefix("airbyte.mcp.")) in _LLMOBS_ARG_KEYS
        }
    )
    if "gen_ai.tool.call.id" in attrs:
        metadata["tool_id"] = attrs["gen_ai.tool.call.id"]
    LLMObs.annotate(span, metadata=metadata)


def _annotate_request(span: Span, ctx: ServerRequestContext[Any]) -> None:
    """Annotate protocol identity without copying tool arguments into the SDK."""
    from ddtrace.llmobs import LLMObs  # noqa: PLC0415

    request_types = {
        "initialize": InitializeRequest,
        "tools/list": ListToolsRequest,
    }
    params = ctx.params if isinstance(ctx.params, dict) else {}
    tool_call = ctx.method == "tools/call"
    name = str(params.get("name", "unknown_tool"))
    tags = {"mcp_method": ctx.method, "integration": "mcp"}
    if tool_call:
        tags.update(mcp_tool=name, mcp_tool_kind="server")
    elif ctx.method == "initialize":
        client = params.get("clientInfo") or {}
        client_version = client.get("version") if isinstance(client, dict) else None
        if isinstance(client, dict) and client.get("name") and client_version:
            tags["client_name"] = str(client["name"])
            tags["client_version"] = f"{client['name']}_{client_version}"
    attrs = _request_trace_attributes()
    if tool_call:
        try:
            from fastmcp.server.dependencies import get_context  # noqa: PLC0415
            from fastmcp.server.middleware import MiddlewareContext  # noqa: PLC0415
            from mcp.types import CallToolRequestParams  # noqa: PLC0415

            from airbyte.mcp._scope import scope_from_request  # noqa: PLC0415

            try:
                fastmcp_context = get_context()
            except RuntimeError:
                fastmcp_context = None
            scope = scope_from_request(
                MiddlewareContext(
                    message=CallToolRequestParams.model_validate(params),
                    fastmcp_context=fastmcp_context,
                )
            )
            attrs.update(
                {
                    f"airbyte.mcp.{key}": value
                    for key, value in scope.to_properties().items()
                    if value is not None
                }
            )
        except Exception:
            logger.debug("Early tool scope unavailable")
    _annotate_attributes(span, attrs)
    session = attrs.get("airbyte.mcp.session_id")
    if session:
        tags["mcp_session_id"] = session
    LLMObs.annotate(
        span,
        tags=tags,
        metadata={"pyairbyte.version": get_version()},
        # Seed scalar payloads so the processor can replace Input even if the
        # tool fails before the metadata middleware runs. Never seed raw data.
        input_data=REDACTED if tool_call else None,
        output_data=REDACTED if tool_call else None,
    )
    if not tool_call:
        request = (
            request_types[ctx.method]
            .model_validate({"method": ctx.method, "params": params})
            .model_dump(exclude={"params": {"meta": "_dd_trace_context"}})
        )
        LLMObs.annotate(span, input_data=request)


class _DatadogRequestMiddleware:
    def __init__(self, *, environ: Mapping[str, str] | None = None) -> None:
        self.distributed_tracing = _env(environ).get(
            "DD_MCP_DISTRIBUTED_TRACING", "true"
        ).lower() in {"true", "1"}

    def _activate_distributed_context(self, params: dict[str, Any]) -> None:
        from ddtrace import tracer  # noqa: PLC0415
        from ddtrace.llmobs import LLMObs  # noqa: PLC0415
        from ddtrace.propagation.http import HTTPPropagator  # noqa: PLC0415

        meta = params.get("_meta")
        headers = meta.get("_dd_trace_context") if isinstance(meta, dict) else None
        if not self.distributed_tracing or not isinstance(headers, dict):
            return
        if not all(
            isinstance(key, str) and isinstance(value, str) for key, value in headers.items()
        ):
            return
        context = HTTPPropagator.extract(headers)
        current = tracer.current_trace_context()
        if context is None or not context.trace_id:
            return
        previous = tracer.context_provider.active()
        try:
            LLMObs.activate_distributed_headers(headers)
        finally:
            # HTTP may already have joined the APM trace without the separate
            # LLM context. Activate both, then keep the nearer HTTP APM parent.
            if current and current.trace_id == context.trace_id:
                tracer.context_provider.activate(previous)

    async def __call__(  # noqa: PLR0912, PLR0915
        self,
        ctx: ServerRequestContext[Any],
        call_next: Callable[[ServerRequestContext[Any]], Awaitable[Any]],
    ) -> Any:  # noqa: ANN401
        from ddtrace import tracer  # noqa: PLC0415
        from ddtrace.llmobs import LLMObs  # noqa: PLC0415

        # Native server instrumentation covered initialize/call. Listing is an
        # explicit addition; other operations retain deployment HTTP tracing.
        if ctx.method not in {"initialize", "tools/list", "tools/call"}:
            return await call_next(ctx)
        span = None
        try:
            previous = tracer.context_provider.active()
        except Exception:
            previous = None
        # Datadog maintains a separate LLM context; its public activation API
        # updates both, but exposes no public restoration API.
        llm_provider = None
        previous_llm = None
        try:
            if LLMObs._instance is not None:  # noqa: SLF001
                llm_provider = LLMObs._instance._llmobs_context_provider  # noqa: SLF001
                previous_llm = llm_provider.active()
        except Exception:
            llm_provider = None
            logger.debug("Datadog LLM context unavailable")
        tool_call = ctx.method == "tools/call"
        try:
            params = ctx.params if isinstance(ctx.params, dict) else {}
            if tool_call and llm_provider is not None:
                try:
                    self._activate_distributed_context(params)
                except Exception:
                    logger.debug("Datadog MCP distributed context unavailable")
            name = str(params.get("name", "unknown_tool")) if tool_call else f"mcp.{ctx.method}"
            start = LLMObs.tool if tool_call else LLMObs.task
            span = start(name=name)
            # LLMObs retains its display name independently of these APM fields.
            span.name = f"mcp.{ctx.method}"
            if tool_call:
                # Per-tool APM trace metrics group by resource; keep it bounded.
                span.resource = name if name in _TOOL_MODULES else "unknown_tool"
            else:
                span.resource = "server_request"
            span.set_metric("_dd.measured", 1)
            _annotate_request(span, ctx)
        except Exception:
            logger.debug("Datadog request attributes unavailable")
        try:
            with suppress_fastmcp_telemetry():
                result = await call_next(ctx)
            if span is not None:
                try:
                    response = {
                        "initialize": InitializeResult,
                        "tools/list": ListToolsResult,
                        "tools/call": CallToolResult,
                    }[ctx.method].model_validate(result)
                    output = REDACTED if tool_call else response.model_dump(mode="json")
                    if tool_call and getattr(response, "is_error", False):
                        span.error = 1
                        error_type = span.get_tag("airbyte.mcp.error_type") or "ToolError"
                        _annotate_attributes(
                            span,
                            {
                                "airbyte.mcp.outcome": span.get_tag("airbyte.mcp.outcome")
                                or "tool_error",
                                "airbyte.mcp.error_type": error_type,
                                "error.type": error_type,
                            },
                        )
                        span.set_tag("error.message", "tool resulted in an error")
                    else:
                        _annotate_attributes(span, {"airbyte.mcp.outcome": "success"})
                    LLMObs.annotate(span, output_data=output)
                except Exception:
                    logger.debug("Datadog response attributes unavailable")
            return result
        finally:
            try:
                if span is not None:
                    # Passing the exception to ddtrace would export its message
                    # and traceback, which can contain arguments and results.
                    if error := sys.exc_info()[1]:
                        span.error = 1
                        _annotate_attributes(span, _exception_attributes(error))
                    span.__exit__(None, None, None)
            except Exception:
                logger.debug("Datadog span completion failed")
            finally:
                try:
                    tracer.context_provider.activate(previous)
                except Exception:
                    logger.debug("Datadog context restoration failed")
                finally:
                    if llm_provider is not None:
                        try:
                            llm_provider.activate(previous_llm)
                        except Exception:
                            logger.debug("Datadog LLM context restoration failed")
