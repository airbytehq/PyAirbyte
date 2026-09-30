# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Opt-in native Datadog MCP spans with the hosted platform's payload policy."""

from __future__ import annotations

import json
import logging
import sys
from typing import TYPE_CHECKING, Any

from fastmcp.telemetry import suppress_fastmcp_telemetry
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    InitializeRequest,
    InitializeResult,
    ListToolsRequest,
    ListToolsResult,
)
from opentelemetry.instrumentation.requests import RequestsInstrumentor

from airbyte.mcp._otel import (
    _INTENT_ATTRIBUTES,
    INTENT_INSTRUCTIONS_SENTENCE,
    IntentCaptureMiddleware,
    _build_tool_maps,
    _flag,
)
from airbyte.version import get_version


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from ddtrace.llmobs import LLMObsSpan
    from ddtrace.trace import Span
    from fastmcp import FastMCP
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import ToolResult
    from mcp.server.context import ServerRequestContext
    from mcp.types import CallToolRequestParams

logger = logging.getLogger(__name__)
REDACTED = "[REDACTED]"
SENSITIVE_ARGUMENTS = frozenset({"config", "testing_values", "api_args"})
SENSITIVE_OUTPUT_TOOLS = frozenset(
    {
        "execute_agent_connector",
        "execute_agent_connector_ro",
        "get_cloud_sync_logs",
        "get_connection_artifact",
        "get_stream_previews",
        "read_source_stream_records",
        "run_sql_query",
    }
)


def redact_tool_span(span: LLMObsSpan) -> LLMObsSpan:
    """Apply the platform's selected-field policy, including outbound MCP calls."""
    from ddtrace.llmobs.types import Message  # noqa: PLC0415

    if span.get_tag("mcp_tool_kind") == "client":
        span.input = [Message(content=REDACTED, role="")]
        span.output = [Message(content=REDACTED, role="")]
        return span
    tool_name = span.get_tag("mcp_tool")
    if tool_name is None:
        return span
    try:
        messages = []
        for message in span.input:
            request = json.loads(message.get("content", ""))
            params = request.get("params") if isinstance(request, dict) else None
            arguments = params.get("arguments") if isinstance(params, dict) else None
            if isinstance(arguments, dict):
                for name in SENSITIVE_ARGUMENTS & arguments.keys():
                    arguments[name] = REDACTED
            clean_message = message.copy()
            clean_message["content"] = json.dumps(request, sort_keys=True)
            messages.append(clean_message)
        span.input = messages
    except Exception:
        logger.warning("Could not parse an MCP request; dropping its tracing input.")
        span.input = [Message(content=REDACTED, role="")]
    if tool_name in SENSITIVE_OUTPUT_TOOLS:
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
    middleware.insert(index, _DatadogRequestMiddleware())
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
        from ddtrace.llmobs import LLMObs  # noqa: PLC0415

        # Native SDK spans describe protocol calls. An in-process nested tool
        # must not overwrite the request span's identity or handled error class.
        if _INTENT_ATTRIBUTES.get() is not None:
            return await call_next(context)
        token = _INTENT_ATTRIBUTES.set(attrs)
        try:
            span = tracer.current_span()
        except Exception:
            span = None
        try:
            return await call_next(context)
        except Exception as exc:
            attrs["airbyte.mcp.error_type"] = type(exc.__cause__ or exc).__name__
            raise
        finally:
            _INTENT_ATTRIBUTES.reset(token)
            if span is not None:
                try:
                    from airbyte.mcp._scope import current_call_scope  # noqa: PLC0415

                    scope = current_call_scope()
                    if scope and scope.workspace_source == "default" and scope.workspace_id:
                        attrs["airbyte.mcp.workspace_id"] = scope.workspace_id
                        attrs["airbyte.mcp.scope_source"] = "default"
                    span.set_tags({key: str(value) for key, value in attrs.items()})
                    LLMObs.annotate(
                        span,
                        metadata={
                            **{
                                key.removeprefix("airbyte.mcp."): value
                                for key, value in attrs.items()
                                if key.startswith("airbyte.mcp.")
                            },
                            **(
                                {"tool_id": attrs["gen_ai.tool.call.id"]}
                                if "gen_ai.tool.call.id" in attrs
                                else {}
                            ),
                        },
                    )
                except Exception:
                    logger.debug("Datadog tool attributes unavailable")


def _annotate_request(span: Span, ctx: ServerRequestContext[Any]) -> None:
    """Serialize the same typed request envelope as Datadog's MCP integration."""
    from ddtrace.llmobs import LLMObs  # noqa: PLC0415
    from fastmcp.server.dependencies import get_http_headers  # noqa: PLC0415

    request_types = {
        "initialize": InitializeRequest,
        "tools/list": ListToolsRequest,
        "tools/call": CallToolRequest,
    }
    params = ctx.params if isinstance(ctx.params, dict) else {}
    tool_call = ctx.method == "tools/call"
    name = str(params.get("name", "unknown_tool"))
    tags = {"mcp_method": ctx.method, "integration": "mcp"}
    if tool_call:
        tags.update(mcp_tool=name, mcp_tool_kind="server")
    elif ctx.method == "initialize":
        client = params.get("clientInfo") or {}
        if isinstance(client, dict) and client.get("name") and client.get("version"):
            tags.update(
                client_name=str(client["name"]),
                client_version=f"{client['name']}_{client['version']}",
            )
    session = get_http_headers(include={"mcp-session-id"}).get("mcp-session-id")
    if session:
        tags["mcp_session_id"] = session
    LLMObs.annotate(span, tags=tags, metadata={"pyairbyte.version": get_version()})
    # Match native SDK serialization, including defaults and model field
    # names, rather than constructing a different wire-shaped envelope.
    request = (
        request_types[ctx.method]
        .model_validate({"method": ctx.method, "params": params})
        .model_dump(exclude={"params": {"meta": "_dd_trace_context"}})
    )
    arguments = (request.get("params") or {}).get("arguments")
    if isinstance(arguments, dict):
        if isinstance(arguments.get("telemetry"), dict):
            arguments.pop("telemetry", None)
        for key in SENSITIVE_ARGUMENTS & arguments.keys():
            arguments[key] = REDACTED
    LLMObs.annotate(span, input_data=request)


class _DatadogRequestMiddleware:
    async def __call__(
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
        tool_call = ctx.method == "tools/call"
        name = "unknown_tool"
        try:
            params = ctx.params if isinstance(ctx.params, dict) else {}
            name = str(params.get("name", "unknown_tool")) if tool_call else f"mcp.{ctx.method}"
            start = LLMObs.tool if tool_call else LLMObs.task
            span = start(name=name)
            # LLMObs retains its display name independently of these APM fields.
            span.name = f"mcp.{ctx.method}"
            span.resource = "server_tool_call" if tool_call else "server_request"
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
                    output = response.model_dump(mode="json")
                    if tool_call and getattr(response, "is_error", False):
                        span.error = 1
                        span.set_tag("error.type", "ToolError")
                        span.set_tag("error.message", "tool resulted in an error")
                    # Redact before annotation too: a failed tag annotation must
                    # never bypass the selected output policy in the processor.
                    if tool_call and name in SENSITIVE_OUTPUT_TOOLS:
                        output = REDACTED
                    LLMObs.annotate(span, output_data=output)
                except Exception:
                    logger.debug("Datadog response attributes unavailable")
            return result
        finally:
            try:
                if span is not None:
                    span.__exit__(*sys.exc_info())
            except Exception:
                logger.debug("Datadog span completion failed")
            finally:
                try:
                    tracer.context_provider.activate(previous)
                except Exception:
                    logger.debug("Datadog context restoration failed")
