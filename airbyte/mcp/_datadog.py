# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Native Datadog MCP spans without raw tool payloads."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import TYPE_CHECKING, Any

from fastmcp.telemetry import suppress_fastmcp_telemetry
from mcp.types import (
    CallToolResult,
    InitializeRequest,
    InitializeResult,
    ListToolsRequest,
    ListToolsResult,
)
from opentelemetry.instrumentation.requests import RequestsInstrumentor

from airbyte.mcp import _arg_trace
from airbyte.mcp._otel import (
    _INTENT_ATTRIBUTES,
    _TOOL_ARG_CLASSES,
    INTENT_INSTRUCTIONS_SENTENCE,
    IntentCaptureMiddleware,
    _build_tool_maps,
    _env,
    _exception_attributes,
    _flag,
    _request_trace_attributes,
    result_error_like_attributes,
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
    _build_tool_maps(environ)
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
        attrs: dict[str, str | bool | int],
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
            result = await call_next(context)
        except BaseException as exc:
            attrs.update(_exception_attributes(exc))
            raise
        else:
            try:
                attrs.update(result_error_like_attributes(context, result))
            except Exception:
                logger.debug("Result classification skipped")
            return result
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


def _annotate_attributes(span: Span, attrs: Mapping[str, str | bool | int]) -> None:
    """Keep native APM attributes and LLM metadata consistent.

    Argument-tracing keys pass the same validator as the OTLP exporter: ints become
    metrics, bools lowercase tags, and only `LLMOBS_ALLOWED` keys reach LLM metadata.
    """
    from ddtrace.llmobs import LLMObs  # noqa: PLC0415

    attrs = dict(attrs)
    _arg_trace.merge_tool_flats(attrs)
    arg_attrs = {key: attrs.pop(key) for key in list(attrs) if _arg_trace.is_new_key(key)}
    tool = attrs.get("gen_ai.tool.name")
    accepted: dict[str, str | bool | int] = {}
    dropped = 0
    if isinstance(tool, str) and tool in _TOOL_ARG_CLASSES:
        accepted, dropped = _arg_trace.validate(tool, arg_attrs, _TOOL_ARG_CLASSES)
    span.set_tags({key: str(value) for key, value in attrs.items()})
    for key, value in accepted.items():
        if isinstance(value, bool):
            span.set_tag(key, "true" if value else "false")
        elif isinstance(value, int):
            span.set_metric(key, value)
        else:
            span.set_tag(key, value)
    if dropped:
        span.set_metric(_arg_trace.DROPPED_KEY, dropped)
    metadata: dict[str, object] = {
        key.removeprefix("airbyte.mcp."): value
        for key, value in attrs.items()
        if key.startswith("airbyte.mcp.")
    }
    metadata.update(
        {
            short: value
            for key, value in accepted.items()
            if (short := key.removeprefix("airbyte.mcp.")) in _arg_trace.LLMOBS_ALLOWED
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
        if isinstance(client, dict) and client.get("name") and client.get("version"):
            tags.update(
                client_name=str(client["name"]),
                client_version=f"{client['name']}_{client['version']}",
            )
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
