# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Process-wide log output configuration for the hosted MCP HTTP entrypoint.

`AIRBYTE_MCP_LOG_FORMAT` selects the format:

- `text` (default): leaves the existing console logging untouched. Importing
  `airbyte_cdk` already configures the root logger (AirbyteMessage-shaped
  lines on stdout), fastmcp writes Rich console output to stderr, and uvicorn
  uses its own text handlers. Intended for local runs.
- `json`: one JSON object per line on stdout for every logger, including
  fastmcp and uvicorn. Intended for log collectors such as Datadog
  `serverless-init` and Cloud Logging.

Values are case-insensitive; blank is treated as unset. Any other value fails
startup rather than silently falling back to text.
"""

from __future__ import annotations

import logging
import os
import sys
import traceback
from typing import TYPE_CHECKING, Any, Literal

import structlog


if TYPE_CHECKING:
    from structlog.typing import EventDict, WrappedLogger


LOG_FORMAT_ENV = "AIRBYTE_MCP_LOG_FORMAT"

LogFormat = Literal["text", "json"]
LOG_FORMATS: tuple[LogFormat, ...] = ("text", "json")

# Loggers that install their own handlers with `propagate = False`, which would
# bypass the root JSON handler.
_SELF_HANDLED_LOGGERS = ("fastmcp", "ddtrace")

_JSON_LOGGER_LEVELS = {
    "mcp.server.streamable_http": logging.WARNING,
    "ddtrace.contrib.internal.grpc.aio_client_interceptor": logging.ERROR,
    "ddtrace.llmobs._llmobs": logging.ERROR,
}


def resolve_log_format() -> LogFormat:
    """Return the log format selected by `AIRBYTE_MCP_LOG_FORMAT`."""
    value = os.getenv(LOG_FORMAT_ENV, "").strip().lower() or "text"
    for log_format in LOG_FORMATS:
        if value == log_format:
            return log_format
    msg = f"Invalid `{LOG_FORMAT_ENV}` value {value!r}; expected one of: {', '.join(LOG_FORMATS)}."
    raise ValueError(msg)


def _add_severity(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Add stdlib level names for Datadog and Cloud Logging.

    Datadog's Python pipeline sets status from `levelname`, and Cloud Logging
    reads `severity`.
    """
    levelname = event_dict["_record"].levelname
    event_dict["levelname"] = levelname
    event_dict["severity"] = levelname
    return event_dict


def _add_logger_name(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Add the logger name under Datadog's standard `logger.name` attribute."""
    event_dict["logger"] = {"name": event_dict["_record"].name}
    return event_dict


def _drop_color_message(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Drop uvicorn's `color_message` extra, an ANSI-colored copy of the message."""
    event_dict.pop("color_message", None)
    return event_dict


def _add_error(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Move exception info into Datadog's standard `error.*` attributes.

    Keeping the traceback inside the event means a raised exception is one log
    entry rather than one entry per traceback line.
    """
    exc_info = event_dict.pop("exc_info", None)
    if exc_info:
        exc_type, exc_value, exc_tb = exc_info
        event_dict["error"] = {
            "kind": exc_type.__name__,
            "message": str(exc_value),
            "stack": "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
        }
    stack_info = event_dict.pop("stack_info", None)
    if stack_info:
        event_dict["stack_info"] = stack_info
    return event_dict


def _build_json_formatter() -> logging.Formatter:
    """Build a formatter that renders every stdlib record as one JSON line.

    `ExtraAdder` copies non-standard record attributes into the event, which
    carries ddtrace's `dd.trace_id` / `dd.span_id` / `dd.service` / `dd.env` /
    `dd.version` (added when `DD_LOGS_INJECTION` is on) and any `extra=` fields.
    """
    return structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_severity,
            _add_logger_name,
            structlog.stdlib.ExtraAdder(),
            _drop_color_message,
        ],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            _add_error,
            structlog.processors.EventRenamer("message"),
            structlog.processors.JSONRenderer(),
        ],
    )


def configure_logging(log_format: LogFormat) -> dict[str, Any]:
    """Configure process-wide logging and return matching uvicorn overrides.

    The returned mapping is passed as `uvicorn_config` to the HTTP server. For
    `json` it sets `log_config=None`, so uvicorn installs no handlers of its own
    and its loggers propagate to the root JSON handler.
    """
    if log_format == "text":
        # A no-op once `airbyte_cdk` has configured the root logger on import;
        # kept as the fallback when nothing has.
        logging.basicConfig(level=logging.INFO)
        return {}

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_build_json_formatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)

    for name in _SELF_HANDLED_LOGGERS:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True

    for name, level in _JSON_LOGGER_LEVELS.items():
        logging.getLogger(name).setLevel(level)

    return {"log_config": None}
