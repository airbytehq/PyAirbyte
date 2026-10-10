# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `AIRBYTE_MCP_LOG_FORMAT` handling in `airbyte.mcp._logging`."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from typing import Any

import ddtrace  # noqa: F401
import pytest

from airbyte.mcp import _logging, http_main
from airbyte.mcp._logging import (
    LOG_FORMAT_ENV,
    _build_json_formatter,
    configure_logging,
    resolve_log_format,
)


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Restore the logger state mutated by `configure_logging`."""
    root = logging.getLogger()
    logger_names = ("fastmcp", "ddtrace", *_logging._JSON_LOGGER_LEVELS)
    logger_states = {
        name: (
            logging.getLogger(name).handlers[:],
            logging.getLogger(name).level,
            logging.getLogger(name).propagate,
        )
        for name in logger_names
    }
    root_handlers = root.handlers[:]
    root_level = root.level
    yield
    root.handlers[:] = root_handlers
    root.setLevel(root_level)
    for name, (handlers, level, propagate) in logger_states.items():
        logger = logging.getLogger(name)
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate


def _record(**kwargs: Any) -> logging.LogRecord:
    defaults: dict[str, Any] = {
        "name": "airbyte.mcp.test",
        "level": logging.WARNING,
        "pathname": __file__,
        "lineno": 1,
        "msg": "hello %s",
        "args": ("world",),
        "exc_info": None,
    }
    defaults.update(kwargs)
    return logging.LogRecord(**defaults)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, "text", id="unset"),
        pytest.param("", "text", id="empty"),
        pytest.param("  ", "text", id="blank"),
        pytest.param("text", "text", id="text"),
        pytest.param("json", "json", id="json"),
        pytest.param(" JSON ", "json", id="case_and_whitespace"),
    ],
)
def test_resolve_log_format(
    monkeypatch: pytest.MonkeyPatch, value: str | None, expected: str
) -> None:
    if value is None:
        monkeypatch.delenv(LOG_FORMAT_ENV, raising=False)
    else:
        monkeypatch.setenv(LOG_FORMAT_ENV, value)

    assert resolve_log_format() == expected


def test_resolve_log_format_rejects_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LOG_FORMAT_ENV, "jsonl")

    with pytest.raises(ValueError, match="AIRBYTE_MCP_LOG_FORMAT.*'jsonl'.*text, json"):
        resolve_log_format()


def test_json_formatter_renders_standard_fields() -> None:
    record = _record()
    record.__dict__["dd.trace_id"] = "abc123"
    record.__dict__["dd.span_id"] = "456"

    payload = json.loads(_build_json_formatter().format(record))

    assert payload["message"] == "hello world"
    assert payload["severity"] == "WARNING"
    assert payload["levelname"] == payload["severity"]
    assert payload["logger"] == {"name": "airbyte.mcp.test"}
    assert payload["dd.trace_id"] == "abc123"
    assert payload["dd.span_id"] == "456"
    assert "timestamp" in payload
    assert "error" not in payload


def test_json_formatter_keeps_traceback_in_one_line() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        record = _record(
            level=logging.ERROR, msg="failed", args=(), exc_info=sys.exc_info()
        )

    output = _build_json_formatter().format(record)
    payload = json.loads(output)

    assert "\n" not in output
    assert payload["severity"] == "ERROR"
    assert payload["levelname"] == payload["severity"]
    assert payload["error"]["kind"] == "RuntimeError"
    assert payload["error"]["message"] == "boom"
    assert "Traceback" in payload["error"]["stack"]
    assert "exc_info" not in payload


@pytest.mark.usefixtures("restore_logging")
def test_configure_json_logging_routes_everything_to_root(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fastmcp_logger = logging.getLogger("fastmcp")
    fastmcp_logger.addHandler(logging.NullHandler())
    fastmcp_logger.propagate = False

    uvicorn_config = configure_logging("json")
    fastmcp_logger.warning("from fastmcp")
    logging.getLogger("uvicorn.error").info("from uvicorn")

    assert uvicorn_config == {"log_config": None}
    assert fastmcp_logger.handlers == []
    assert fastmcp_logger.propagate is True
    lines = capsys.readouterr().out.splitlines()
    messages = [json.loads(line)["message"] for line in lines]
    assert messages == ["from fastmcp", "from uvicorn"]


@pytest.mark.usefixtures("restore_logging")
def test_configure_json_logging_clears_ddtrace_handler() -> None:
    ddtrace_logger = logging.getLogger("ddtrace")
    ddtrace_logger.addHandler(logging.StreamHandler())
    ddtrace_logger.propagate = False

    configure_logging("json")

    assert ddtrace_logger.handlers == []
    assert ddtrace_logger.propagate is True


@pytest.mark.usefixtures("restore_logging")
def test_quiet_logger_levels_apply_only_in_json_mode() -> None:
    loggers = {name: logging.getLogger(name) for name in _logging._JSON_LOGGER_LEVELS}
    for logger in loggers.values():
        logger.setLevel(logging.DEBUG)

    configure_logging("json")

    assert {
        name: logger.level for name, logger in loggers.items()
    } == _logging._JSON_LOGGER_LEVELS

    for logger in loggers.values():
        logger.setLevel(logging.DEBUG)
    assert configure_logging("text") == {}
    assert all(logger.level == logging.DEBUG for logger in loggers.values())


@pytest.mark.usefixtures("restore_logging")
def test_configure_text_logging_leaves_uvicorn_defaults() -> None:
    assert configure_logging("text") == {}


def test_main_passes_uvicorn_config(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    sentinel = {"log_config": None}
    monkeypatch.setattr(http_main.app, "instructions", http_main.app.instructions)
    monkeypatch.setattr(http_main.app, "middleware", list(http_main.app.middleware))
    monkeypatch.setattr(http_main, "set_hosted_mcp_mode", lambda: None)
    monkeypatch.setattr("airbyte.mcp._otel.install", lambda app: None)
    monkeypatch.setattr(http_main, "configure_logging", lambda log_format: sentinel)
    monkeypatch.setattr(
        http_main, "run_mcp_http_server", lambda app, **kwargs: captured.update(kwargs)
    )

    http_main.main()

    assert captured["uvicorn_config"] is sentinel


def test_main_fails_fast_on_invalid_format(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LOG_FORMAT_ENV, "yaml")
    monkeypatch.setattr(
        _logging,
        "configure_logging",
        lambda log_format: pytest.fail("should not configure"),
    )

    with pytest.raises(ValueError, match="AIRBYTE_MCP_LOG_FORMAT"):
        http_main.main()
