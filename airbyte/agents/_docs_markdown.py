# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Render Agents API docs content blocks as a single Markdown document.

The Agents API (and PyAirbyte's built-in destination docs) express docs bodies as a
list of typed content blocks. MCP tools present them as Markdown instead of exposing
the raw block schema.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable


def _render_heading(block: dict[str, Any]) -> str:
    text = block.get("text")
    if not text:
        return ""
    level = block.get("level") or 2
    return f"{'#' * int(level)} {text}"


def _render_paragraph(block: dict[str, Any]) -> str:
    return str(block.get("text") or "")


def _render_list(block: dict[str, Any]) -> str:
    items = block.get("items") or []
    lines: list[str] = []
    for item in items:
        text = item.get("text") if isinstance(item, dict) else item
        if text is None:
            continue
        lines.append(f"- {text}")
    return "\n".join(lines)


def _render_code(block: dict[str, Any]) -> str:
    code = block.get("code")
    if not code:
        return ""
    return f"```{block.get('language') or ''}\n{code}\n```"


def _render_table(block: dict[str, Any]) -> str:
    headers = block.get("headers") or []
    if not headers:
        return ""
    lines = [
        "| " + " | ".join(str(header) for header in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend(
        "| " + " | ".join(str(cell) for cell in row) + " |" for row in block.get("rows") or []
    )
    return "\n".join(lines)


_BLOCK_RENDERERS: dict[str, Callable[[dict[str, Any]], str]] = {
    "heading": _render_heading,
    "paragraph": _render_paragraph,
    "list": _render_list,
    "code": _render_code,
    "table": _render_table,
}


def _render_block(block: dict[str, Any]) -> str:
    renderer = _BLOCK_RENDERERS.get(block.get("type") or "")
    if renderer is not None:
        return renderer(block)
    text = block.get("text")
    if text:
        return str(text)
    return f"```json\n{json.dumps(block, indent=2)}\n```"


def render_docs_content_markdown(content: list[dict[str, Any]]) -> str:
    """Render Agents API docs content blocks as a single Markdown document."""
    rendered = [_render_block(block) for block in content]
    return "\n\n".join(block for block in rendered if block).rstrip()
