# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `airbyte.agents._docs_markdown`."""

from __future__ import annotations

import json
from typing import Any

import pytest

from airbyte.agents._docs_markdown import render_docs_content_markdown


@pytest.mark.parametrize(
    ("block", "expected"),
    [
        pytest.param(
            {"type": "heading", "text": "Title"}, "## Title", id="heading-default-level"
        ),
        pytest.param(
            {"type": "heading", "level": 3, "text": "Sub"},
            "### Sub",
            id="heading-level-3",
        ),
        pytest.param({"type": "paragraph", "text": "Hello"}, "Hello", id="paragraph"),
        pytest.param(
            {"type": "list", "items": ["a", "b"]}, "- a\n- b", id="list-strings"
        ),
        pytest.param(
            {"type": "list", "items": [{"text": "a"}, {"text": "b"}]},
            "- a\n- b",
            id="list-dict-items",
        ),
        pytest.param(
            {"type": "code", "language": "sql", "code": "SELECT 1"},
            "```sql\nSELECT 1\n```",
            id="code-with-language",
        ),
        pytest.param(
            {"type": "code", "code": "SELECT 1"},
            "```\nSELECT 1\n```",
            id="code-no-language",
        ),
        pytest.param(
            {
                "type": "table",
                "headers": ["name", "count"],
                "rows": [["issues", 3], ["pulls", 1]],
            },
            "| name | count |\n| --- | --- |\n| issues | 3 |\n| pulls | 1 |",
            id="table",
        ),
        pytest.param(
            {"type": "callout", "text": "Watch out"},
            "Watch out",
            id="unknown-with-text",
        ),
        pytest.param(
            {"type": "widget", "config": {"size": 2}},
            f"```json\n{json.dumps({'type': 'widget', 'config': {'size': 2}}, indent=2)}\n```",
            id="unknown-without-text-json-fence",
        ),
        pytest.param({"type": "paragraph", "text": None}, "", id="empty-payload"),
    ],
)
def test_render_block(block: dict[str, Any], expected: str) -> None:
    """Each content block type renders to its expected Markdown."""
    assert render_docs_content_markdown([block]) == expected


def test_render_document() -> None:
    """Blocks join with blank lines, empty blocks are skipped, and [] renders ""."""
    content = [
        {"type": "paragraph", "text": None},
        {"type": "heading", "text": "Title"},
        {"type": "paragraph", "text": "Body"},
    ]
    assert render_docs_content_markdown(content) == "## Title\n\nBody"
    assert render_docs_content_markdown([]) == ""
