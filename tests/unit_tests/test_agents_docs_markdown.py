# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `airbyte.agents._docs_markdown`."""

from __future__ import annotations

import json

from airbyte.agents._docs_markdown import render_docs_content_markdown


def test_heading_defaults_to_level_two() -> None:
    assert (
        render_docs_content_markdown([{"type": "heading", "text": "Title"}])
        == "## Title"
    )


def test_heading_uses_explicit_level() -> None:
    assert (
        render_docs_content_markdown([{"type": "heading", "level": 3, "text": "Sub"}])
        == "### Sub"
    )


def test_paragraph_renders_text() -> None:
    assert (
        render_docs_content_markdown([{"type": "paragraph", "text": "Hello"}])
        == "Hello"
    )


def test_list_renders_string_items() -> None:
    assert (
        render_docs_content_markdown([{"type": "list", "items": ["a", "b"]}])
        == "- a\n- b"
    )


def test_list_renders_dict_items_via_text() -> None:
    assert (
        render_docs_content_markdown([
            {"type": "list", "items": [{"text": "a"}, {"text": "b"}]}
        ])
        == "- a\n- b"
    )


def test_code_renders_fenced_block_with_language() -> None:
    assert (
        render_docs_content_markdown([
            {"type": "code", "language": "sql", "code": "SELECT 1"}
        ])
        == "```sql\nSELECT 1\n```"
    )


def test_code_renders_fenced_block_without_language() -> None:
    assert (
        render_docs_content_markdown([{"type": "code", "code": "SELECT 1"}])
        == "```\nSELECT 1\n```"
    )


def test_table_renders_pipe_table() -> None:
    block = {
        "type": "table",
        "headers": ["name", "count"],
        "rows": [["issues", 3], ["pulls", 1]],
    }
    assert render_docs_content_markdown([block]) == (
        "| name | count |\n| --- | --- |\n| issues | 3 |\n| pulls | 1 |"
    )


def test_unknown_block_with_text_renders_text() -> None:
    assert (
        render_docs_content_markdown([{"type": "callout", "text": "Watch out"}])
        == "Watch out"
    )


def test_unknown_block_without_text_renders_json_fence() -> None:
    block = {"type": "widget", "config": {"size": 2}}
    expected = f"```json\n{json.dumps(block, indent=2)}\n```"
    assert render_docs_content_markdown([block]) == expected


def test_empty_content_renders_empty_string() -> None:
    assert render_docs_content_markdown([]) == ""


def test_empty_blocks_are_skipped_and_blocks_joined() -> None:
    content = [
        {"type": "paragraph", "text": None},
        {"type": "heading", "text": "Title"},
        {"type": "paragraph", "text": "Body"},
    ]
    assert render_docs_content_markdown(content) == "## Title\n\nBody"
