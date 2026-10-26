# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Regression coverage for document model construction and rendering."""

from __future__ import annotations

from datetime import datetime as DateTime
from typing import Any

import pytest

from airbyte._util.document_rendering import DocumentRenderer
from airbyte.documents import Document


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"last_modified": None},
        {"last_modified": "2026-10-20T12:00:00Z"},
    ],
)
def test_document_construction(fields: dict[str, Any]) -> None:
    """Documents validate without callers rebuilding the model's forward references."""
    document = Document(content="Example", metadata={"source": "test"}, **fields)
    assert document.page_content == "Example"
    assert document.metadata == {"source": "test"}
    if fields.get("last_modified"):
        assert document.last_modified == DateTime.fromisoformat(
            "2026-10-20T12:00:00+00:00"
        )
    else:
        assert document.last_modified is None


def test_document_rendering() -> None:
    """The record-to-document path constructs the document model successfully."""
    renderer = DocumentRenderer(content_properties=["body"], metadata_properties=["id"])
    document = renderer.render_document({"body": "Example", "id": "record-1"})
    assert document.page_content == "Example"
    assert document.metadata == {"id": "record-1"}


def test_document_schema() -> None:
    """The model's JSON schema resolves the optional timestamp at runtime."""
    schema = Document.model_json_schema()
    assert {"type": "string", "format": "date-time"} in schema["properties"][
        "last_modified"
    ]["anyOf"]
