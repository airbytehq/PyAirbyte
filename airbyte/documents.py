"""Methods for converting Airbyte records into documents.

This module is modeled after the LangChain project's `Documents` class:
- https://github.com/langchain-ai/langchain/blob/master/libs/core/langchain_core/documents/base.py
"""
from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel


MAX_SINGLE_LINE_LENGTH = 60

if TYPE_CHECKING:
    import datetime
    from collections.abc import Iterable


def _to_title_case(name: str, /) -> str:
    """Convert a string to title case.

    Unlike Python's built-in `str.title` method, this function doesn't lowercase the rest of the
    string. This is useful for converting "snake_case" to "Title Case" without negatively affecting
    strings that are already in title case or camel case.
    """
    return " ".join(word[0].upper() + word[1:] for word in name.split("_"))


class Document(BaseModel):
    """A PyAirbyte document is a specific projection on top of a record.

    Documents have the following structure:
    - id (str): A unique string identifier for the document.
    - content (str): A string representing the record when rendered as a document.
    - metadata (dict[str, Any]): Associated metadata about the document, such as the record's IDs
      and/or URLs.

    This class is modeled after the LangChain project's `Document` class.

    TODO:
    - Decide if we need to rename 'content' to 'page_content' in order to match LangChain name.
    """

    id: str
    content: str
    metadata: dict[str, Any]
    last_modified: datetime.datetime | None = None

    @classmethod
    def from_records(cls, records: Iterable[dict[str, Any]]) -> Iterable[Document]:
        """Create an iterable of documents from an iterable of records."""
        yield from {cls.from_record(record) for record in records}

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> Document:
        """Create a document from a record.

        TODO:
        - Parse 'id' from primary key records, if available. Otherwise hash the record data.
        - Parse 'last_modified' from the record, when available.
        - Add a convention to let the source define how 'content' should be rendered. In
          that case, the default rendering behavior below would become the fallback.
        - Add smarter logic for deciding which fields are metadata and which are content. In this
          first version, we assume that all string fields are content and all other fields are
          metadata - which doesn't work well for URLs, IDs, and many other field types.
        """
        primary_keys: list[str] = []  # TODO: Get the actual primary keys here.
        document_fields: list[str] = [
            property_name for property_name, value in record.values() if isinstance(value, str)
        ]
        metadata_fields = set(record.keys()) - set(document_fields)
        doc_id: str = (
            "-".join(str(record[key]) for key in primary_keys)
            if primary_keys
            else str(hash(record))
        )
        last_modified_key = None  # TODO: Get the actual last modified key here, when available.

        # Short content is rendered as a single line, while long content is rendered as a indented
        # multi-line string with a 100 character width.
        content = "\n".join(
            f"{_to_title_case(key)}: {value}"
            if len(value) < MAX_SINGLE_LINE_LENGTH
            else f"{_to_title_case(key)}: \n{textwrap.wrap(
                value,
                width=100,
                initial_indent=' ' * 4,
                subsequent_indent=' ' * 4,
                break_long_words=False,
            )}"
            for key, value in record.items()
            if key in document_fields
        )
        return cls(
            id=doc_id,
            content=content,
            metadata={key: record[key] for key in metadata_fields},
            last_modified=record[last_modified_key] if last_modified_key else None,
        )
