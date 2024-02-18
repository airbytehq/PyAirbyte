"""Methods for converting Airbyte records into documents.

This module is modeled after the LangChain project's `Documents` class:
- https://github.com/langchain-ai/langchain/blob/master/libs/core/langchain_core/documents/base.py

To inform how to render a specific stream's records as documents, this implementation proposes that
sources define a `document_rendering` annotation in their JSON schema. This property would contain
instructions for how to render records as documents, such as which properties to render as content,
which properties to render as metadata, and which properties to render as annotations.

Assuming a stream like GitHub Issues, the `document_rendering` annotation might look like this:
```json
{
    "airbyte_document_rendering": {
        "title_property": "title",
        "content_properties": ["body"],
        "frontmatter_properties": ["url", "author"],
        "metadata_properties": ["id", "created_at", "updated_at", "url"]
    }
}
```

Note that the `airbyte_document_rendering` annotation is optional.
"""
from __future__ import annotations

import textwrap
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import BaseModel


if TYPE_CHECKING:
    import datetime
    from collections.abc import Iterable

    from airbyte_protocol.models import ConfiguredAirbyteStream


MAX_SINGLE_LINE_LENGTH = 60
AIRBYTE_DOCUMENT_RENDERING = "airbyte_document_rendering"
TITLE_PROPERTY = "title_property"
CONTENT_PROPS = "content_properties"
FRONTMATTER_PROPS = "frontmatter_properties"
METADATA_PROPERTIES = "metadata_properties"


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
    def from_records(
        cls,
        records: Iterable[dict[str, Any]],
        stream_metadata: ConfiguredAirbyteStream,
    ) -> Iterable[Document]:
        """Create an iterable of documents from an iterable of records."""
        yield from {
            cls.from_record(record=record, stream_metadata=stream_metadata) for record in records
        }

    @classmethod
    def from_record(
        cls,
        record: dict[str, Any],
        stream_metadata: ConfiguredAirbyteStream,
    ) -> Document:
        """Create a document from a record.

        The document will be rendered as a markdown document, with content, frontmatter, and an
        optional title. If there are multiple properties to render as content, they will be rendered
        beneath H2 section headers. If there is only one property to render as content, it will be
        rendered without a section header. If a title property is specified, it will be rendered as
        an H1 header at the top of the document.

        If metadata properties are not specified, then they will default to those properties which
        are not specified as content, title, or frontmatter properties. Metadata properties are
        not rendered in the document, but are carried with the document in a separate dictionary
        object.

        TODO:
        - Only use cursor_field for 'last_modified' when cursor_field is a timestamp.
        """
        primary_keys: list[str] = []
        all_properties = list(record.keys())

        # TODO: Let the source define how 'content' should be rendered. In that case, the source
        # specifies specific properties to render as properties (at the top of the document) and
        # which properties to render as content (in the body of the document). By default, we assume
        # that properties not defined as properties or as content are metadata, but this may be
        # overridden by the source, for instance in cases of redundancies.
        if AIRBYTE_DOCUMENT_RENDERING in stream_metadata.stream.json_schema:
            render_instructions = stream_metadata.stream.json_schema[AIRBYTE_DOCUMENT_RENDERING]
            if TITLE_PROPERTY in render_instructions:
                title_prop: str | None = render_instructions[TITLE_PROPERTY] or None
            if CONTENT_PROPS in render_instructions:
                content_props: list[str] = render_instructions[CONTENT_PROPS]
            if FRONTMATTER_PROPS in render_instructions:
                frontmatter_props: list[str] = render_instructions[FRONTMATTER_PROPS]
            if METADATA_PROPERTIES in render_instructions:
                metadata_props: list[str] = render_instructions[METADATA_PROPERTIES]
        else:
            title_prop: str | None = None
            frontmatter_props: list[str] = [
                property_name
                for property_name, value in record.items()
                if isinstance(value, str) and len(value) < MAX_SINGLE_LINE_LENGTH
            ]
            content_props: list[str] = [
                property_name
                for property_name in all_properties
                if property_name not in frontmatter_props
            ]
            metadata_props = set(record.keys()) - set(content_props) - set(content_props)

        doc_id: str = (
            "-".join(str(record[key]) for key in stream_metadata.primary_key)
            if primary_keys
            else str(hash(record))
        )
        if stream_metadata.cursor_field:
            last_modified_key = ".".join(stream_metadata.cursor_field)
        last_modified_key = None  # TODO: Get the actual last modified key here, when available.

        content: str = (
            "---\n"
            + yaml.dump(OrderedDict((key, record[key]) for key in frontmatter_props))
            + "---\n"
        )
        if title_prop:
            content += f"# {record[title_prop]}\n\n"

        if len(content_props) > 0:
            pass
        elif len(content_props) == 1:
            # Only one property to render as content; no need for section headers.
            content += "\n".join(
                textwrap.wrap(
                    record[content_props[0]],
                    width=100,
                    break_long_words=False,
                )
            )
        else:
            # Multiple properties to render as content; use H2 section headers.
            content += "\n".join(
                f"## {_to_title_case(key)}\n\n{textwrap.wrap(
                    record[key],
                    width=100,
                    break_long_words=False,
                )}\n"
                for key in content_props
            )
        return cls(
            id=doc_id,
            content=content,
            metadata={key: record[key] for key in metadata_props},
            last_modified=record[last_modified_key] if last_modified_key else None,
        )
