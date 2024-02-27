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

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

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

    def __str__(self) -> str:
        return self.content

    @classmethod
    def from_records(
        cls,
        records: Iterable[Mapping[str, Any]],
        stream_metadata: ConfiguredAirbyteStream,
    ) -> Iterable[Document]:
        """Create an iterable of documents from an iterable of records."""
        yield from {
            cls.from_record(
                record=cast(dict, record),
                stream_metadata=stream_metadata,
            )
            for record in records
        }

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, Any],
        stream_metadata: ConfiguredAirbyteStream,
        render_instructions: DocumentRenderer,
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

        if stream_metadata.cursor_field:
            last_modified_key = ".".join(stream_metadata.cursor_field)
        last_modified_key = None  # TODO: Get the actual last modified key here, when available.

        return cls(
            id=doc_id,
            content=content,
            metadata={key: record[key] for key in metadata_props},
            last_modified=record[last_modified_key] if last_modified_key else None,
        )


class CustomRenderingInstructions(BaseModel):
    """Instructions for rendering a stream's records as documents."""

    title_property: str | None
    content_properties: list[str]
    frontmatter_properties: list[str]
    metadata_properties: list[str]


class DocumentRenderer(BaseModel):
    """Instructions for rendering a stream's records as documents."""

    title_property: str | None
    cursor_property: str | None
    body_properties: list[str]
    metadata_properties: list[str]
    primary_key_properties: list[str]

    render_frontmatter: bool = True

    def render_document(self, record: dict[str, Any]) -> Document:
        """Render a record as a document.

        The document will be rendered as a markdown document, with content, frontmatter, and an
        optional title. If there are multiple properties to render as content, they will be rendered
        beneath H2 section headers. If there is only one property to render as content, it will be
        rendered without a section header. If a title property is specified, it will be rendered as
        an H1 header at the top of the document.

        Returns:
            A tuple of (content: str, metadata: dict).
        """
        content = ""
        if self.render_frontmatter:
            content += "---\n"
            content += "\n".join(
                [yaml.dump({key: record[key]}) for key in self.metadata_properties]
            )
            content += "---\n"
        if self.title_property:
            content += f"# {record[self.title_property]}\n\n"

        doc_id: str = (
            "-".join(str(record[key]) for key in self.primary_key_properties)
            if self.primary_key_properties
            else str(hash(record))
        )

        if len(self.body_properties) == 0:
            pass
        elif len(self.body_properties) == 1:
            # Only one property to render as content; no need for section headers.
            content += str(record[self.body_properties[0]])
        else:
            # Multiple properties to render as content; use H2 section headers.
            content += "\n".join(
                f"## {_to_title_case(key)}\n\n{record[key]}\n\n" for key in self.body_properties
            )

        return Document(
            id=doc_id,
            content=content,
            metadata={key: record[key] for key in self.metadata_properties},
        )

    def render_documents(self, records: Iterable[dict[str, Any]]) -> Iterable[Document]:
        """Render an iterable of records as documents."""
        yield from (self.render_document(record=record) for record in records)


class DocumentAutoRenderer(BaseModel):
    """Automatically render a stream's records as documents.

    This class is a convenience class for automatically rendering a stream's records as documents.
    It is a thin wrapper around the `DocumentRenderer` class, and is intended to be used when the
    source does not provide a `document_rendering` annotation in its JSON schema."""

    def __init__(self, stream_metadata: ConfiguredAirbyteStream) -> None:
        """Create a new DocumentAutoRenderer."""

        render_instructions: dict | None = None
        title_prop: str | None = None
        content_props: list[str] = []
        metadata_props: list[str] = []

        if AIRBYTE_DOCUMENT_RENDERING in stream_metadata.stream.json_schema:
            render_instructions = stream_metadata.stream.json_schema[AIRBYTE_DOCUMENT_RENDERING]
            if TITLE_PROPERTY in render_instructions:
                title_prop: str | None = render_instructions[TITLE_PROPERTY] or None
            if CONTENT_PROPS in render_instructions:
                content_props: list[str] = render_instructions[CONTENT_PROPS]
            if METADATA_PROPERTIES in render_instructions:
                metadata_props: list[str] = render_instructions[METADATA_PROPERTIES]

        if stream_metadata.cursor_field:
            cursor_prop: str | None = ".".join(stream_metadata.cursor_field)

        super().__init__(
            title_property=title_prop,
            cursor_property=cursor_prop,
            body_properties=[
                key
                for key, value in stream_metadata.json_schema.get("properties", {}).items()
                if value.get("type") == "string"
            ],
            metadata_properties=[
                key
                for key, value in stream_metadata.json_schema.get("properties", {}).items()
                if value.get("type") != "string"
            ],
            primary_key_properties=stream_metadata.primary_key,
        )
