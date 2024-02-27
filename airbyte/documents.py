"""Methods for converting Airbyte records into documents."""
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

    id: str | None = None
    content: str
    metadata: dict[str, Any]
    last_modified: datetime.datetime | None = None

    def __str__(self) -> str:
        return self.content

    @property
    def page_content(self) -> str:
        """Return the content of the document.

        This is an alias for the `content` property, and is provided for duck-type compatibility
        with the LangChain project's `Document` class.
        """
        return self.content


class CustomRenderingInstructions(BaseModel):
    """Instructions for rendering a stream's records as documents."""

    title_property: str | None
    content_properties: list[str]
    frontmatter_properties: list[str]
    metadata_properties: list[str]


class DocumentRenderer(BaseModel):
    """Instructions for rendering a stream's records as documents."""

    title_property: str | None
    content_properties: list[str] | None
    metadata_properties: list[str] | None
    render_metadata: bool = False

    # TODO: Add primary key and cursor key support:
    # primary_key_properties: list[str]
    # cursor_property: str | None

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
        if not self.metadata_properties:
            self.metadata_properties = [
                key
                for key in record
                if key not in (self.content_properties or []) and key != self.title_property
            ]
        if self.title_property:
            content += f"# {record[self.title_property]}\n\n"
        if self.render_metadata or not self.content_properties:
            content += "```yaml\n"
            content += yaml.dump({key: record[key] for key in self.metadata_properties})
            content += "```\n"

        # TODO: Add support for primary key and doc ID generation:
        # doc_id: str = (
        #     "-".join(str(record[key]) for key in self.primary_key_properties)
        #     if self.primary_key_properties
        #     else str(hash(record))
        # )

        if not self.content_properties:
            pass
        elif len(self.content_properties) == 1:
            # Only one property to render as content; no need for section headers.
            content += str(record[self.content_properties[0]])
        else:
            # Multiple properties to render as content; use H2 section headers.
            content += "\n".join(
                f"## {_to_title_case(key)}\n\n{record[key]}\n\n" for key in self.content_properties
            )

        return Document(
            # id=doc_id,  # TODD: Add support for primary key and doc ID generation.
            content=content,
            metadata={key: record[key] for key in self.metadata_properties},
        )

    def render_documents(self, records: Iterable[Mapping[str, Any]]) -> Iterable[Document]:
        """Render an iterable of records as documents."""
        yield from (self.render_document(record=record) for record in records)
