"""This examples script demonstrates how to render documents from a source."""
from __future__ import annotations

import rich

import airbyte as ab


read_result = ab.get_source(
    "source-github",
    config={
        "repositories": ["airbytehq/quickstarts"],
        "credentials": {"personal_access_token": ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN")},
    },
    streams=["issues"],
).read()

for doc in read_result["issues"].to_documents(
    title_property="title",
    content_properties=["body"],
    metadata_properties=["state", "url", "number"],
    # primary_key_properties=["id"],
    # cursor_property="updated_at",
    render_metadata=True,
):
    rich.print(rich.markdown.Markdown(str(doc) + "\n\n" + str("-" * 40)))
