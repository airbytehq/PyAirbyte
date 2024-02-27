import airbyte as ab


read_result = ab.get_source(
    "source-github",
    config={
        "repositories": ["airbytehq/quickstarts"],
        "credentials": {"personal_access_token": ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN")},
    },
    streams=["issues"],
).read()

rendering = ab.documents.DocumentRenderer(
    title_property="title",
    cursor_property="updated_at",
    body_properties=["body"],
    metadata_properties=["state"],
    primary_key_properties=["id"],
    render_frontmatter=True,
)

for doc in rendering.render_documents(read_result["issues"]):
    print(str(doc))
    print("-" * 80)
