# Contributing

Thank you for your interest in contributing to PyAirbyte!

## Docstring Style

Use **Markdown** formatting in all docstrings — not reStructuredText (reST).

- Use single backticks for inline code: `` `MyClass` ``, not double backticks (` ``MyClass`` `).
- Reference methods as `` `get_column_info()` ``, not `:meth:\`get_column_info\``.
- Use standard Markdown for emphasis, lists, and links.
- The `Python Docstring Format Check` CI job validates this style; run `poe lint-docstrings` locally or invoke the standalone `./scripts/check_docstring_markdown.uv` script with only uv installed.

## MCP Tools Are a Presentation Layer

Modules under `airbyte/mcp/` are thin wrappers over the core library. Business logic — API
calls, status derivation, cross-referencing between Cloud and Agents data — belongs on the
public core classes (for example `airbyte.cloud.CloudWorkspace`), where it is reusable and
unit-testable on its own.

If you find yourself writing a helper in `airbyte/mcp/` that talks to an API or encodes
domain rules, move it to the most appropriate core module or util module and create the
necessary public Python interface first.

As a general rule (with rare exceptions), there shouldn't be anything that you can do through
the MCP tools which you couldn't also do with the public Python interface. (Hence the framing
as a "presentation" layer on top of the core modules.)

## MCP Tool Titles and Annotations

Every MCP tool must have a `title` and must declare explicit `read_only`, `destructive`, and
`open_world` values on `@mcp_tool(...)`. This is a requirement for listing the MCP server in client
marketplaces such as the ChatGPT app directory, and clients use these values to decide when to
ask the user for confirmation. Set them from what the tool actually does, not from what the
wrapper defaults to.

- `title`: a short, human-readable, verb-first action. By default it is derived from the tool name
  in sentence case (`rename_connection` becomes `Rename connection`), so name tools accordingly.
  Pass `title=` only when the derived title reads poorly, and add a short inline comment giving
  the reason, for example `title="Run local SQL query",  # Use capital 'SQL' in tool title`.
  Titles are display text only and must not tell the model when or how to use this or any other
  tool. Don't include the product or server name, because clients already show it. Qualify the
  noun only to tell apart tools on the same server (for example `Search the web` vs.
  `Search Drive files`).
- `read_only`: `True` only when the tool cannot change any state. Starting jobs, queuing work, and
  persisting artifacts are not read-only.
- `destructive`: for non-read-only tools, `True` when the tool may delete, overwrite, cancel, or
  otherwise make a non-additive change. Use `False` only for purely additive writes. Being able
  to undo a change does not by itself make it non-destructive.
- `open_world`: `True` when the tool reaches the public internet or open-ended external entities.
  A tool confined to the user's own account or workspace may use `False`, even when the service
  is hosted remotely.

Authoritative references:

- MCP [`ToolAnnotations`](https://modelcontextprotocol.io/specification/2025-11-25/schema#toolannotations)
  and [`BaseMetadata.title`](https://modelcontextprotocol.io/specification/2025-11-25/schema#basemetadata)
- OpenAI app submission guidelines, ["Correct annotation"](https://developers.openai.com/apps-sdk/app-submission-guidelines)
- OpenAI [Build your MCP server](https://developers.openai.com/apps-sdk/build/mcp-server)
  and [Plan tools](https://developers.openai.com/apps-sdk/plan/tools)

## Agent-facing Cloud Error Messages

Cloud API error text comes from the response's nonempty `detail`, falling back to `title`.
PyAirbyte preserves the selected string; fix poor Cloud wording in the platform.
Only actor-not-ready setup instructions, 403 resource/access guidance, and sync-start
409 recovery guidance are added. Sync-start 409s append narrowly typed disabled-connection or active-job recovery
guidance; other local messages and guidance never override the selected Cloud text.
Bodies must be JSON objects of at most 65,536 UTF-8 bytes; invalid or non-problem responses
use a status-only fallback. `message`, `data.message`, and `errorId` are not displayed.
Agent errors, caught-error billing messages, and connector warnings pass through credential
redaction. Native Datadog uses the selected,
unredacted Cloud message without guidance; this does not add it to LLM Obs metadata.

Non-Cloud MCP trace messages are the first line of the exception class's docstring
(trace-only, exported to Datadog as `error.message`), so keep those first lines fixed text
with no IDs, values or URLs.

## 🚀 Releasing

This project uses [`semantic-pr-release-drafter`](https://github.com/airbytehq/semantic-pr-release-drafter) for automated release management. To release, simply click "`Edit`" on the latest release draft from the [releases page](https://github.com/airbytehq/PyAirbyte/releases), and then click "`Publish release`". This publish operation will trigger all necessary downstream publish operations.

ℹ️ For more detailed instructions, please see the [Releasing Guide](https://github.com/airbytehq/semantic-pr-release-drafter/blob/main/docs/releasing.md).
