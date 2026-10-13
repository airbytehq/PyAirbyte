# Contributing

Thank you for your interest in contributing to PyAirbyte!

## Docstring Style

Use **Markdown** formatting in all docstrings — not reStructuredText (reST).

- Use single backticks for inline code: `` `MyClass` ``, not double backticks (` ``MyClass`` `).
- Reference methods as `` `get_column_info()` ``, not `:meth:\`get_column_info\``.
- Use standard Markdown for emphasis, lists, and links.

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

## Agent-facing Cloud Error Messages

Cloud API error text comes from the response's nonempty `detail`, falling back to `title`.
PyAirbyte preserves the selected string; fix poor Cloud wording in the platform.
Only actor-not-ready setup instructions and 403 resource/access guidance are added.
Bodies must be JSON objects of at most 65,536 UTF-8 bytes; invalid or non-problem responses
use a status-only fallback. `message`, `data.message`, and `errorId` are not displayed.
Agent text still passes through credential redaction. Native Datadog uses the selected,
unredacted Cloud message without guidance; this does not add it to LLM Obs metadata.

Non-Cloud MCP trace messages are the first line of the exception class's docstring
(trace-only, exported to Datadog as `error.message`), so keep those first lines fixed text
with no IDs, values or URLs.

## 🚀 Releasing

This project uses [`semantic-pr-release-drafter`](https://github.com/airbytehq/semantic-pr-release-drafter) for automated release management. To release, simply click "`Edit`" on the latest release draft from the [releases page](https://github.com/airbytehq/PyAirbyte/releases), and then click "`Publish release`". This publish operation will trigger all necessary downstream publish operations.

ℹ️ For more detailed instructions, please see the [Releasing Guide](https://github.com/airbytehq/semantic-pr-release-drafter/blob/main/docs/releasing.md).
