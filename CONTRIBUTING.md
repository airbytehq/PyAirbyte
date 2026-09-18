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
unit-testable on its own. An MCP tool should only:

- Parse and validate its inputs.
- Call one or more core methods.
- Convert the result into the tool's Pydantic response model (including mapping expected
  failures to "unknown" values where the tool contract calls for it).

If you find yourself writing a helper in `airbyte/mcp/` that talks to an API or encodes
domain rules, move it to the core module and call it from the tool instead.

## 🚀 Releasing

This project uses [`semantic-pr-release-drafter`](https://github.com/aaronsteers/semantic-pr-release-drafter) for automated release management. To release, simply click "`Edit`" on the latest release draft from the [releases page](https://github.com/airbytehq/PyAirbyte/releases), and then click "`Publish release`". This publish operation will trigger all necessary downstream publish operations.

ℹ️ For more detailed instructions, please see the [Releasing Guide](https://github.com/aaronsteers/semantic-pr-release-drafter/blob/main/docs/releasing.md).
