# Contributing

Thank you for your interest in contributing to PyAirbyte!

## Docstring Style

Use **Markdown** formatting in all docstrings — not reStructuredText (reST).

- Use single backticks for inline code: `` `MyClass` ``, not double backticks (` ``MyClass`` `).
- Reference methods as `` `get_column_info()` ``, not `:meth:\`get_column_info\``.
- Use standard Markdown for emphasis, lists, and links.

## 🚀 Releasing

This project uses [`semantic-pr-release-drafter`](https://github.com/aaronsteers/semantic-pr-release-drafter) for automated release management. To release, simply click "`Edit`" on the latest release draft from the [releases page](https://github.com/airbytehq/PyAirbyte/releases), and then click "`Publish release`". This publish operation will trigger all necessary downstream publish operations.

ℹ️ For more detailed instructions, please see the [Releasing Guide](https://github.com/aaronsteers/semantic-pr-release-drafter/blob/main/docs/releasing.md).
