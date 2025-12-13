## 👋 Greetings, Airbyte Team Member!

Here are some helpful tips and reminders for your convenience.

### Testing This PyAirbyte Version

You can test this version of PyAirbyte using the following:

```bash
# Run PyAirbyte CLI from this branch:
uvx --from 'git+https://github.com/airbytehq/PyAirbyte.git@{{ .branch_name }}' pyairbyte --help

# Install PyAirbyte from this branch for development:
pip install 'git+https://github.com/airbytehq/PyAirbyte.git@{{ .branch_name }}'
```

### Helpful Resources

- [PyAirbyte Documentation](https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started)
- [API Reference](https://airbytehq.github.io/PyAirbyte/)

### PR Slash Commands

Airbyte Maintainers can execute the following slash commands on your PR:

- `/fix-pr` - Fixes most formatting and linting issues
- `/poetry-lock` - Updates poetry.lock file
- `/test-pr` - Runs tests with the updated PyAirbyte
- `/prerelease` - Builds and publishes a prerelease version to TestPyPI

### Community Support

Questions? Join the [#pyairbyte channel](https://airbytehq.slack.com/archives/C06FZ238P8W) in our Slack workspace.

[📝 _Edit this welcome message._](https://github.com/airbytehq/PyAirbyte/blob/main/.github/pr-welcome-internal.md)
