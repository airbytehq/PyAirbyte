## 👋 Welcome to PyAirbyte!

Thank you for your contribution from **{{ .repo_name }}**! We're excited to have you in the Airbyte community.

### Testing This PyAirbyte Version

You can test this version of PyAirbyte using the following:

```bash
# Run PyAirbyte CLI from this branch:
uvx --from 'git+https://github.com/{{ .repo_name }}.git@{{ .branch_name }}' pyairbyte --help

# Install PyAirbyte from this branch for development:
pip install 'git+https://github.com/{{ .repo_name }}.git@{{ .branch_name }}'
```

### Helpful Resources

- [Contributing Guidelines](https://github.com/airbytehq/PyAirbyte/blob/main/docs/CONTRIBUTING.md)
- [PyAirbyte Documentation](https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started)
- [API Reference](https://airbytehq.github.io/PyAirbyte/)

### PR Slash Commands

As needed or by request, Airbyte Maintainers can execute the following slash commands on your PR:

- `/fix-pr` - Fixes most formatting and linting issues
- `/poetry-lock` - Updates poetry.lock file
- `/test-pr` - Runs tests with the updated PyAirbyte

### Community Support

If you have any questions, feel free to ask in the PR comments or join our community:
- [Airbyte Slack](https://airbytehq.slack.com/) - Join the [#pyairbyte channel](https://airbytehq.slack.com/archives/C06FZ238P8W)
- [GitHub Discussions](https://github.com/airbytehq/PyAirbyte/discussions)
