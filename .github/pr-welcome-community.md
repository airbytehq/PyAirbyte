## 👋 Welcome to PyAirbyte!

Thank you for your contribution from **{{ .repo_name }}**! We're excited to have you in the Airbyte community.

<details>
<summary><b>💡 Show Tips and Tricks</b></summary>

### Testing This PyAirbyte Version

Install this branch as a persistent preview CLI before running usage or test examples:

```bash
# Install a uv-managed Python and require uv to use only uv-managed Python.
uv python install 3.11
uv tool install --managed-python --python 3.11 \
  'git+https://github.com/{{ .repo_name }}.git@{{ .branch_name }}'

# Use the preview-suffixed CLI to avoid conflicting with a released install.
airbyte-preview --help
```

### PR Slash Commands

As needed or by request, Airbyte Maintainers can execute the following slash commands on your PR:

- `/fix-pr` - Fixes most formatting and linting issues
- `/uv-lock` - Updates uv.lock file
- `/test-pr` - Runs tests with the updated PyAirbyte
- `/prerelease` - Builds and publishes a prerelease version to PyPI

</details>

<details>
<summary><b>📚 Show Repo Guidance</b></summary>

### Helpful Resources

- [Contributing Guidelines](https://github.com/airbytehq/PyAirbyte/blob/main/docs/CONTRIBUTING.md)
- [PyAirbyte Documentation](https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started)
- [API Reference](https://airbytehq.github.io/PyAirbyte/)

If you have any questions, feel free to ask in the PR comments or join our community:
- [Airbyte Slack](https://airbytehq.slack.com/) - Join the [#pyairbyte channel](https://airbytehq.slack.com/archives/C06FZ238P8W)
- [GitHub Discussions](https://github.com/airbytehq/PyAirbyte/discussions)

[📝 _Edit this welcome message._](https://github.com/airbytehq/PyAirbyte/blob/main/.github/pr-welcome-community.md)

</details>
