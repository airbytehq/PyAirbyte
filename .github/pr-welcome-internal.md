## 👋 Greetings, Airbyte Team Member!

Here are some helpful tips and reminders for your convenience.

<details>
<summary><b>💡 Show Tips and Tricks</b></summary>

### Testing This PyAirbyte Version

Install this branch as a persistent preview CLI before running usage or test examples:

```bash
# Install a uv-managed Python and require uv to use only uv-managed Python.
uv python install 3.11
uv tool install --managed-python --python 3.11 \
  'git+https://github.com/airbytehq/PyAirbyte.git@{{ .branch_name }}'

# Use the preview-suffixed CLI to avoid conflicting with a released install.
airbyte-preview --help
```

### PR Slash Commands

Airbyte Maintainers can execute the following slash commands on your PR:

- `/fix-pr` - Fixes most formatting and linting issues
- `/uv-lock` - Updates uv.lock file
- `/test-pr` - Runs tests with the updated PyAirbyte
- `/prerelease` - Builds and publishes a prerelease version to PyPI

</details>

<details>
<summary><b>📚 Show Repo Guidance</b></summary>

### Helpful Resources

- [PyAirbyte Documentation](https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started)
- [API Reference](https://airbytehq.github.io/PyAirbyte/)

### Community Support

Questions? Join the [#pyairbyte channel](https://airbytehq.slack.com/archives/C06FZ238P8W) in our Slack workspace.

[📝 _Edit this welcome message._](https://github.com/airbytehq/PyAirbyte/blob/main/.github/pr-welcome-internal.md)

</details>
