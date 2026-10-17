## 👋 Welcome to PyAirbyte!

Thank you for your contribution from **{{ .repo_name }}**! We're excited to have you in the Airbyte community.

<details>
<summary><b>💡 Show Tips and Tricks</b></summary>

### Testing This PyAirbyte Version

You can test this version of PyAirbyte using the following:

```bash
# Run PyAirbyte CLI from this branch:
uvx --from 'git+https://github.com/{{ .repo_name }}.git@{{ .branch_name }}' pyairbyte --help

# Install PyAirbyte from this branch for development:
pip install 'git+https://github.com/{{ .repo_name }}.git@{{ .branch_name }}'
```

### PR Slash Commands

As needed or by request, Airbyte Maintainers can execute the following slash commands on your PR:

- `/fix-pr` - Fixes most formatting and linting issues
- `/uv-lock` - Updates uv.lock file
- `/test-pr` - Runs tests with the updated PyAirbyte
- `/prerelease` - Builds and publishes a prerelease version to PyPI
- `/evals` - Runs the Cloud MCP evals that use the MCP tools this PR changed (`/evals full=true` runs the full suite; `/evals compare=true` runs the PR and its base and posts the difference). Add any `cloud_mcp_evals run` arguments after it, e.g. `/evals full=true --repeats 3 scenarios/regressions`. Write options as `--repeats 3`, not `--repeats=3`.

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
