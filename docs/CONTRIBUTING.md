# Contributing to PyAirbyte

Learn how you can become a contributor to PyAirbyte.

## Development

- Make sure [Poetry is installed](https://python-poetry.org/docs/#) (version 2.0 or higher).
- Run `poetry install`
- For examples, check out the `examples` folder. They can be run via `poetry run python examples/<example file>`

Note: By default in Poetry 2.0, `poetry lock` only refreshes the lockfile without pulling new versions. This is the same behavior as the previous `poetry lock --no-update` command.

- Unit tests and type checks can be run via `poetry run pytest`

## Documentation

Regular documentation lives in the `/docs` folder. Based on the doc strings of public methods, we generate API documentation using [pdoc](https://pdoc.dev).

To generate the documentation, run:

```console
poe docs-generate
```

Or to build and open in one step:

```console
poe docs-preview
```

or `poetry run poe docs-preview` if you don't have [Poe](https://poethepoet.natn.io/index.html) installed.

The `docs-generate` Poe task is mapped to the `run()` function of `docs/generate.py`.

Documentation pages will be generated in the `docs/generated` folder. The `test_docs.py` test in pytest will automatically update generated content. This updates must be manually committed before docs tests will pass.

## Release

Releases are published automatically to PyPi in response to a "published" event on a GitHub Release Tag.

To publish to PyPi, simply [create a GitHub Release](https://github.com/airbytehq/PyAirbyte/releases/new) with the correct version. Once you publish the release on GitHub it will automatically trigger a PyPi publish workflow in GitHub actions.

> **Warning**
>
> Be careful - "Cmd+Enter" will not 'save' but will instead 'publish'. (If you want to save a draft, use the mouse. 😅)

> **Note**
>
> There is no version to bump. Version is calculated during build and publish, using the [poetry-dynamic-versioning](https://github.com/mtkennerly/poetry-dynamic-versioning) plugin.

## Coverage

To run a coverage report, run:

```console
poetry run poe coverage-html
```

This will generate a coverage report in the `htmlcov` folder.

Note: If you have pre-installed [Poe](https://poethepoet.natn.io/index.html)
(`pipx install poethepoet`), then you can omit the `poetry run` prefix.

## Versioning

Versioning follows [Semantic Versioning](https://semver.org/). For new features, bump the minor version. For bug fixes, bump the patch version. For pre-releases, append `dev.N` to the version. For example, `0.1.0dev.1` is the first pre-release of the `0.1.0` version.

## GitHub Action Workflows

This repo uses a policy of SHA-pinning GitHub Actions, for hardened security.

To pin your GitHub actions, you can use the [pinact](https://github.com/suzuki-shunsuke/pinact) tool:

```bash
# Install pinact CLI tool
go install github.com/suzuki-shunsuke/pinact/cmd/pinact@latest

# Pin all GitHub Actions in workflow files
pinact run

# Pin actions in a specific file
pinact run .github/workflows/python_lint.yml

# Check if actions are pinned (dry-run)
pinact run --dry-run
```

You can also use the `/gh-ci-fix` slash command on pull requests to automatically pin actions.

# Convert from from fixed version to sha
# Example: actions/checkout@v4 -> actions/checkout@08e... # v4.3.0
pinact run [optional_file]
```

## Contributing to the PyAirbyte MCP Server

The Airbyte MCP server is part of the PyAirbyte project. Contributions are welcome!

You can contribute to the MCP server by adding new tools, improving existing functionality, or
fixing bugs. The server is built using the FastMCP framework, which provides a flexible
interface for defining tools and handling requests.

As a starting point, you can clone the repo and inspect the server definition using the `Poe` task:

### Testing the MCP Server From Source

```bash
poetry install --all-extras
poetry run poe mcp-inspect
```

In your MCP config, you can test your development updates using `poetry` as the entrypoint:

```json
{
  "mcpServers": {
    "airbyte": {
      "command": "poetry",
      "args": [
        "--directory=/path/to/repos/PyAirbyte",
        "run",
        "airbyte-mcp"
      ],
      "env": {
        "AIRBYTE_MCP_ENV_FILE": "/path/to/my/.mcp/airbyte_mcp.env"
      }
    }
  }
}
```

> _**NOTE:** The MCP Server does not hot-reload after a session is started. To incorporate code changes made during an agent session, please close and re-open your MCP client._

### Testing MCP Tools

The easiest way to test PyAirbyte MCP tools during development is using the built-in Poe tasks.

```bash
poe mcp-tool-test <tool_name> '<json_args>'

poe mcp-tool-test list_connectors '{}'
poe mcp-tool-test get_config_spec '{"connector_name": "source-pokeapi"}'
poe mcp-tool-test validate_config \
    '{"connector_name": "source-pokeapi", "config": {"pokemon_name": "pikachu"}}'
poe mcp-tool-test run_sync \
    '{"connector_name": "source-pokeapi", "config": {"pokemon_name": "pikachu"}}'

poe mcp-tool-test check_airbyte_cloud_workspace '{}'
poe mcp-tool-test list_deployed_cloud_connections '{}'
```

You can also invoke the server using one of these helper tasks:

```bash
poe mcp-serve-local    # STDIO transport (default)
poe mcp-serve-http     # HTTP transport on localhost:8000
poe mcp-serve-sse      # Server-Sent Events transport on localhost:8000

poe mcp-inspect        # Show all available MCP tools and their schemas
```
