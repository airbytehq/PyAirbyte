# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

r"""***PyAirbyte MCP Server - Model Context Protocol Integration***

The PyAirbyte MCP (Model Context Protocol) server provides a standardized interface for
managing Airbyte connectors through MCP-compatible clients. This experimental feature
allows you to list connectors, validate configurations, and run sync operations using
the MCP protocol.

The Model Context Protocol (MCP) is an open standard that enables AI assistants and
other tools to securely connect to data sources, tools, and services. PyAirbyte's MCP
server implementation allows you to build and interact with Airbyte connectors through
this standardized protocol.

Create a JSON configuration file to register the PyAirbyte MCP server with your MCP
client. Create a file named `server_config.json`:

> **NOTE:**
> This MCP server implementation is experimental and may change without notice between minor
> versions of PyAirbyte. The API may be modified or entirely refactored in future versions.

# MCP Server Configuration

Assuming `uv` is installed, you can use the following configuration:

```json
{
  "mcpServers": {
    "airbyte": {
      "command": "uvx",
      "args": ["--from=airbyte", "airbyte-mcp"],
      "env": {}
    }
  }
}
```

If you prefer to pre-install, first run `uv install airbyte` or `pipx install airbyte`,
and then create the configuration file as follows:

```json
{
  "mcpServers": {
    "airbyte": {
      "command": "airbyte-mcp",
      "args": [],
      "env": {
        "AIRBYTE_MCP_ENV_FILE": "~/.mcp/airbyte_mcp.env"
      }
    }
  }
}
```

## Testing the MCP Server

You can use your tool of choice to test the MCP server. Below we'll use the Devin `mcp-cli`
tool. First, we install our MCP CLI client:

```
uv tool install devin-mcp-cli
```

Next, we export the configuration path so that MCP CLI can find your server:

```bash
# You can use a custom path in your home directory:
export MCP_CLI_CONFIG_PATH=~/.mcp/mcp_server_config.json
touch $MCP_CLI_CONFIG_PATH

# Then you can symlink your file to also be available in Claude Desktop path:
mkdir -p ~/Library/"Application Support"/Claude
ln -s ~/.mcp/mcp_server_config.json ~/Library/"Application Support"/Claude/claude_desktop_config.json

# Confirm the Claude Desktop symlink is working:
cat ~/Library/"Application Support"/Claude/claude_desktop_config.json
```

### Check that your server is properly registered

```bash
mcp-cli server list
mcp-cli tool list
```

This should show `airbyte` in the list of available servers.

### Verify the server can be reached

```bash
mcp-cli server check
```

This should show `✓ Connected` for the Airbyte server with a list of available tools.

### Test the MCP Tools

You should now be able to validate specific tools:

```bash
# Show a list of all available Airbyte source connectors:
mcp-cli tool call list_connectors --server airbyte

# Get JSON schema of expected config for the specified connector:
mcp-cli tool call get_config_spec --server airbyte --input '{"connector_name": "source-pokeapi"}'

# Validate the provided configuration:
mcp-cli tool call validate_config --server airbyte --input '{
  "connector_name": "source-pokeapi",
  "config": {
    "pokemon_name": "pikachu"
  }
}'

# Run a sync operation with the provided configuration:
mcp-cli tool call run_sync --server airbyte --input '{
  "connector_name": "source-pokeapi",
  "config": {
    "pokemon_name": "pikachu"
  }
}'
```

## Contributing to PyAirbyte and the Airbyte MCP Server

The Airbyte MCP server is part of the PyAirbyte project. Contributions are welcome!

You can contribute to the MCP server by adding new tools, improving existing functionality, or
fixing bugs. The server is built using the FastMCP framework, which provides a flexible
interface for defining tools and handling requests.

As a starting point, you can clone the repo and inspect the server definition using the `fastmcp`
CLI tool:

```bash
poetry install --all-extras
poetry run fastmcp inspect airbyte/mcp/server.py:app
```

In your MCP config, you can test your development updates using `poetry` as the entrypoint:

```json
{
  "mcpServers": {
    "airbyte": {
      "command": "poetry",
      "args": [
        "--directory=~/repos/PyAirbyte",
        "run",
        "airbyte-mcp"
      ],
      "env": {
        "AIRBYTE_MCP_ENV_FILE": "~/.mcp/airbyte_mcp.env"
      }
    }
  }
}
```

### Additional resources

- [Model Context Protocol Documentation](https://modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

For issues and questions:
- [PyAirbyte Contributing Guide](https://github.com/airbytehq/PyAirbyte/blob/main/docs/CONTRIBUTING.md)
- [PyAirbyte GitHub Issues](https://github.com/airbytehq/pyairbyte/issues)
- [PyAirbyte Discussions](https://github.com/airbytehq/pyairbyte/discussions)

"""  # noqa: D415

from airbyte.mcp import server


__all__: list[str] = ["server"]

__docformat__ = "google"
