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
      "env": {
        "AIRBYTE_MCP_ENV_FILE": "~/.mcp/airbyte_mcp.env"
      }
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


The easiest way to test PyAirbyte MCP tools during development is using the built-in Poe tasks.
These tasks automatically inherit environment variables from your shell session:

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


```bash
poe mcp-serve-local    # STDIO transport (default)
poe mcp-serve-http     # HTTP transport on localhost:8000
poe mcp-serve-sse      # Server-Sent Events transport on localhost:8000

poe mcp-inspect        # Show all available MCP tools and their schemas
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
