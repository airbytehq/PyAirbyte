# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

"""***PyAirbyte MCP Server - Model Context Protocol Integration***

The PyAirbyte MCP (Model Context Protocol) server provides a standardized interface for managing Airbyte connectors through MCP-compatible clients. This experimental feature allows you to list connectors, validate configurations, and run sync operations using the MCP protocol.


The Model Context Protocol (MCP) is an open standard that enables AI assistants and other tools to securely connect to data sources, tools, and services. PyAirbyte's MCP server implementation allows you to interact with Airbyte connectors through this standardized protocol.


The PyAirbyte MCP server provides four main tools:

1. **`list_connectors`** - List all available Airbyte source connectors
2. **`get_config_spec`** - Get the configuration specification for a specific connector
3. **`validate_config`** - Validate a connector configuration (includes secret detection)
4. **`run_sync`** - Execute a sync operation from a source connector to DuckDB cache


- Python 3.10 or higher
- PyAirbyte installed with MCP dependencies: `pip install airbyte[mcp]`
- MCP CLI tool: `pip install devin-mcp-cli`



Create a JSON configuration file to register the PyAirbyte MCP server with your MCP client. Create a file named `server_config.json`:

```json
{
  "mcpServers": {
    "pyairbyte": {
      "command": "python",
      "args": ["-m", "airbyte.mcp.server"],
      "env": {}
    }
  }
}
```


Export the configuration path so the MCP CLI can find your server:

```bash
export MCP_CLI_CONFIG_PATH=/path/to/your/server_config.json
```


Check that your server is properly registered:

```bash
mcp-cli server list
```

This should show `pyairbyte` in the list of available servers.


Verify the server can be reached:

```bash
mcp-cli server check
```

This should show `✓ Connected` for the pyairbyte server with 4 available tools.



```bash
mcp-cli tool call list_connectors --server pyairbyte
```

This returns a markdown-formatted list of all available Airbyte source connectors.


```bash
mcp-cli tool call get_config_spec --server pyairbyte --input '{"connector_name": "source-pokeapi"}'
```

This returns the JSON schema defining the required configuration parameters for the specified connector.


```bash
mcp-cli tool call validate_config --server pyairbyte --input '{
  "connector_name": "source-pokeapi",
  "config": {
    "pokemon_name": "pikachu"
  }
}'
```

This validates the provided configuration and checks for hardcoded secrets.


```bash
mcp-cli tool call run_sync --server pyairbyte --input '{
  "connector_name": "source-pokeapi", 
  "config": {
    "pokemon_name": "pikachu"
  }
}'
```

This executes a full sync operation, reading data from the source and writing it to a local DuckDB cache.



The Pokemon API connector is a simple example that requires minimal configuration:

```json
{
  "pokemon_name": "ditto"
}
```

Valid pokemon names must match the pattern `^[a-z0-9_\-]+$` (lowercase letters, numbers, underscores, and hyphens only).


For connectors that read from files, you might need to provide file paths:

```json
{
  "url": "https://example.com/data.csv",
  "format": "csv"
}
```


Database connectors typically require connection details:

```json
{
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "username": "user",
  "password": "secret"
}
```

**Note:** The MCP server includes secret detection and will warn if hardcoded secrets are found in configurations.


- **Secret Detection**: The server automatically scans configurations for hardcoded secrets and issues warnings
- **Local Execution**: The MCP server runs locally and does not send data to external services
- **Stdio Transport**: Communication uses stdio transport for secure local process communication



If `mcp-cli server list` doesn't show your server:
- Verify the `server_config.json` file exists and has correct syntax
- Check that `MCP_CLI_CONFIG_PATH` environment variable is set correctly
- Ensure PyAirbyte is installed with MCP dependencies


If `mcp-cli server check` shows connection problems:
- Verify Python is available in your PATH
- Check that all PyAirbyte dependencies are installed: `pip install airbyte[mcp]`
- Try running the server directly: `python -m airbyte.mcp.server`


If tool calls fail:
- Verify the connector name exists using `list_connectors`
- Check that your configuration matches the connector's specification using `get_config_spec`
- Validate your configuration using `validate_config` before running sync operations


The PyAirbyte MCP server is designed to work with AI assistants and other MCP-compatible clients. The standardized protocol allows AI systems to:

- Discover available data connectors
- Understand configuration requirements
- Validate configurations before execution
- Execute data sync operations safely


- **Experimental Feature**: The MCP server is experimental and may change without notice
- **Local Execution Only**: Currently designed for local development and testing
- **DuckDB Cache**: Sync operations write to local DuckDB cache by default
- **Python Dependencies**: Requires Python environment with all connector dependencies


For detailed information about the MCP server implementation, see the `airbyte.mcp.server` module documentation.


The PyAirbyte MCP server is part of the PyAirbyte project. Contributions are welcome! Please see the [PyAirbyte Contributing Guide](https://github.com/airbytehq/PyAirbyte/blob/main/docs/CONTRIBUTING.md) for more information.


For issues and questions:
- [PyAirbyte GitHub Issues](https://github.com/airbytehq/pyairbyte/issues)
- [PyAirbyte Discussions](https://github.com/airbytehq/pyairbyte/discussions)

----------------------

"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.mcp.server import PyAirbyteServer


if TYPE_CHECKING:
    from airbyte.mcp import server


__all__ = ["PyAirbyteServer", "server"]

__docformat__ = "google"
