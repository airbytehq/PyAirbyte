# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

r"""***PyAirbyte MCP Server - Model Context Protocol Integration***

> **NOTE:**
> This MCP server implementation is experimental and may change without notice between minor
> versions of PyAirbyte. The API may be modified or entirely refactored in future versions.

The PyAirbyte MCP (Model Context Protocol) server provides a standardized interface for
managing Airbyte connectors through MCP-compatible clients. This experimental feature
allows you to list connectors, validate configurations, and run sync operations using
the MCP protocol.

## Getting Started with PyAirbyte MCP

To get started with the PyAirbyte MCP server, follow these steps:

1. Create a Dotenv secrets file.
2. Register the MCP server with your MCP client.
3. Test the MCP server connection using your MCP client.

### Step 1: Generate a Dotenv Secrets File

To get started with the PyAirbyte MCP server, you will need to create a dotenv
file containing your Airbyte Cloud credentials, as well as credentials for any
third-party services you wish to connect to via Airbyte.

Create a file named `~/.mcp/airbyte_mcp.env` with the following content:

```ini
# Airbyte Project Artifacts Directory
AIRBYTE_PROJECT_DIR=/path/to/any/writeable/project-dir

# Airbyte Cloud Credentials (Required for Airbyte Cloud Operations)
AIRBYTE_CLOUD_CLIENT_ID=your_api_key
AIRBYTE_CLOUD_CLIENT_SECRET=your_api_secret
AIRBYTE_CLOUD_WORKSPACE_ID=your_workspace_id

# API-Specific Credentials (Optional, depending on your connectors)

# For example, for a PostgreSQL source connector:
# POSTGRES_HOST=your_postgres_host
# POSTGRES_PORT=5432
# POSTGRES_DB=your_database_name
# POSTGRES_USER=your_database_user
# POSTGRES_PASSWORD=your_database_password

# For example, for a Stripe source connector:
# STRIPE_API_KEY=your_stripe_api_key
# STRIPE_API_SECRET=your_stripe_api_secret
# STRIPE_WEBHOOK_SECRET=your_stripe_webhook_secret
```

Note:
1. You can add more environment variables to this file as needed for different connectors. To start,
   you only need to create the file and pass it to the MCP server.
2. Ensure that this file is kept secure, as it contains sensitive information. Your LLM
   *should never* be given direct access to this file or its contents.
3. The MCP tools will give your LLM the ability to view *which* variables are available, but it
   does not give access to their values.
4. The `AIRBYTE_PROJECT_DIR` variable specifies a directory where the MCP server can
   store temporary project files. Ensure this directory is writable by the user running
   the MCP server.

### Step 2: Registering the MCP Server

First install `uv` (`brew install uv`).

Then, create a file named `server_config.json` (or the file name required by your MCP client)
with the following content. This uses `uvx` (from `brew install uv`) to run the MCP
server. If a matching version Python is not yet installed, a `uv`-managed Python
version will be installed automatically. This will also auto-update to use the
"latest" Airbyte MCP release at time of launch. You can alternatively pin to a
specific version of Python and/or of the Airbyte library if you have special
requirements.

```json
{
  "mcpServers": {
    "airbyte": {
      "command": "uvx",
      "args": [
        "--python=3.11",
        "--from=airbyte@latest",
        "airbyte-mcp"
      ],
      "env": {
        "AIRBYTE_MCP_ENV_FILE": "/path/to/my/.mcp/airbyte_mcp.env",
        "AIRBYTE_CLOUD_MCP_SAFE_MODE": "1",
        "AIRBYTE_CLOUD_MCP_READONLY_MODE": "0"
      }
    }
  }
}
```

Note:
- Replace `/path/to/my/.mcp/airbyte_mcp.env` with the absolute path to your dotenv file created in
  Step 1.

### Step 3: Testing the MCP Server Connection

You can test the MCP server connection using your MCP client.

Helpful prompts to try:

1. "Use your MCP tools to list all available Airbyte connectors."
2. "Use your MCP tools to get information about the Airbyte Stripe connector."
3. "Use your MCP tools to list all variables you have access to in the dotenv secrets
   file."
4. "Use your MCP tools to check your connection to your Airbyte Cloud workspace."
5. "Use your MCP tools to list all available destinations in my Airbyte Cloud workspace."

## Airbyte Cloud MCP Server Safety

The PyAirbyte MCP server supports environment variables to control safety and access levels for
Airbyte Cloud operations.

**Important:** The below settings only affect Cloud operations; local operations are not affected.

### Airbyte Cloud Safe Mode

Safe mode is enabled by default and is controlled by the `AIRBYTE_CLOUD_MCP_SAFE_MODE` environment
variable.

When enabled, write operations are allowed but destructive operations (updates, deletions) are
only allowed for objects created within the same session. For example, you can create a new
connector and then delete it, but you cannot delete an existing connector that was not created in
the current session. Modifications to configurations are likewise treated as potentially destructive
and are only allowed for objects created in the current session.

Set the environment variable `AIRBYTE_CLOUD_MCP_SAFE_MODE=0` to disable safe mode.

### Airbyte Cloud Read-Only Mode

Read-only mode is not enabled by default and is controlled by the
`AIRBYTE_CLOUD_MCP_READONLY_MODE` environment variable.

When enabled, only read-only Cloud tools are available. Write and destructive operations are
disabled.

This mode does allow running syncs on existing connectors, since sync operations
are not considered to be modifications of the Airbyte Cloud workspace.

Set the environment variable `AIRBYTE_CLOUD_MCP_READONLY_MODE=1` to enable read-only mode.
## Troubleshooting

### Architecture Overview




The MCP server uses PyAirbyte under the hood to manage Airbyte connectors. PyAirbyte
supports both Python-native connectors (installed via pip/uv) and Docker-based connectors
(run in containers). Understanding this architecture helps when diagnosing issues.


### Path Requirements

**Always use absolute paths in your environment files.** Relative paths, tilde (`~`), or
environment variables like `$HOME` will not work correctly.

The `AIRBYTE_PROJECT_DIR` environment variable is critical - it specifies where PyAirbyte
stores connector artifacts, cache files, and temporary data. Ensure this directory:
- Uses an absolute path (e.g., `/Users/username/airbyte-projects`)
- Is writable by the user running the MCP server
- Has sufficient disk space for connector operations

Example of correct path configuration:
```ini
AIRBYTE_PROJECT_DIR=/Users/username/airbyte-projects

AIRBYTE_PROJECT_DIR=./airbyte-projects

AIRBYTE_PROJECT_DIR=~/airbyte-projects

AIRBYTE_PROJECT_DIR=$HOME/airbyte-projects
```


### Security Model

The MCP server implements a security model that protects your credentials:
- **LLM sees only environment variable names** - The AI assistant can see which variables
  are available (e.g., `POSTGRES_PASSWORD`) but never their actual values
- **MCP server reads actual values** - Only the MCP server process accesses the secret
  values when executing operations
- **Credentials never exposed to LLM** - Your API keys, passwords, and other secrets remain secure
- **Safe Mode protects existing resources** - When enabled (default), destructive
  operations are only allowed on resources created in the current session

This design allows AI assistants to help configure connectors without compromising security.


### Prerequisites Checklist

Before using the MCP server, verify these prerequisites:

1. **Docker Desktop is running** - Many connectors require Docker. Ensure Docker Desktop
   is started and healthy.
2. **Docker CLI is in PATH** - Verify by running `docker --version` in your terminal.
3. **Absolute paths in env file** - Double-check that all paths use absolute notation.
4. **Valid Airbyte Cloud API credentials** - Ensure you have:
   - `AIRBYTE_CLOUD_CLIENT_ID` (not the same as Client Secret)
   - `AIRBYTE_CLOUD_CLIENT_SECRET`
   - `AIRBYTE_CLOUD_WORKSPACE_ID`
   - Note: Client ID and Client Secret are different values - do not confuse them.


### Common Issues

**Issue: "Docker not found" or "Cannot connect to Docker daemon"**
- Solution: Start Docker Desktop and ensure it's running. Verify with `docker ps`.

**Issue: "Permission denied" when accessing AIRBYTE_PROJECT_DIR**
- Solution: Ensure the directory exists and is writable. Create it with
  `mkdir -p /path/to/dir` if needed.

**Issue: "Invalid credentials" when connecting to Airbyte Cloud**
- Solution: Verify your Client ID and Client Secret are correct and not swapped. These are
  different values.

**Issue: Connector fails with "path not found"**
- Solution: Check that all paths in your env file are absolute, not relative or using `~`.

## Contributing to the Airbyte MCP Server

- [PyAirbyte Contributing Guide](https://github.com/airbytehq/PyAirbyte/blob/main/docs/CONTRIBUTING.md)

### Additional resources

- [Model Context Protocol Documentation](https://modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

For issues and questions:
- [PyAirbyte GitHub Issues](https://github.com/airbytehq/pyairbyte/issues)
- [PyAirbyte Discussions](https://github.com/airbytehq/pyairbyte/discussions)

"""  # noqa: D415

from airbyte.mcp import cloud_ops, connector_registry, local_ops, server


__all__: list[str] = [
    "cloud_ops",
    "connector_registry",
    "local_ops",
    "server",
]

__docformat__ = "google"
