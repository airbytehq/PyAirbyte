# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP prompt definitions for the PyAirbyte MCP server.

This module defines prompts that can be invoked by MCP clients to perform
common workflows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from pydantic import Field


if TYPE_CHECKING:
    from fastmcp import FastMCP


TEST_MY_TOOLS_GUIDANCE = """
Test all available tools in this MCP server to confirm they are working properly.

Guidelines:
- Iterate through each tool systematically
- Use read-only operations whenever possible
- For tools that modify data, use test/safe modes or skip if no safe testing method exists
- Avoid creating persistent side effects (e.g., don't create real resources, connections, or data)
- Document which tools were tested and their status
- Report any errors or issues encountered
- Provide a summary of the test results at the end

Focus on validating that tools:
1. Accept their required parameters correctly
2. Return expected output formats
3. Handle errors gracefully
4. Connect to required services (if applicable)

Test Environment:
- If declared, use the `AIRBYTE_CLOUD_WORKSPACE_ID` env var to identify the test workspace.
- Always describe to your user exactly which workspace you are using for testing, and how you
  determined the workspace to use.

Testing Registry Tools:
- Test `list_connectors()` to list available connectors
- Test `get_connector_info(connector_name)` with a common connector like 'source-faker'
- Test `get_api_docs_urls(connector_name)` to retrieve documentation URLs

Testing Local Tools:
- Test `validate_connector_config()` with a simple connector configuration
- Test `list_source_streams()` to discover available streams
- Test `sync_source_to_cache()` with a small record limit (if safe to do so)
- Test `run_sql_query()` on cached data (if available)

Testing Cloud Tools (if workspace is configured):
- Test `list_cloud_sources()` to list sources in the workspace
- Test `list_cloud_destinations()` to list destinations
- Test `list_cloud_connections()` to list connections
- Test `deploy_source_to_cloud()` only if explicitly allowed (creates resources)
- Test `run_cloud_sync()` only on test connections (triggers actual syncs)

Be efficient and practical in your testing approach.
""".strip()

SCOPE_GUIDANCE = {
    "registry": """
Focus testing on Registry tools only:
- `list_connectors()` - List available connectors
- `get_connector_info(connector_name)` - Get connector details
- `get_api_docs_urls(connector_name)` - Get documentation URLs

These are read-only operations that don't require credentials or cloud access.
""",
    "local": """
Focus testing on Local tools only:
- `validate_connector_config()` - Validate connector configurations
- `list_source_streams()` - Discover available streams
- `sync_source_to_cache()` - Sync data to local cache
- `run_sql_query()` - Query cached data

These operations may require connector credentials but don't need cloud access.
""",
    "cloud": """
Focus testing on Cloud tools only:
- `list_cloud_sources()` - List sources in workspace
- `list_cloud_destinations()` - List destinations
- `list_cloud_connections()` - List connections
- `deploy_source_to_cloud()` - Deploy sources (creates resources)
- `create_connection_on_cloud()` - Create connections (creates resources)
- `run_cloud_sync()` - Trigger syncs

These operations require Airbyte Cloud credentials and workspace access.
Be cautious with write operations as they create real resources.
""",
}


def register_prompts(app: FastMCP) -> None:
    """Register all prompts with the FastMCP app.

    Args:
        app: FastMCP application instance
    """

    @app.prompt(
        name="test-my-tools",
        description="Test all available MCP tools to confirm they are working properly",
    )
    def test_my_tools_prompt(
        scope: Annotated[
            str | None,
            Field(
                description=(
                    "Optional scope to focus testing on specific tool domains. "
                    "Valid values: 'registry', 'local', 'cloud'. "
                    "If not provided, all tools will be tested."
                ),
            ),
        ] = None,
    ) -> list[dict[str, str]]:
        """Generate a prompt that instructs the agent to test available tools."""
        content = TEST_MY_TOOLS_GUIDANCE

        if scope and scope.lower() in SCOPE_GUIDANCE:
            scope_text = SCOPE_GUIDANCE[scope.lower()]
            content = f"{content}\n\n---\n\n**Scope: {scope.lower()}**\n{scope_text}"

        return [
            {
                "role": "user",
                "content": content,
            }
        ]
