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

Be efficient and practical in your testing approach.
""".strip()

SCOPE_GUIDANCE = {
    "registry": "Focus testing on Registry tools only.",
    "local": "Focus testing on Local tools only.",
    "cloud": "Focus testing on Cloud tools only.",
}


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


def register_prompts(app: FastMCP) -> None:
    """Register all prompts with the FastMCP app."""
    app.prompt(
        name="test-my-tools",
        description="Test all available MCP tools to confirm they are working properly",
    )(test_my_tools_prompt)
