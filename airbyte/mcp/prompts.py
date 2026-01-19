# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP prompt definitions for the PyAirbyte MCP server.

This module defines prompts that can be invoked by MCP clients to perform
common workflows.
"""

from __future__ import annotations

from typing import Annotated

from fastmcp_extensions import mcp_prompt
from pydantic import Field


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


@mcp_prompt(
    name="test-my-tools",
    description="Test all available MCP tools to confirm they are working properly",
)
def test_my_tools_prompt(
    scope: Annotated[
        str | None,
        Field(
            description=(
                "Optional free-form text to focus or constrain testing. "
                "This can be a single word, a sentence, or a paragraph "
                "describing the desired scope or constraints."
            ),
        ),
    ] = None,
) -> list[dict[str, str]]:
    """Generate a prompt that instructs the agent to test available tools."""
    content = TEST_MY_TOOLS_GUIDANCE

    if scope:
        content = f"{content}\n\n---\n\nAdditional scope or constraints:\n{scope}"

    return [
        {
            "role": "user",
            "content": content,
        }
    ]
