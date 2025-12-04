# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP smoke test prompt for validating all tools and resources."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from airbyte.mcp._tool_utils import get_registered_tools


def mcp_smoke_tests_prompt(
    read_only: Annotated[
        bool,
        Field(
            description=(
                "If True, only test read-only tools. "
                "If False, test all tools including destructive ones."
            ),
            default=True,
        ),
    ],
) -> list[dict[str, str]]:
    """Run smoke tests on all MCP tools and resources to confirm they are working properly.

    This prompt provides instructions for testing all available MCP tools and checking
    all MCP resource assets. It generates a comprehensive test plan based on the
    registered tools and resources.

    Args:
        read_only: If True, only test read-only tools. If False, test all tools.

    Returns:
        List of prompt messages with testing instructions.
    """
    all_tools = get_registered_tools()

    if read_only:
        tools_to_test = [(func, ann) for func, ann in all_tools if ann.get("readOnlyHint", False)]
    else:
        tools_to_test = all_tools

    tools_by_domain: dict[str, list[str]] = {}
    for func, ann in tools_to_test:
        domain = ann.get("domain", "unknown")
        if domain not in tools_by_domain:
            tools_by_domain[domain] = []
        tools_by_domain[domain].append(func.__name__)

    prompt_parts = [
        "# MCP Smoke Test Instructions\n",
        f"Testing mode: {'READ-ONLY' if read_only else 'ALL TOOLS (including destructive)'}\n",
        "\n## Overview\n",
        "This smoke test will validate that all MCP tools and resources are "
        "functioning correctly. ",
        "Follow the instructions below to test each component systematically.\n",
        "\n## Tools to Test\n",
    ]

    for domain, tool_names in sorted(tools_by_domain.items()):
        prompt_parts.extend(
            [
                f"\n### Domain: {domain}\n",
                f"Number of tools: {len(tool_names)}\n",
                "Tools:\n",
            ]
        )
        prompt_parts.extend(f"- {tool_name}\n" for tool_name in sorted(tool_names))

    prompt_parts.extend(
        [
            "\n## Testing Instructions\n",
            "\n### 1. Tool Testing\n",
            "For each tool listed above:\n",
            "1. Call the tool with minimal/safe parameters\n",
            "2. Verify it returns a response without errors\n",
            "3. Document any failures with error messages\n",
            "\n### 2. Resource Testing\n",
            "Test all available MCP resources:\n",
            "1. List all available resources\n",
            "2. Attempt to read each resource\n",
            "3. Verify the content is accessible\n",
            "\n### 3. Report Generation\n",
            "After testing, generate a report with:\n",
            "- Total tools tested\n",
            "- Number of successful tests\n",
            "- Number of failed tests\n",
            "- List of any failures with error details\n",
            "- Resource access results\n",
            "\n## Expected Behavior\n",
            "- Read-only tools should execute without modifying any data\n",
            "- Tools requiring configuration should fail gracefully with clear error messages\n",
            "- All resources should be accessible\n",
            "\n## Notes\n",
            "- Some tools may require valid configurations or credentials to fully test\n",
            "- Failures due to missing configuration are expected and should be documented\n",
            "- Focus on verifying that tools are callable and return appropriate responses\n",
        ]
    )

    prompt_text = "".join(prompt_parts)

    return [
        {
            "role": "user",
            "content": prompt_text,
        }
    ]


def register_smoke_test_prompt(app: FastMCP) -> None:
    """Register the smoke test prompt with the FastMCP app.

    Args:
        app: The FastMCP app instance
    """
    app.prompt(
        name="mcp_smoke_tests_prompt",
        description=(
            "Run smoke tests on all MCP tools and resources to confirm " "they are working properly"
        ),
    )(mcp_smoke_tests_prompt)
