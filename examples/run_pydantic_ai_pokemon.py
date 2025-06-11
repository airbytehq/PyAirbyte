#!/usr/bin/env python3

"""Example script demonstrating PydanticAI agent with PyAirbyte MCP server integration.

This script shows how to:
1. Connect a PydanticAI agent to the PyAirbyte MCP server via stdio transport
2. Use GitHub Models as an OpenAI-compatible endpoint for the agent
3. Use MCP tools to sync Pokemon data from the PokeAPI source connector
4. Handle misspelling correction ("Bulbsaur" → "Bulbasaur") via LLM reasoning
5. Sync data for Pikachu, Charizard, and Bulbasaur to the default DuckDB cache

Usage (from PyAirbyte root directory):
> export GITHUB_TOKEN=your_github_token_here
> poetry run python ./examples/run_pydantic_ai_pokemon.py

The agent will use the MCP server tools to find the source-pokeapi connector,
validate configurations, and sync Pokemon data to the local DuckDB cache.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio
from pydantic_ai.models.openai import OpenAIModel


async def main() -> None:
    """Main entry point for the PydanticAI Pokemon sync example."""
    print("=== PydanticAI Pokemon Sync with PyAirbyte MCP Server ===\n")

    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        print("❌ Error: GITHUB_TOKEN environment variable is required")
        print("Please set your GitHub token: export GITHUB_TOKEN=your_token_here")
        sys.exit(1)

    model = OpenAIModel(
        model_name="gpt-4o-mini",  # Using a smaller model for the example
        base_url="https://models.github.ai/inference",
        api_key=github_token,
    )

    mcp_server = MCPServerStdio(
        command="python",
        args=["-m", "airbyte.mcp.server"],
        env=os.environ.copy(),  # Inherit environment variables
        cwd=Path.cwd(),  # Use current working directory
        tool_prefix="airbyte",  # Prefix tools with "airbyte_"
    )

    agent = Agent(
        model=model,
        mcp_servers=[mcp_server],
        system_prompt="""You are an AI assistant that helps sync Pokemon data using Airbyte connectors.

You have access to Airbyte MCP tools that let you:
- List available connectors
- Get configuration specifications for connectors
- Validate connector configurations
- Run sync operations to load data into DuckDB cache

Your task is to sync Pokemon data for Pikachu, Charizard, and Bulbasaur from the PokeAPI source.

Important notes:
- Use the source-pokeapi connector for Pokemon data
- Each Pokemon requires a separate sync with its own configuration
- The pokemon_name config field should be lowercase
- If you see "Bulbsaur" in the request, correct it to "Bulbasaur"
- Sync data to the default DuckDB cache
- Always validate configurations before running syncs
- Provide clear status updates on your progress

Be helpful and explain what you're doing at each step.""",
    )

    user_prompt = """Please use your Airbyte MCP server tools to sync data from the pokeapi into our db cache. We want data for Pikachu, Charizard, and Bulbsaur."""

    print("🤖 Starting PydanticAI agent with MCP server connection...")
    print(f"📡 Using GitHub Models endpoint: https://models.github.ai/inference")
    print(f"🔧 MCP Server: PyAirbyte with airbyte_ tool prefix")
    print(f"💬 User request: {user_prompt}\n")

    try:
        async with agent.run_mcp_servers():
            print("✅ MCP server connection established")
            print("🚀 Running PydanticAI agent...\n")

            result = await agent.run(user_prompt)

            print("🎉 Agent execution completed!")
            print(f"📝 Final response:\n{result.data}\n")

            print("🔍 Verifying sync results...")
            verify_result = await agent.run(
                "Please check what Pokemon data was successfully synced to our cache. "
                "List the Pokemon names and confirm the data is available."
            )
            print(f"✅ Verification result:\n{verify_result.data}")

    except Exception as e:
        print(f"❌ Error during agent execution: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    print("\n🎯 Pokemon sync example completed successfully!")
    print("The agent should have:")
    print("  ✅ Corrected 'Bulbsaur' to 'Bulbasaur'")
    print("  ✅ Found and used the source-pokeapi connector")
    print("  ✅ Synced data for all three Pokemon to DuckDB cache")
    print("  ✅ Validated configurations before running syncs")


if __name__ == "__main__":
    asyncio.run(main())
