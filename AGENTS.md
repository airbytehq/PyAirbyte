# Agents

## MCP UI Development

When adding or changing MCP tools that return UI elements, use the
`mcp-ui-development-testing` skill from `airbytehq/ai-skills`
(`.agents/skills/mcp-ui-development-testing/SKILL.md`).

Key conventions:

- Name UI-first tools with a `show_` prefix.
- Return bounded agent-readable text plus structured UI content.
- Make any capped agent preview explicit, because the agent cannot see the user-facing UI.
- Verify the server-side payload contract.
- Capture human-reviewable evidence with MCPJam or Goose Desktop when retesting is requested, including the rendered widget and any important UI interaction.
