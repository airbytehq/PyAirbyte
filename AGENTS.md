# Agents

> [!IMPORTANT]
> **`main` is patches-only.** The `release-candidate/v1` branch is the active trunk for new development.
>
> - **New features and enhancements:** branch from and target `release-candidate/v1`, not `main`. Only bug fixes and patches for the current release should target `main`.
> - **Investigations and debugging:** check out `release-candidate/v1` to see the latest code. Changes merged to `main` are automatically synced into the release-candidate branch by the [RC Branch: Update from `main`](.github/workflows/rc-branch-update.yml) workflow.

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
