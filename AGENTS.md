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

## Agent-facing Cloud Error Messages

Cloud API error text comes from the response's nonempty `detail`, falling back to `title`.
PyAirbyte preserves the selected string; fix poor Cloud wording in the platform.
Only actor-not-ready setup instructions and 403 resource/access guidance are added.
Bodies must be JSON objects of at most 65,536 UTF-8 bytes; invalid or non-problem responses
use a status-only fallback. `message`, `data.message`, and `errorId` are not displayed.
Agent errors, caught-error billing messages, and connector warnings pass through credential
redaction. Native Datadog uses the selected,
unredacted Cloud message without guidance; this does not add it to LLM Obs metadata.

Non-Cloud MCP trace messages are the first line of the exception class's docstring
(trace-only, exported to Datadog as `error.message`), so keep those first lines fixed text
with no IDs, values or URLs.

## MCP Argument Tracing

Trace classes come from `airbyte/mcp/arg_trace_rules.yaml`; do not put `TraceArg` markers in tool signatures.
When `tests/unit_tests/test_mcp_arg_trace_parity.py` reports an unclassified argument, add or adjust a YAML rule; `poe mcp-arg-trace-plan` writes the full plan and each decision source to gitignored `arg_trace_plan.json`.
