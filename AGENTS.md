# Agents

## MCP Server: Deployment and Auth Retesting

PyAirbyte ships the MCP server *as part of the published PyPI package* — there
is **no hosted deployment** in this repo (unlike `airbyte-ops-mcp`, which runs on
Cloud Run). Consumers run it themselves:

- **stdio** (default, no transport auth): `uvx --from=airbyte airbyte-mcp`
- **HTTP** (for remote/hosted use, behind their own load balancer):
  `airbyte-mcp-http` (entry point `airbyte.mcp.http_main:main`, serves on
  `/mcp` by default).

"Redeploy" therefore means **publish a new release** to PyPI (via
`.github/workflows/pypi_publish.yml` on a GitHub Release) — merging to `main`
does not deploy anything. Retesting is done locally.

**Retesting transport auth.** Transport auth is assembled in
`airbyte/mcp/server.py` via `fastmcp_extensions.resolve_mcp_auth` (interactive
OIDC and/or headless bearer, combined with `MultiAuth`). To retest the headless
bearer path locally, boot the HTTP server with the Airbyte Cloud realm defaults
enabled:

```bash
MCP_AUTH_AIRBYTE_CLOUD=true MCP_SERVER_URL=http://localhost:8080 \
  uv run airbyte-mcp-http    # serves on http://localhost:8080/mcp
```

Then run the matrix (mints a short-lived app token from
`AIRBYTE_CLOUD_CLIENT_ID`/`AIRBYTE_CLOUD_CLIENT_SECRET` and exercises the `/mcp`
endpoint). Expected: no token → `401`, valid token → `200`, tampered → `401`:

```bash
TOKEN=$(curl -s -X POST https://api.airbyte.com/v1/applications/token \
  -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$AIRBYTE_CLOUD_CLIENT_ID\",\"client_secret\":\"$AIRBYTE_CLOUD_CLIENT_SECRET\"}" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

req() {  # $1 = optional Authorization header value
  curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8080/mcp \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    ${1:+-H "Authorization: $1"} \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
}

req                              # no token   -> 401
req "Bearer $TOKEN"              # valid       -> 200
req "Bearer ${TOKEN%????}XXXX"   # tampered    -> 401
```

In stdio mode there is no transport auth, so no bearer is required.

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
