# Hosting the Airbyte Replication MCP Server over HTTPS

The Airbyte Replication MCP server supports remote clients that require a public HTTPS endpoint. The local stdio transport (`airbyte-mcp`) is for desktop MCP clients only.

For hosted deployment, use the **`airbyte-mcp-http`** entry point, which serves the MCP protocol over **Streamable HTTP** — the transport remote MCP clients expect.

## Architecture

```text
Remote MCP client (MCP Inspector, hosted agent platform, etc.)
        │  HTTPS
        ▼
Reverse proxy / load balancer  (TLS termination)
        │  HTTP :8080
        ▼
airbyte-mcp-http  (streamable-http, stateless)
        │
        ▼
Airbyte Cloud / local connector tools
```

TLS is terminated at the reverse proxy or load balancer. The MCP server itself listens on plain HTTP (`0.0.0.0:8080`).

There is **no official Airbyte-hosted public MCP endpoint** in this repository. You self-host the server in your own infrastructure and expose it at a public HTTPS URL.

## Quick start with Docker

Build and run the MCP server container:

```bash
docker build -f Dockerfile.mcp -t airbyte-mcp .
docker run --rm -p 8080:8080 \
  -e AIRBYTE_MCP_ENV_FILE=/secrets/airbyte_mcp.env \
  -v "$HOME/.mcp/airbyte_mcp.env:/secrets/airbyte_mcp.env:ro" \
  airbyte-mcp
```

For HTTPS with automatic certificates, use the included Compose stack:

```bash
# Set your public hostname, then start Caddy + MCP server
export MCP_PUBLIC_HOST=mcp.example.com
docker compose -f deploy/docker-compose.mcp.yml up -d
```

Caddy terminates TLS and proxies to the MCP server. Set `MCP_SERVER_URL=https://mcp.example.com` in the Compose file or environment so OIDC callbacks resolve correctly.

Health checks: `GET /health` returns `{"status": "ok"}`.

## MCP endpoint URL

The public MCP URL depends on how you configure `MCP_SERVER_URL`:

| `MCP_SERVER_URL` | Internal mount path | Public MCP endpoint |
| --- | --- | --- |
| `https://mcp.example.com` | `/mcp` | `https://mcp.example.com/mcp` |
| `https://mcp.example.com/airbyte` | `/` (path-stripped LB) | `https://mcp.example.com/airbyte` |

When configuring a remote MCP client, use the **public MCP endpoint URL** from the table above.

## Environment variables

| Variable | Required | Description |
| --- | --- | --- |
| `AIRBYTE_MCP_ENV_FILE` | Recommended | Path to dotenv secrets file (Airbyte Cloud credentials, connector secrets) |
| `MCP_SERVER_URL` | Recommended | Public HTTPS base URL (used for OIDC callbacks and mount path) |
| `MCP_CORS_ORIGINS` | Optional | Comma-separated CORS origins (e.g. `https://app.example.com`) |
| `OIDC_CONFIG_URL` | Optional | Keycloak OIDC discovery URL (enables OAuth when all three OIDC vars are set) |
| `OIDC_CLIENT_ID` | Optional | OIDC client ID |
| `OIDC_CLIENT_SECRET` | Optional | OIDC client secret |
| `AIRBYTE_CLOUD_MCP_SAFE_MODE` | Optional | Default `1`; restrict destructive ops to session-created resources |
| `AIRBYTE_CLOUD_MCP_READONLY_MODE` | Optional | Default `0`; disable write tools when `1` |

## Integrating with remote MCP clients

Remote MCP clients connect over HTTPS using **Streamable HTTP** (with SSE fallback). Stdio-based servers cannot be used directly.

### 1. Deploy and expose HTTPS

1. Deploy `airbyte-mcp-http` (Docker image from `Dockerfile.mcp`, or run directly).
2. Put it behind a reverse proxy with a valid TLS certificate.
3. Confirm the endpoint responds: `curl -s https://your-host/health`.

### 2. Register the server with your client

In your remote MCP client's admin or integration settings:

1. Add a new remote MCP server.
2. Enter your public MCP URL (for example, `https://mcp.example.com/mcp`).
3. Choose an authentication method (see below).

### 3. Authentication options

#### Option A: Bearer token (simplest)

Use this when the MCP server is reachable only on your network or protected by your proxy, and you want the client to send Airbyte Cloud credentials on each request.

1. Select **Bearer Token** authentication in your MCP client.
2. Provide an Airbyte Cloud API bearer token (or the token your deployment expects in the `Authorization` header).

The server resolves Airbyte Cloud credentials from HTTP headers in this order:

1. `Authorization` (bearer token)
2. `X-Airbyte-Cloud-Client-Id` / `X-Airbyte-Cloud-Client-Secret`
3. `X-Airbyte-Workspace-Id`
4. Environment variables from `AIRBYTE_MCP_ENV_FILE`

If your MCP client supports custom headers, you can pass `X-Airbyte-Workspace-Id` and client credentials instead of a bearer token.

#### Option B: OIDC (Keycloak)

Use this to protect the MCP endpoint with OAuth. Configure Keycloak (or compatible OIDC provider) and set all three OIDC environment variables.

Whitelist your MCP client's OAuth callback URLs in your OIDC provider. Consult your client's documentation for the exact redirect URIs to register.

Set `MCP_SERVER_URL` to your public HTTPS URL before enabling OIDC so redirect URIs resolve correctly.

#### Option C: Network-level protection

If the MCP server has no OIDC and is exposed publicly, protect it with firewall rules, IP allowlists, or an API gateway in front of the endpoint.

### 4. CORS (if needed)

If your MCP client's OAuth or browser-based flows require CORS, set:

```bash
export MCP_CORS_ORIGINS="https://app.example.com,https://admin.example.com"
```

Remote clients typically connect server-to-server, so CORS is often unnecessary for tool calls. Enable it if you see CORS errors during OAuth or connection setup.

## Local development with Streamable HTTP

Test the same transport used in production:

```bash
poe mcp-serve-streamable-http
```

This starts the server on `http://127.0.0.1:8080/mcp`. Use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) to connect over HTTP for local testing.

## Reverse proxy examples

### Caddy (included in `deploy/`)

See `deploy/Caddyfile` and `deploy/docker-compose.mcp.yml`.

### nginx

```nginx
server {
    listen 443 ssl;
    server_name mcp.example.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location /health {
        proxy_pass http://127.0.0.1:8080/health;
    }
}
```

## Related documentation

- [MCP server setup (stdio clients)](../airbyte/mcp/__init__.py) — local Claude Desktop / Cursor configuration
- [Contributing: MCP development](./CONTRIBUTING.md#mcp-server-development)
