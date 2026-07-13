# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. When OIDC env vars are set, authentication is
handled by `OIDCProxy` (configured in `server.py`).

Environment variables:

- `MCP_SERVER_URL`: Public base URL. Used for OIDC redirect callbacks and to
  derive the MCP endpoint mount path (serves at `/` when the URL has a path
  prefix, otherwise defaults to `/mcp`).
- `OIDC_CONFIG_URL`: Keycloak OIDC discovery URL (enables auth with all three OIDC vars)
- `OIDC_CLIENT_ID`: OIDC client identifier
- `OIDC_CLIENT_SECRET`: OIDC client secret
"""

from __future__ import annotations

import html
import logging
import os
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from starlette.responses import HTMLResponse

from airbyte.mcp.server import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    MCP_SERVER_URL_ENV,
    app,
)


if TYPE_CHECKING:
    from starlette.requests import Request


logger = logging.getLogger(__name__)

# Human-facing landing page shown when a browser GETs the MCP endpoint.
MCP_LANDING_TITLE = "Airbyte MCP Server"
MCP_LANDING_DOCS_URL = "https://docs.airbyte.com/ai-agents/"


def _render_mcp_landing_html(endpoint_url: str, docs_url: str) -> str:
    """Render the landing page shown when a browser opens the MCP endpoint.

    MCP endpoints only speak the streamable HTTP protocol (`POST`/`DELETE`), so
    a browser visit would otherwise return a bare `405 Method Not Allowed`. This
    page explains what the URL is and links to setup instructions.
    """
    safe_url = html.escape(endpoint_url)
    safe_docs = html.escape(docs_url)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{MCP_LANDING_TITLE}</title>
<style>
  :root {{ --accent:#615eff; --ink:#0b0b23; --muted:#5b5b7a; --bg:#f7f7fb; --line:#e6e6f0; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--ink);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }}
  .wrap {{ max-width:640px; margin:0 auto; padding:64px 24px; }}
  .card {{ background:#fff; border:1px solid var(--line); border-radius:16px; padding:40px; }}
  .badge {{ display:inline-block; font-size:12px; font-weight:600; letter-spacing:.05em;
    text-transform:uppercase; color:var(--accent); margin-bottom:16px; }}
  h1 {{ margin:0 0 16px; font-size:24px; }}
  p {{ color:var(--muted); line-height:1.6; margin:0 0 16px; }}
  .url {{ display:block; font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
    background:var(--bg); border:1px solid var(--line); border-radius:8px;
    padding:12px 14px; color:var(--ink); word-break:break-all; margin:0 0 24px; }}
  a.btn {{ display:inline-block; background:var(--accent); color:#fff; text-decoration:none;
    font-weight:600; padding:12px 20px; border-radius:8px; }}
  .foot {{ margin-top:24px; font-size:13px; color:var(--muted); }}
  .foot a {{ color:var(--accent); }}
</style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="badge">Model Context Protocol</div>
      <h1>{MCP_LANDING_TITLE}</h1>
      <p>This URL is an <strong>MCP endpoint</strong>, not a web page. Add it to an
         MCP client such as Claude, Cursor, VS Code, or Goose &mdash; it isn't meant
         to be opened directly in a browser.</p>
      <p>Configure your MCP client with this streamable-HTTP endpoint:</p>
      <span class="url">{safe_url}</span>
      <a class="btn" href="{safe_docs}">Setup instructions &rarr;</a>
      <p class="foot">Powered by <a href="https://airbyte.com">Airbyte</a>.</p>
    </div>
  </div>
</body>
</html>
"""


async def mcp_landing_page(request: Request) -> HTMLResponse:  # noqa: ARG001, RUF029
    """Serve a human-friendly landing page for browser `GET`s to the MCP endpoint."""
    server_url = os.getenv(
        MCP_SERVER_URL_ENV,
        f"http://localhost:{DEFAULT_HTTP_PORT}",
    )
    return HTMLResponse(_render_mcp_landing_html(server_url, MCP_LANDING_DOCS_URL))


def main() -> None:
    """Start the Airbyte MCP server with HTTP transport."""
    logging.basicConfig(level=logging.INFO)

    # When deployed behind a path-stripping LB (MCP_SERVER_URL has a path
    # component like /cloud-mcp), serve the MCP endpoint at root so the
    # public URL is just the base path. Otherwise keep the FastMCP default.
    server_url = os.getenv(
        MCP_SERVER_URL_ENV,
        f"http://localhost:{DEFAULT_HTTP_PORT}",
    )
    mcp_path = "/" if urlparse(server_url).path.strip("/") else "/mcp"

    # Serve a browser-friendly landing page on GET at the MCP path. In stateless
    # mode FastMCP only binds POST/DELETE there, so this GET route does not
    # interfere with MCP traffic.
    app.custom_route(mcp_path, methods=["GET"], name="mcp_landing_page")(mcp_landing_page)

    logger.info(
        "Starting Airbyte MCP HTTP server on %s:%d (mcp_path=%r)",
        DEFAULT_HTTP_HOST,
        DEFAULT_HTTP_PORT,
        mcp_path,
    )

    app.run(
        transport="streamable-http",
        host=DEFAULT_HTTP_HOST,
        port=DEFAULT_HTTP_PORT,
        path=mcp_path,
        stateless_http=True,
    )


if __name__ == "__main__":
    main()
