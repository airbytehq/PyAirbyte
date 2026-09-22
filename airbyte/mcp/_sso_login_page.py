# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""The identifier-entry page shown before the upstream redirect on SSO-enabled deployments.

Markup and styling only. `airbyte.mcp._sso_auth.AirbyteSsoOidcProxy` decides when the
page is shown, validates what comes back, and owns the CSRF and cookie handling; this
module just turns that state into HTML. It is the MCP equivalent of the Cloud webapp's
`SSOIdentifierPage`, rendered server-side because the proxy has no frontend bundle,
in the same way FastMCP renders its own consent and error pages.

The page is built with FastMCP's `create_page`, whose Content Security Policy permits
inline styles and nothing external, so the stylesheet lives here as a string.
"""

from __future__ import annotations

import html

from fastmcp.utilities.ui import create_page


CHOICE_DEFAULT = "default"
"""Form value for "Continue with Airbyte Cloud" (the deployment's default realm)."""

CHOICE_SSO = "sso"
"""Form value for "Sign in with SSO" (a customer realm named by the company identifier)."""

LOGIN_PAGE_STYLES = """
    :root { --accent: #615eff; --accent-dark: #4b48d6; --ink: #0b0b23; --muted: #5b5b7a;
            --line: #e6e6f0; --error: #b42318; --error-bg: #fef3f2; }
    .container { text-align: left; }
    .badge { display: inline-block; font-size: 12px; font-weight: 600; letter-spacing: .05em;
             text-transform: uppercase; color: var(--accent); margin-bottom: 12px; }
    h1 { text-align: left; margin-bottom: 0.75rem; color: var(--ink); }
    p.lead { color: var(--muted); line-height: 1.6; margin: 0 0 1.25rem; }
    .client { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.8125rem;
              background: #f7f7fb; border: 1px solid var(--line); border-radius: 8px;
              padding: 10px 12px; word-break: break-all; margin: 0 0 1.5rem; color: var(--ink); }
    .client strong { display: block; font-family: inherit; color: var(--muted);
                     font-size: 0.75rem; margin-bottom: 4px; }
    form { margin: 0; }
    .option { border: 1px solid var(--line); border-radius: 12px; padding: 1.25rem;
              margin-bottom: 1rem; }
    .option h2 { font-size: 1rem; margin: 0 0 0.5rem; color: var(--ink); }
    .option p { color: var(--muted); font-size: 0.875rem; line-height: 1.5; margin: 0 0 0.875rem; }
    label { display: block; font-size: 0.8125rem; font-weight: 600; color: var(--ink);
            margin-bottom: 0.375rem; }
    input[type=text] { width: 100%; font: inherit; padding: 0.625rem 0.75rem;
                       border: 1px solid var(--line); border-radius: 8px;
                       margin-bottom: 0.875rem; color: var(--ink); }
    input[type=text]:focus { outline: 2px solid var(--accent); outline-offset: 1px;
                             border-color: var(--accent); }
    button { width: 100%; font: inherit; font-weight: 600; padding: 0.75rem 1rem;
             border-radius: 8px; border: 1px solid transparent; cursor: pointer; }
    .btn-primary { background: var(--accent); color: #fff; }
    .btn-primary:hover { background: var(--accent-dark); }
    .btn-secondary { background: #fff; color: var(--accent); border-color: var(--accent); }
    .btn-secondary:hover { background: #f4f4ff; }
    .error { background: var(--error-bg); border: 1px solid #fecdca; color: var(--error);
             border-radius: 8px; padding: 0.75rem 0.875rem; font-size: 0.875rem;
             margin-bottom: 1rem; }
    .divider { text-align: center; color: var(--muted); font-size: 0.75rem;
               text-transform: uppercase; letter-spacing: .05em; margin: 0.25rem 0 1rem; }
    .foot { margin-top: 1rem; font-size: 0.8125rem; color: var(--muted); line-height: 1.5; }
"""


def render_login_page(
    *,
    txn_id: str,
    csrf_token: str,
    client_name: str,
    client_redirect_uri: str,
    company_identifier: str = "",
    error_message: str | None = None,
) -> str:
    """Render the identifier-entry page shown before the upstream redirect.

    Both forms post back to this same URL (like FastMCP's consent page, so a
    path-stripping load balancer cannot break the action) and carry the transaction
    id plus the CSRF token; the SSO form adds the company identifier.
    """
    hidden = (
        f'<input type="hidden" name="txn_id" value="{html.escape(txn_id, quote=True)}">'
        f'<input type="hidden" name="csrf_token" value="{html.escape(csrf_token, quote=True)}">'
    )
    error_html = (
        f'<div class="error" role="alert">{html.escape(error_message)}</div>'
        if error_message
        else ""
    )
    content = f"""
        <div class="container">
            <span class="badge">Airbyte MCP</span>
            <h1>Sign in to Airbyte Cloud</h1>
            <p class="lead">
                <strong>{html.escape(client_name)}</strong> wants to use Airbyte Cloud through
                this MCP server. Choose how you sign in.
            </p>
            <div class="client">
                <strong>After signing in you will return to</strong>
                {html.escape(client_redirect_uri)}
            </div>
            {error_html}
            <form method="POST" action="" class="option">
                {hidden}
                <input type="hidden" name="choice" value="{CHOICE_DEFAULT}">
                <h2>Airbyte Cloud account</h2>
                <p>Email and password, Google, or GitHub.</p>
                <button type="submit" class="btn-primary">Continue with Airbyte Cloud</button>
            </form>
            <div class="divider">or</div>
            <form method="POST" action="" class="option">
                {hidden}
                <input type="hidden" name="choice" value="{CHOICE_SSO}">
                <h2>Single sign-on</h2>
                <p>Use the company identifier your organization set up for SSO.</p>
                <label for="company_identifier">Company identifier</label>
                <input type="text" id="company_identifier" name="company_identifier"
                       value="{html.escape(company_identifier, quote=True)}"
                       autocomplete="organization" autocapitalize="none" spellcheck="false"
                       maxlength="63" required>
                <button type="submit" class="btn-secondary">Sign in with SSO</button>
            </form>
            <p class="foot">Not you? Close this window and the connection will not be
            authorized.</p>
        </div>
    """
    return create_page(
        content, title="Sign in to Airbyte Cloud", additional_styles=LOGIN_PAGE_STYLES
    )
