# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Renders the identifier-entry page shown before the upstream redirect on SSO deployments.

The markup lives in `_templates/sso_login_page.html` and the stylesheet in
`_templates/sso_login_page.css`, both shipped inside the package and loaded through
Jinja2's `PackageLoader`. `airbyte.mcp._sso_auth.AirbyteSsoOidcProxy` decides when the
page is shown, validates what comes back, and owns the CSRF and cookie handling; this
module only turns that state into HTML. It is the MCP equivalent of the Cloud webapp's
`SSOIdentifierPage`, rendered server-side because the proxy has no frontend bundle, in
the same way FastMCP renders its own consent and error pages.

The stylesheet is inlined into a `<style>` tag at render time because the page's
Content Security Policy (FastMCP's default for its OAuth pages) allows inline styles and
nothing external. Autoescaping is on, so every interpolated value is HTML-escaped.
"""

from __future__ import annotations

import functools

from jinja2 import Environment, PackageLoader, StrictUndefined


CHOICE_DEFAULT = "default"
"""Form value for "Continue with Airbyte Cloud" (the deployment's default realm)."""

CHOICE_SSO = "sso"
"""Form value for "Sign in with SSO" (a customer realm named by the company identifier)."""

TEMPLATE_PACKAGE = "airbyte.mcp"
TEMPLATE_DIR = "_templates"
LOGIN_TEMPLATE = "sso_login_page.html"
LOGIN_STYLESHEET = "sso_login_page.css"

PAGE_TITLE = "Sign in to Airbyte Cloud"
IDENTIFIER_MAX_LENGTH = 63

CSP_POLICY = "default-src 'none'; style-src 'unsafe-inline'; img-src https: data:; base-uri 'none'"
"""Same policy FastMCP applies to its consent page. Deliberately no `form-action`:
Chrome enforces that directive on the redirect that follows a form post, which would
block the hop to Keycloak."""


@functools.cache
def _loader() -> PackageLoader:
    return PackageLoader(TEMPLATE_PACKAGE, TEMPLATE_DIR)


@functools.cache
def _environment() -> Environment:
    return Environment(
        loader=_loader(),
        autoescape=True,
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )


@functools.cache
def _stylesheet() -> str:
    source, _, _ = _loader().get_source(_environment(), LOGIN_STYLESHEET)
    return source


def render_login_page(
    *,
    txn_id: str,
    csrf_token: str,
    client_name: str,
    client_redirect_uri: str,
    company_identifier: str = "",
    error_message: str | None = None,
) -> str:
    """Render the identifier-entry page for one OAuth transaction.

    Both forms carry the transaction id and the CSRF token; the SSO form adds the
    company identifier, prefilled from `company_identifier` when the user has signed
    in with SSO before or is correcting a rejected value. `error_message` is shown
    inline above the forms.
    """
    return (
        _environment()
        .get_template(LOGIN_TEMPLATE)
        .render(
            title=PAGE_TITLE,
            csp_policy=CSP_POLICY,
            styles=_stylesheet(),
            txn_id=txn_id,
            csrf_token=csrf_token,
            client_name=client_name,
            client_redirect_uri=client_redirect_uri,
            company_identifier=company_identifier,
            error_message=error_message,
            choice_default=CHOICE_DEFAULT,
            choice_sso=CHOICE_SSO,
            identifier_max_length=IDENTIFIER_MAX_LENGTH,
        )
    )
