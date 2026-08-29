# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive connector configuration form MCP App."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Annotated, Any
from urllib.parse import urlparse

from fastmcp.apps import UI_MIME_TYPE, AppConfig, ResourceCSP
from fastmcp.tools.base import ToolResult
from fastmcp_extensions import mcp_resource
from pydantic import Field

from airbyte import exceptions as exc
from airbyte._util.registry_spec import get_connector_spec_from_registry
from airbyte.mcp._secret_intake import mint_intake_token
from airbyte.mcp._tool_utils import INTERACTIVE_UI_ANNOTATION, mcp_tool


CONNECTOR_FORM_RESOURCE_URI = "ui://airbyte/connector-config-form"
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"
DEFAULT_HTTP_PORT = 8080
DEFAULT_MCP_SERVER_URL = f"http://localhost:{DEFAULT_HTTP_PORT}"

_HTML_RESOURCE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Connector configuration</title>
<style>
body{font:14px system-ui,sans-serif;margin:0;padding:20px;color:#172033}
form{display:grid;gap:14px;max-width:620px}label{display:grid;gap:5px;font-weight:600}
input{border:1px solid #aab4c3;border-radius:5px;padding:8px;font:inherit}
button{background:#635bff;color:white;border:0;border-radius:5px;
padding:9px 14px;font:inherit;cursor:pointer}
#status{min-height:20px}.error{color:#b42318}.success{color:#067647}
</style></head>
<body><h2 id="title">Connector configuration</h2><form id="form"></form><p id="status"></p>
<script>
(() => {
  const state = { result: null };
  const form = document.getElementById("form");
  const status = document.getElementById("status");
  const title = document.getElementById("title");
  const post = (message) => window.parent.postMessage(message, "*");
  const pathValue = (object, path) => path.split(".").reduce((value, key) =>
    value && typeof value === "object" ? value[key] : undefined, object);
  const setPath = (object, path, value) => {
    const keys = path.split(".");
    let target = object;
    keys.slice(0, -1).forEach((key) => target = target[key] ||= {});
    target[keys[keys.length - 1]] = value;
  };
  const fields = (schema, prefix = "") => {
    const properties = schema && schema.properties || {};
    return Object.entries(properties).flatMap(([name, child]) => {
      const path = prefix ? `${prefix}.${name}` : name;
      if (child && child.type === "object" && child.properties)
        return fields(child, path);
      return [{ path, schema: child || {} }];
    });
  };
  const render = (result) => {
    state.result = result;
    title.textContent = `${result.connector_name} configuration`;
    form.replaceChildren();
    const required = new Set((result.spec_schema.required || []));
    fields(result.spec_schema).sort((a,b) =>
      (required.has(b.path.split(".").pop()) ? 1 : 0) -
      (required.has(a.path.split(".").pop()) ? 1 : 0) ||
      (a.schema.order || 999) - (b.schema.order || 999)
    ).forEach(({path, schema}) => {
      const label = document.createElement("label");
      label.textContent = `${schema.title || path}${required.has(path) ? " *" : ""}`;
      const input = document.createElement("input");
      input.name = path;
      input.type = result.secret_fields.includes(path) ? "password" : "text";
      const value = pathValue(result.non_secret_defaults || {}, path);
      if (value !== undefined)
        input.value = typeof value === "string" ? value : JSON.stringify(value);
      label.appendChild(input); form.appendChild(label);
    });
    const button = document.createElement("button");
    button.type = "submit"; button.textContent = "Save configuration"; form.appendChild(button);
  };
  form.addEventListener("submit", async (event) => {
    event.preventDefault(); status.className = ""; status.textContent = "Saving…";
    const visible = {}, secrets = {};
    form.querySelectorAll("input").forEach((input) => {
      if (!input.value) return;
      if (state.result.secret_fields.includes(input.name)) secrets[input.name] = input.value;
      else setPath(visible, input.name, input.value);
    });
    try {
      let body = {secret_refs: {}};
      if (Object.keys(secrets).length) {
        const response = await fetch(state.result.intake_endpoint, {
          method: "POST", headers: {"Authorization": `Bearer ${state.result.intake_token}`,
          "Content-Type": "application/json"}, body: JSON.stringify({secrets})
        });
        if (!response.ok) throw new Error("The server rejected the secret submission.");
        body = await response.json();
      }
      const payload = {status: "submitted", visible_config: visible, secret_refs: body.secret_refs};
      post({jsonrpc: "2.0", method: "ui/updateModelContext", params: {content: payload}});
      status.className = "success"; status.textContent = "Configuration submitted.";
    } catch (error) { status.className = "error"; status.textContent = error.message; }
  });
  window.addEventListener("message", (event) => {
    const message = event.data || {};
    const result = message.params && (message.params.tool_result || message.params.result);
    if (result) render(result.structuredContent || result.structured_content || result);
    if (message.method === "ui/notifications/tool-result" && message.params) render(message.params);
  });
  post({jsonrpc: "2.0", id: 1, method: "ui/initialize", params: {
    protocolVersion: "2025-06-18", capabilities: {},
    clientInfo: {name: "airbyte-form", version: "0.1"}
  }});
})();
</script></body></html>"""


def _server_origin() -> str:
    server_url = os.getenv(MCP_SERVER_URL_ENV, "").strip() or DEFAULT_MCP_SERVER_URL
    parsed = urlparse(server_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _intake_endpoint() -> str:
    return f"{_server_origin()}/secret-intake"


def _schema_secret_paths(schema: Mapping[str, Any], prefix: str = "") -> set[str]:
    paths: set[str] = set()
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        return paths
    for name, child in properties.items():
        if not isinstance(name, str) or not isinstance(child, Mapping):
            continue
        path = f"{prefix}.{name}" if prefix else name
        if child.get("airbyte_secret") is True:
            paths.add(path)
        paths.update(_schema_secret_paths(child, path))
    return paths


def _paths_present(value: object, prefix: str = "") -> set[str]:
    if not isinstance(value, Mapping):
        return set()
    paths: set[str] = set()
    for name, child in value.items():
        if not isinstance(name, str):
            continue
        path = f"{prefix}.{name}" if prefix else name
        paths.add(path)
        paths.update(_paths_present(child, path))
    return paths


@mcp_resource(
    CONNECTOR_FORM_RESOURCE_URI,
    "Self-contained Airbyte connector configuration form.",
    UI_MIME_TYPE,
)
def connector_config_form_resource() -> str:
    """Return the raw HTML connector configuration form."""
    return _HTML_RESOURCE


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    annotations={INTERACTIVE_UI_ANNOTATION: True},
    app=AppConfig(
        resource_uri=CONNECTOR_FORM_RESOURCE_URI,
        csp=ResourceCSP(connect_domains=[_server_origin()]),
    ),
)
def show_connector_config_form(
    connector_name: Annotated[
        str,
        Field(description="Connector name, such as `source-pokeapi`."),
    ],
    config_defaults: Annotated[
        dict[str, Any] | str | None,
        Field(
            description=(
                "Non-secret configuration defaults to prefill. Secret values must "
                "be entered by the user in the form."
            ),
        ),
    ] = None,
) -> ToolResult:
    """Show a hosted-safe connector configuration form."""
    if config_defaults is None:
        config_defaults = {}
    if isinstance(config_defaults, str):
        try:
            config_defaults = json.loads(config_defaults)
        except json.JSONDecodeError as error:
            raise exc.PyAirbyteInputError(
                message="config_defaults must be a JSON object.",
                context={"connector_name": connector_name},
            ) from error
    if not isinstance(config_defaults, dict):
        raise exc.PyAirbyteInputError(
            message="config_defaults must be an object.",
            context={"connector_name": connector_name},
        )

    spec_schema = get_connector_spec_from_registry(connector_name, platform="cloud")
    if spec_schema is None:
        spec_schema = get_connector_spec_from_registry(connector_name, platform="oss")
    if not spec_schema:
        raise exc.PyAirbyteInputError(
            message=f"Could not fetch a configuration schema for '{connector_name}'.",
            context={"connector_name": connector_name},
        )
    secret_fields = sorted(_schema_secret_paths(spec_schema))
    supplied_fields = _paths_present(config_defaults)
    supplied_secrets = sorted(supplied_fields.intersection(secret_fields))
    if supplied_secrets:
        raise exc.PyAirbyteInputError(
            message=(
                "Secret values cannot be provided in config_defaults. "
                "The user must enter them in the form."
            ),
            context={"secret_fields": supplied_secrets},
        )

    intake_token = mint_intake_token(secret_fields)
    content = json.dumps(
        {
            "connector_name": connector_name,
            "non_secret_defaults": config_defaults,
            "secret_fields": secret_fields,
            "note": (
                "The user will enter secrets in the form. The model will only "
                "receive opaque secret_intake:: references."
            ),
        },
        separators=(",", ":"),
    )
    structured_content = {
        "connector_name": connector_name,
        "schema": spec_schema,
        "spec_schema": spec_schema,
        "non_secret_defaults": config_defaults,
        "secret_fields": secret_fields,
        "intake_token": intake_token,
        "intake_endpoint": _intake_endpoint(),
    }
    return ToolResult(content=content, structured_content=structured_content)


__all__ = ["show_connector_config_form"]
