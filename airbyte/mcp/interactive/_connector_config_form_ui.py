# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive connector configuration form MCP App."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Annotated, Any, Literal
from urllib.parse import urlencode, urlparse

from fastmcp import Context  # noqa: TC002
from fastmcp.apps import UI_MIME_TYPE, AppConfig, ResourceCSP
from fastmcp.tools.base import ToolResult
from fastmcp_extensions import get_mcp_config
from pydantic import Field
from starlette.responses import HTMLResponse

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte._util.registry_spec import get_connector_spec_from_registry
from airbyte.constants import (
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.mcp._config_submit import (
    ConfigSubmitError,
    _schema_secret_paths,
    decrypt_action_token,
    initiate_oauth_flow,
    mint_action_token,
)
from airbyte.mcp._tool_utils import (
    INTERACTIVE_UI_ANNOTATION,
    _resolve_transport_bearer_token,
    mcp_tool,
)


if TYPE_CHECKING:
    from fastmcp import FastMCP
    from starlette.requests import Request


CONNECTOR_FORM_RESOURCE_URI = "ui://airbyte/connector-config-form"
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"
DEFAULT_HTTP_PORT = 8080
DEFAULT_MCP_SERVER_URL = f"http://localhost:{DEFAULT_HTTP_PORT}"
_MAX_AGENT_CONTENT_LENGTH = 12_000

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
window.__AIRBYTE_FORM_PAYLOAD__ = null;
(() => {
  const state = { result: null };
  const standalonePayload = window.__AIRBYTE_FORM_PAYLOAD__;
  const standalone = standalonePayload !== null;
  const form = document.getElementById("form");
  const status = document.getElementById("status");
  const title = document.getElementById("title");
  const post = (message) => window.parent.postMessage(message, "*");
  const setStatus = (className, message, resultStatus) => {
    status.className = className;
    status.textContent = message;
    if (standalone || resultStatus === "pending") return;
    const notify = document.createElement("button");
    notify.type = "button";
    notify.textContent = "Notify agent";
    notify.addEventListener("click", () => post({
      jsonrpc: "2.0",
      method: "ui/message",
      params: {content: [{
        type: "text",
        text: `Connector configuration ${state.result.action} result: ${resultStatus}`
      }]}
    }));
    status.appendChild(document.createTextNode(" "));
    status.appendChild(notify);
  };
  const requestResize = () => {
    if (standalone) return;
    const contentHeight = Math.max(
      document.documentElement.scrollHeight,
      document.body.scrollHeight
    );
    post({jsonrpc: "2.0", method: "ui/notifications/size-changed",
      params: {height: Math.min(contentHeight + 20, 600)}});
  };
  const pathValue = (object, path) => path.split(".").reduce((value, key) =>
    value && typeof value === "object" ? value[key] : undefined, object);
  const setPath = (object, path, value) => {
    const keys = path.split(".");
    if (keys.some((key) => ["__proto__", "constructor", "prototype"].includes(key))) return;
    let target = object;
    keys.slice(0, -1).forEach((key) => {
      if (typeof target[key] !== "object" || target[key] === null || Array.isArray(target[key]))
        target[key] = {};
      target = target[key];
    });
    target[keys[keys.length - 1]] = value;
  };
  const setInputPath = (target, input) => {
    if (!input.value && input.type !== "hidden") return;
    const value = input.dataset.arrayField === "1"
      ? input.value.split(",").map((item) => item.trim()).filter(Boolean)
      : input.value;
    setPath(target, input.name, value);
  };
  const collectConfig = (includeSecrets) => {
    const config = JSON.parse(JSON.stringify(state.result.non_secret_defaults || {}));
    form.querySelectorAll("input").forEach((input) => {
      if (!includeSecrets && state.result.secret_fields.includes(input.name)) return;
      setInputPath(config, input);
    });
    return config;
  };
  const fields = (schema, prefix = "") => {
    const properties = schema && schema.properties || {};
    return Object.entries(properties).flatMap(([name, child]) => {
      const path = prefix ? `${prefix}.${name}` : name;
      if (child && ["oneOf", "anyOf", "allOf"].some((key) => Array.isArray(child[key])))
        return [{ path, schema: child }];
      if (child && child.type === "object" && child.properties)
        return fields(child, path);
      return [{ path, schema: child || {} }];
    });
  };
  const discriminatorEntries = (schema) =>
    Object.entries(schema && schema.properties || {}).filter(([, property]) =>
      property && typeof property === "object" &&
      (Object.prototype.hasOwnProperty.call(property, "const") ||
        (Array.isArray(property.enum) && property.enum.length === 1))
    );
  const discriminatorValue = (property) =>
    Object.prototype.hasOwnProperty.call(property, "const")
      ? property.const
      : property.enum[0];
  const render = (result) => {
    state.result = result;
    title.textContent = `${result.connector_name} configuration`;
    form.replaceChildren();
    const required = new Set((result.spec_schema.required || []));
    fields(result.spec_schema).sort((a,b) =>
      (required.has(b.path.split(".").pop()) ? 1 : 0) -
      (required.has(a.path.split(".").pop()) ? 1 : 0) ||
      (a.schema.order || 999) - (b.schema.order || 999)
    ).forEach(({path, schema}) => renderField(
      form, path, schema, required.has(path.split(".").pop())
    ));
    renderOAuthButton();
    const button = document.createElement("button");
    button.type = "submit"; button.textContent = "Save configuration"; form.appendChild(button);
    requestResize();
  };
  const renderField = (container, path, schema, required) => {
      const label = document.createElement("label");
      label.textContent = `${schema.title || path}${required ? " *" : ""}`;
      const branches = Array.isArray(schema.oneOf) && schema.oneOf.length
        ? schema.oneOf
        : Array.isArray(schema.anyOf) && schema.anyOf.length
          ? schema.anyOf
          : null;
      if (branches && branches.every((branch) =>
        branch && typeof branch === "object" && !Array.isArray(branch)
      )) {
        const select = document.createElement("select");
        branches.forEach((branch, index) => {
          const option = document.createElement("option");
          option.value = String(index);
          option.textContent = branch.title || `Option ${index + 1}`;
          select.appendChild(option);
        });
        const branchContainer = document.createElement("div");
        const defaults = state.result.non_secret_defaults || {};
        const selectedBranch = branches.findIndex((branch) =>
          discriminatorEntries(branch).some(([name, property]) => {
            const branchPath = path ? `${path}.${name}` : name;
            return pathValue(defaults, branchPath) === discriminatorValue(property);
          })
        );
        select.value = String(selectedBranch < 0 ? 0 : selectedBranch);
        const renderBranch = (index) => {
          branchContainer.replaceChildren();
          const branch = branches[index];
          const discriminators = discriminatorEntries(branch);
          discriminators.forEach(([name, property]) => {
            const input = document.createElement("input");
            input.type = "hidden";
            input.name = path ? `${path}.${name}` : name;
            const value = discriminatorValue(property);
            input.value = typeof value === "string" ? value : JSON.stringify(value);
            branchContainer.appendChild(input);
          });
          const discriminatorPaths = new Set(discriminators.map(([name]) =>
            path ? `${path}.${name}` : name
          ));
          const branchRequired = new Set(branch.required || []);
          fields(branch, path)
            .filter(({path: fieldPath}) => !discriminatorPaths.has(fieldPath))
            .sort((a, b) =>
              (branchRequired.has(b.path.split(".").pop()) ? 1 : 0) -
              (branchRequired.has(a.path.split(".").pop()) ? 1 : 0) ||
              (a.schema.order || 999) - (b.schema.order || 999)
            )
            .forEach(({path: fieldPath, schema: fieldSchema}) =>
              renderField(
                branchContainer,
                fieldPath,
                fieldSchema,
                branchRequired.has(fieldPath.split(".").pop())
              )
            );
        };
        select.addEventListener("change", () => {
          renderBranch(Number(select.value));
          requestResize();
        });
        label.appendChild(select);
        label.appendChild(branchContainer);
        container.appendChild(label);
        renderBranch(Number(select.value));
        return;
      }
      if (["oneOf", "anyOf", "allOf"].some((key) => Array.isArray(schema[key]))) {
        const notice = document.createElement("span");
        notice.textContent = "Complex authentication objects are not supported by this form.";
        notice.setAttribute("aria-disabled", "true");
        label.appendChild(notice); container.appendChild(label);
        return;
      }
      const input = document.createElement("input");
      input.name = path;
      input.type = state.result.secret_fields.includes(path) ? "password" : "text";
      if (schema.type === "array") {
        input.dataset.arrayField = "1";
        input.placeholder = "Comma-separated values";
      }
      const value = pathValue(state.result.non_secret_defaults || {}, path);
      if (value !== undefined)
        input.value = schema.type === "array" && Array.isArray(value)
          ? value.join(", ")
          : typeof value === "string" ? value : JSON.stringify(value);
      label.appendChild(input); container.appendChild(label);
  };
  const renderOAuthButton = () => {
    if (!state.result.oauth_supported) return;
    const wrapper = document.createElement("div");
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Sign in with OAuth (via Airbyte Cloud)";
    button.addEventListener("click", async () => {
      button.disabled = true;
      setStatus("", "Starting OAuth sign-in…", "pending");
      try {
        const response = await fetch(state.result.oauth_start_endpoint, {
          method: "POST",
          headers: {"Authorization": `Bearer ${state.result.submit_token}`,
            "Content-Type": "application/json"},
          body: JSON.stringify({config: collectConfig(false)})
        });
        if (!response.ok) throw new Error("OAuth sign-in could not be started.");
        const body = await response.json();
        const consentUrl = body.consent_url;
        window.open(consentUrl, "_blank");
        const link = document.createElement("a");
        link.href = consentUrl;
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = "Open consent page";
        status.textContent = (
          "Complete sign-in in the new tab; the source will be created automatically. "
        );
        status.appendChild(link);
      } catch (error) {
        setStatus("error", "OAuth sign-in could not be started.", "error");
      } finally {
        button.disabled = false;
        requestResize();
      }
    });
    wrapper.appendChild(button);
    form.appendChild(wrapper);
  };
  form.addEventListener("submit", async (event) => {
    event.preventDefault(); setStatus("", "Saving…", "pending");
    requestResize();
    const config = collectConfig(true);
    const visible = collectConfig(false);
    try {
      const response = await fetch(state.result.submit_endpoint, {
        method: "POST", headers: {"Authorization": `Bearer ${state.result.submit_token}`,
        "Content-Type": "application/json"}, body: JSON.stringify({config})
      });
      if (!response.ok) throw new Error("The server rejected the configuration.");
      const body = await response.json();
      const payload = {status: body.status, action: body.action, visible_config: visible};
      if (typeof body.connector_id === "string") payload.connector_id = body.connector_id;
      if (typeof body.connector_url === "string") payload.connector_url = body.connector_url;
      if (!standalone)
        post({jsonrpc: "2.0", method: "ui/updateModelContext", params: {content: payload}});
      setStatus("success", "Configuration submitted.", "success");
      requestResize();
    } catch (error) {
      setStatus("error", "The configuration could not be submitted.", "error");
      requestResize();
    }
  });
  if (standalonePayload) {
    render(standalonePayload);
  } else {
    window.addEventListener("message", (event) => {
      if (event.source !== window.parent) return;
      const message = event.data || {};
      if (message.id === 1 && message.result) {
        post({jsonrpc: "2.0", method: "ui/notifications/initialized"});
        return;
      }
      if (message.method === "ui/notifications/tool-result" && message.params) {
        render(
          message.params.structuredContent ||
          message.params.structured_content ||
          message.params
        );
        return;
      }
      const result = message.params && (message.params.tool_result || message.params.result);
      if (result) render(result.structuredContent || result.structured_content || result);
    });
    post({jsonrpc: "2.0", id: 1, method: "ui/initialize", params: {
      protocolVersion: "2025-06-18", appCapabilities: {},
      appInfo: {name: "airbyte-form", version: "0.1"}
    }});
  }
})();
</script></body></html>"""


def _server_url() -> str:
    server_url = os.getenv(MCP_SERVER_URL_ENV, "").strip() or DEFAULT_MCP_SERVER_URL
    parsed = urlparse(server_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise exc.PyAirbyteInputError(
            message="MCP_SERVER_URL must be a valid HTTP(S) URL.",
        )
    if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1"}:
        raise exc.PyAirbyteInputError(
            message="MCP_SERVER_URL must use HTTPS outside localhost.",
        )
    return server_url.rstrip("/")


def _server_origin() -> str:
    parsed = urlparse(_server_url())
    return f"{parsed.scheme}://{parsed.netloc}"


def _submit_endpoint() -> str:
    return f"{_server_url()}/connector-config-submit"


def _oauth_start_endpoint() -> str:
    return f"{_server_url()}/connector-config-oauth-start"


def _standalone_endpoint(token: str) -> str:
    return f"{_server_url()}/connector-config-form?{urlencode({'token': token})}"


def _cloud_config_value(env_name: str) -> str | None:
    value = os.getenv(env_name, "").strip()
    return value or None


def _resolved_cloud_config_value(
    ctx: Context | None,
    config_name: str,
    env_name: str,
) -> str | None:
    if ctx is not None:
        configured = get_mcp_config(ctx, config_name)
        if configured:
            return configured
    return _cloud_config_value(env_name)


def _mint_form_token(
    connector_name: str,
    *,
    config_defaults: dict[str, Any],
    ctx: Context | None,
    source_id: str | None,
    workspace_id: str | None,
    source_name: str | None,
) -> tuple[str, str]:
    resolved_workspace_id = workspace_id or _resolved_cloud_config_value(
        ctx, "workspace_id", CLOUD_WORKSPACE_ID_ENV_VAR
    )
    bearer_token = _resolve_transport_bearer_token() or _resolved_cloud_config_value(
        ctx, "bearer_token", CLOUD_BEARER_TOKEN_ENV_VAR
    )
    client_id = _resolved_cloud_config_value(ctx, "client_id", CLOUD_CLIENT_ID_ENV_VAR)
    client_secret = _resolved_cloud_config_value(ctx, "client_secret", CLOUD_CLIENT_SECRET_ENV_VAR)
    has_credentials = bool(bearer_token or (client_id and client_secret))
    action: Literal["create", "update", "validate"]
    if source_id:
        if not (has_credentials and resolved_workspace_id):
            raise exc.PyAirbyteInputError(
                message="Cloud credentials and a workspace ID are required to update a source."
            )
        action = "update"
    elif has_credentials and resolved_workspace_id:
        action = "create"
    else:
        action = "validate"
    token = mint_action_token(
        action,
        connector_name,
        non_secret_defaults=config_defaults,
        workspace_id=resolved_workspace_id,
        source_id=source_id,
        source_name=source_name,
        bearer_token=bearer_token,
        client_id=client_id if not bearer_token else None,
        client_secret=client_secret if not bearer_token else None,
        api_url=_resolved_cloud_config_value(ctx, "api_url", CLOUD_API_ROOT_ENV_VAR),
        config_api_url=_resolved_cloud_config_value(
            ctx, "config_api_url", CLOUD_CONFIG_API_ROOT_ENV_VAR
        ),
    )
    return action, token


def _paths_present(value: object, prefix: str = "") -> set[str]:
    if isinstance(value, (list, tuple)):
        paths: set[str] = set()
        for item in value:
            paths.update(_paths_present(item, prefix))
        return paths
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


def _agent_content(
    connector_name: str,
    config_defaults: dict[str, Any],
    secret_fields: list[str],
    standalone_url: str,
    *,
    oauth_supported: bool,
) -> str:
    """Build bounded text for agents that cannot render the form."""
    note = (
        "The user will enter secrets in the form. The model will only receive "
        "a confirmation and non-secret configuration."
    )
    if oauth_supported:
        note += " The user may complete OAuth out-of-band; only safe confirmation returns."
    payload: dict[str, Any] = {
        "connector_name": connector_name,
        "non_secret_defaults": config_defaults,
        "secret_fields": secret_fields,
        "standalone_url": standalone_url,
        "note": note,
        "agent_preview_max_chars": _MAX_AGENT_CONTENT_LENGTH,
        "oauth_supported": oauth_supported,
    }
    content = json.dumps(payload, separators=(",", ":"))
    if len(content) <= _MAX_AGENT_CONTENT_LENGTH:
        return content

    payload["non_secret_defaults"] = {}
    payload["agent_preview_truncated"] = True
    content = json.dumps(payload, separators=(",", ":"))
    if len(content) <= _MAX_AGENT_CONTENT_LENGTH:
        return content

    payload["secret_fields"] = []
    payload["connector_name"] = connector_name[:256]
    return json.dumps(payload, separators=(",", ":"))


def _connector_spec_schema(connector_name: str) -> dict[str, Any] | None:
    spec_schema = get_connector_spec_from_registry(connector_name, platform="cloud")
    if spec_schema is None:
        spec_schema = get_connector_spec_from_registry(connector_name, platform="oss")
    return spec_schema


def _form_payload(
    connector_name: str,
    spec_schema: dict[str, Any],
    config_defaults: dict[str, Any],
    secret_fields: list[str],
    action: str,
    submit_token: str,
) -> dict[str, Any]:
    oauth_supported = action in {"create", "update"} and api_util.is_oauth_source_type(
        connector_name.removeprefix("source-")
    )
    return {
        "connector_name": connector_name,
        "schema": spec_schema,
        "spec_schema": spec_schema,
        "non_secret_defaults": config_defaults,
        "secret_fields": secret_fields,
        "action": action,
        "submit_token": submit_token,
        "submit_endpoint": _submit_endpoint(),
        "oauth_start_endpoint": _oauth_start_endpoint(),
        "oauth_supported": oauth_supported,
    }


def connector_config_form_page(payload: Mapping[str, Any]) -> str:
    """Render the connector configuration form as a standalone browser page."""
    encoded_payload = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).replace(
        "</", "<\\/"
    )
    assignment = f"window.__AIRBYTE_FORM_PAYLOAD__ = {encoded_payload};"
    return _HTML_RESOURCE.replace(
        "window.__AIRBYTE_FORM_PAYLOAD__ = null;",
        assignment,
        1,
    )


_INVALID_FORM_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Invalid form link</title></head>
<body><p>This link is invalid or has expired.</p></body></html>"""


async def connector_config_form_endpoint(request: Request) -> HTMLResponse:  # noqa: RUF029
    """Render a connector configuration form from a one-shot action capability."""
    token = request.query_params.get("token", "")
    if not token:
        return HTMLResponse(_INVALID_FORM_PAGE, status_code=403)
    try:
        claims = decrypt_action_token(token, consume=False)
        connector_name = claims["connector_name"]
        config_defaults = claims.get("non_secret_defaults", {})
        if not isinstance(config_defaults, dict):
            return HTMLResponse(_INVALID_FORM_PAGE, status_code=403)
        spec_schema = _connector_spec_schema(connector_name)
        if not spec_schema:
            return HTMLResponse(_INVALID_FORM_PAGE, status_code=403)
        secret_fields = sorted(_schema_secret_paths(spec_schema))
        payload = _form_payload(
            connector_name,
            spec_schema,
            config_defaults,
            secret_fields,
            claims["action"],
            token,
        )
    except (ConfigSubmitError, exc.PyAirbyteError, KeyError, TypeError, ValueError):
        return HTMLResponse(_INVALID_FORM_PAGE, status_code=403)
    try:
        return HTMLResponse(connector_config_form_page(payload))
    except (TypeError, ValueError):
        return HTMLResponse(_INVALID_FORM_PAGE, status_code=403)


def connector_config_form_resource() -> str:
    """Return the raw HTML connector configuration form."""
    return _HTML_RESOURCE


def _connector_form_app_config() -> AppConfig:
    return AppConfig(
        resource_uri=CONNECTOR_FORM_RESOURCE_URI,
        csp=ResourceCSP(connect_domains=[_server_origin()]),
    )


def register_connector_config_form_resource(app: FastMCP) -> None:
    """Register the connector form resource with its CSP metadata."""
    app.resource(
        CONNECTOR_FORM_RESOURCE_URI,
        description="Self-contained Airbyte connector configuration form.",
        mime_type=UI_MIME_TYPE,
        app=AppConfig(csp=ResourceCSP(connect_domains=[_server_origin()])),
    )(connector_config_form_resource)


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    annotations={INTERACTIVE_UI_ANNOTATION: True},
    app=_connector_form_app_config,
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
    *,
    ctx: Context | None = None,
    source_id: Annotated[
        str | None,
        Field(description="Existing Cloud source ID to update.", default=None),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(description="Cloud workspace ID for creating the source.", default=None),
    ] = None,
    source_name: Annotated[
        str | None,
        Field(description="Name to use when creating the source.", default=None),
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

    spec_schema = _connector_spec_schema(connector_name)
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

    action, submit_token = _mint_form_token(
        connector_name,
        config_defaults=config_defaults,
        ctx=ctx,
        source_id=source_id,
        workspace_id=workspace_id,
        source_name=source_name,
    )
    standalone_url = _standalone_endpoint(submit_token)
    content = _agent_content(
        connector_name,
        config_defaults,
        secret_fields,
        standalone_url,
        oauth_supported=action in {"create", "update"}
        and api_util.is_oauth_source_type(connector_name.removeprefix("source-")),
    )
    structured_content = _form_payload(
        connector_name,
        spec_schema,
        config_defaults,
        secret_fields,
        action,
        submit_token,
    )
    structured_content["standalone_url"] = standalone_url
    return ToolResult(content=content, structured_content=structured_content)


@mcp_tool(
    read_only=True,
    idempotent=False,
    open_world=True,
    annotations={INTERACTIVE_UI_ANNOTATION: False},
)
def start_connector_oauth(
    connector_name: Annotated[
        str,
        Field(description="Connector name, such as `source-github`."),
    ],
    config: Annotated[
        dict[str, Any] | str | None,
        Field(description="Non-secret connector configuration."),
    ] = None,
    *,
    source_name: Annotated[
        str,
        Field(description="Name to use when creating the source."),
    ],
    workspace_id: Annotated[
        str | None,
        Field(description="Cloud workspace ID for creating the source.", default=None),
    ] = None,
    source_id: Annotated[
        str | None,
        Field(description="Existing Cloud source ID to update.", default=None),
    ] = None,
    ctx: Context | None = None,
) -> ToolResult:
    """Start connector OAuth in a browser without rendering a form."""
    if config is None:
        config = {}
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError as error:
            raise exc.PyAirbyteInputError(
                message="config must be a JSON object.",
                context={"connector_name": connector_name},
            ) from error
    if not isinstance(config, dict):
        raise exc.PyAirbyteInputError(
            message="config must be an object.",
            context={"connector_name": connector_name},
        )
    spec_schema = _connector_spec_schema(connector_name)
    if not spec_schema:
        raise exc.PyAirbyteInputError(
            message=f"Could not fetch a configuration schema for '{connector_name}'.",
            context={"connector_name": connector_name},
        )
    secret_fields = sorted(_schema_secret_paths(spec_schema))
    supplied_secrets = sorted(_paths_present(config).intersection(secret_fields))
    if supplied_secrets:
        raise exc.PyAirbyteInputError(
            message="Secret values cannot be provided in config.",
            context={"secret_fields": supplied_secrets},
        )
    action, token = _mint_form_token(
        connector_name,
        config_defaults=config,
        ctx=ctx,
        source_id=source_id,
        workspace_id=workspace_id,
        source_name=source_name,
    )
    if action == "validate":
        raise exc.PyAirbyteInputError(
            message="Cloud credentials and a workspace ID are required to start OAuth."
        )
    if not api_util.is_oauth_source_type(connector_name.removeprefix("source-")):
        raise exc.PyAirbyteInputError(
            message=f"Connector '{connector_name}' does not support OAuth."
        )
    claims = decrypt_action_token(token, consume=False)
    consent_url, _ = initiate_oauth_flow(
        claims,
        config,
        server_url=_server_url(),
    )
    safe_config = claims.get("non_secret_defaults", {})
    result = {
        "consent_url": consent_url,
        "connector_name": connector_name,
        "action": action,
        "non_secret_config": safe_config,
        "note": (
            "Complete authentication in the browser. The agent will only receive "
            "safe confirmation and will not receive the OAuth secret."
        ),
    }
    return ToolResult(
        content=json.dumps(result, separators=(",", ":")),
        structured_content=result,
    )


__all__ = [
    "connector_config_form_endpoint",
    "start_connector_oauth",
    "show_connector_config_form",
]
