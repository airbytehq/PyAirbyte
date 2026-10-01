# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte guidance MCP tools and prompts.

.. include:: ../../docs/mcp-generated/guidance.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above. Setting `__all__` to an
# empty list tells pdoc (and other doc tools) not to surface the individual
# tool / helper definitions as a redundant "API Documentation" list.
__all__: list[str] = []

import contextlib
from typing import TYPE_CHECKING, Annotated, Any, Literal

import requests
from fastmcp import Context, FastMCP
from fastmcp_extensions import (
    mcp_prompt,
    mcp_tool,
    register_mcp_prompts,
    register_mcp_tools,
)
from pydantic import BaseModel, Field

from airbyte import exceptions as exc
from airbyte._util.registry_spec import get_connector_spec_from_registry
from airbyte.constants import is_hosted_mcp_mode
from airbyte.mcp._docs_results import AgentSkillDocsResult, render_agent_skill_docs_result
from airbyte.mcp._tool_utils import available_when
from airbyte.mcp.cloud import (
    CLOUD_AUTH_TIP_TEXT,
    SKILL_DOCS_SECTION_HINT,
    WORKSPACE_ID_TIP_TEXT,
    _get_cloud_workspace,
)
from airbyte.registry import (
    _DEFAULT_MANIFEST_URL,
    ApiDocsUrl,
    ConnectorMetadata,
    get_available_connectors,
    get_connector_api_docs_urls,
    get_connector_metadata,
)
from airbyte.secrets.util import try_get_secret
from airbyte.sources.util import get_source


if TYPE_CHECKING:
    from airbyte.cloud.workspaces import CloudWorkspace


KAPA_API_KEY_ENV_VAR = "KAPA_API_KEY"
KAPA_RETRIEVAL_API_URL_ENV_VAR = "KAPA_RETRIEVAL_API_URL"
_KAPA_TIMEOUT_SECONDS = 30.0


def _get_configured_value(name: str) -> str:
    secret = try_get_secret(name)
    return str(secret).strip() if secret is not None else ""


def is_kapa_configured() -> bool:
    """Return whether the hosted Kapa Retrieval API tool has both settings."""
    return (
        is_hosted_mcp_mode()
        and bool(_get_configured_value(KAPA_API_KEY_ENV_VAR))
        and bool(_get_configured_value(KAPA_RETRIEVAL_API_URL_ENV_VAR))
    )


@available_when(is_kapa_configured)
@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
)
def search_airbyte_knowledge_sources(
    query: Annotated[
        str,
        Field(
            description=(
                "A single, well-formed natural-language query. " "Must be a complete sentence."
            )
        ),
    ],
) -> list[dict[str, str]]:
    """Search Airbyte knowledge sources.

    Search Airbyte's documentation and other knowledge sources and return the most
    relevant chunks, each with its source URL and markdown content.

    Sources include documentation, the website, OpenAPI specifications, YouTube, and GitHub.
    """
    api_key = _get_configured_value(KAPA_API_KEY_ENV_VAR)
    url = _get_configured_value(KAPA_RETRIEVAL_API_URL_ENV_VAR)
    response = requests.post(
        url,
        headers={"X-API-KEY": api_key},
        json={"query": query},
        timeout=_KAPA_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return [
        {"source_url": item["source_url"], "content": item["content"]} for item in response.json()
    ]


TEST_MY_TOOLS_GUIDANCE = """
Test all available tools in this MCP server to confirm they are working properly.

Guidelines:
- Iterate through each tool systematically
- Use read-only operations whenever possible
- For tools that modify data, use test/safe modes or skip if no safe testing method exists
- Avoid creating persistent side effects (e.g., don't create real resources, connections, or data)
- Document which tools were tested and their status
- Report any errors or issues encountered
- Provide a summary of the test results at the end

Focus on validating that tools:
1. Accept their required parameters correctly
2. Return expected output formats
3. Handle errors gracefully
4. Connect to required services (if applicable)

Be efficient and practical in your testing approach.
""".strip()


@mcp_prompt(
    name="test-my-tools",
    description="Test all available MCP tools to confirm they are working properly",
)
def test_my_tools_prompt(
    scope: Annotated[
        str | None,
        Field(
            description=(
                "Optional free-form text to focus or constrain testing. "
                "This can be a single word, a sentence, or a paragraph "
                "describing the desired scope or constraints."
            ),
        ),
    ] = None,
) -> list[dict[str, str]]:
    """Generate a prompt that instructs the agent to test available tools."""
    content = TEST_MY_TOOLS_GUIDANCE

    if scope:
        content = f"{content}\n\n---\n\nAdditional scope or constraints:\n{scope}"

    return [
        {
            "role": "user",
            "content": content,
        }
    ]


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=SKILL_DOCS_SECTION_HINT + "\n\n" + CLOUD_AUTH_TIP_TEXT,
)
def get_agent_skill_docs(
    ctx: Context,
    *,
    docs_skill_id: Annotated[
        str | None,
        Field(
            description=(
                "Fully-qualified skill ID, e.g. from `describe_cloud_connector` `skill_id`. "
                "Provide this or `connector_id`."
            ),
            default=None,
        ),
    ] = None,
    connector_id: Annotated[
        str | None,
        Field(
            description=(
                "Deployed source or destination ID; resolves that connector's skill docs. "
                "Provide this or `docs_skill_id`."
            ),
            default=None,
        ),
    ] = None,
    section: Annotated[
        str | None,
        Field(
            description=(
                "Optional exact section ID from the guidance's outline to read a single "
                "section. Omit for the overview, metadata, and outline. " + SKILL_DOCS_SECTION_HINT
            ),
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentSkillDocsResult:
    """Returns the requested skill document by ID for an AI agent.

    Pass either a fully-qualified `docs_skill_id` or a `connector_id` (source or
    destination); exactly one is required.

    `section` is optional; if omitted, the summary overview is returned along with
    the list of available sections.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    return render_agent_skill_docs_result(
        workspace.get_agent_skill_docs(docs_skill_id, connector_id=connector_id, section=section)
    )


class ConnectorInfo(BaseModel):
    """@private Class to hold connector information."""

    connector_name: str
    connector_metadata: ConnectorMetadata | None = None
    documentation_url: str | None = None
    config_spec_jsonschema: dict | None = None
    manifest_url: str | None = None


@mcp_tool(
    read_only=True,
    idempotent=True,
)
def get_connector_info(
    connector_name: Annotated[
        str,
        Field(description="The name of the connector to get information for."),
    ],
) -> ConnectorInfo | Literal["Connector not found."]:
    """Get metadata, documentation URL, config spec, and manifest URL for a connector.

    `config_spec_jsonschema` is fetched from the public connector registry over
    HTTP (no Docker or local install required), preferring the `cloud` spec and
    falling back to `oss`. It is `None` when the registry has no spec available
    for the connector.
    """
    if connector_name not in get_available_connectors():
        return "Connector not found."

    connector = get_source(
        connector_name,
        install_if_missing=False,  # Defer to avoid failing entirely if it can't be installed.
    )

    connector_metadata: ConnectorMetadata | None = None
    with contextlib.suppress(Exception):
        connector_metadata = get_connector_metadata(connector_name)

    # Resolve the config spec from the public registry endpoint keyed by version,
    # which works in any runtime (hosted or local) without installing the
    # connector. Prefer the `cloud` spec and fall back to `oss`.
    version = connector_metadata.latest_available_version if connector_metadata else None
    config_spec_jsonschema: dict[str, Any] | None = get_connector_spec_from_registry(
        connector_name,
        version=version,
        platform="cloud",
    )
    if config_spec_jsonschema is None:
        config_spec_jsonschema = get_connector_spec_from_registry(
            connector_name,
            version=version,
            platform="oss",
        )

    manifest_url = _DEFAULT_MANIFEST_URL.format(
        source_name=connector_name,
        version="latest",
    )

    return ConnectorInfo(
        connector_name=connector.name,
        connector_metadata=connector_metadata,
        documentation_url=connector.docs_url,
        config_spec_jsonschema=config_spec_jsonschema,
        manifest_url=manifest_url,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
)
def get_api_docs_urls(
    connector_name: Annotated[
        str,
        Field(
            description=(
                "The canonical connector name "
                "(e.g., 'source-facebook-marketing', 'destination-snowflake')"
            )
        ),
    ],
) -> list[ApiDocsUrl] | Literal["Connector not found."]:
    """Get API documentation URLs for a connector.

    This tool retrieves documentation URLs for a connector's upstream API from multiple sources:
    - Registry metadata (documentationUrl, externalDocumentationUrls)
    - Connector manifest.yaml file (data.externalDocumentationUrls)
    """
    try:
        return get_connector_api_docs_urls(connector_name)
    except exc.AirbyteConnectorNotRegisteredError:
        return "Connector not found."


def register_guidance_tools(app: FastMCP) -> None:
    """Register guidance tools and prompts with the FastMCP app."""
    register_mcp_tools(app, mcp_module=__name__)
    register_mcp_prompts(app, mcp_module=__name__)
