#!/usr/bin/env python3
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Generate Markdown documentation for the PyAirbyte MCP server.

Runs `fastmcp inspect` against `airbyte.mcp.server:app` to obtain the full
FastMCP protocol surface (tools, resources, resource templates, prompts) as a
JSON report, then renders it into a small set of Markdown files under
`docs/mcp-generated/`.

The output is designed to be:

- **Docusaurus-hostable**: each file starts with YAML front-matter (`title`,
  `sidebar_label`, `description`); the body is plain CommonMark + GFM tables +
  `<details><summary>` blocks for collapsible JSON schemas. No MDX-only
  components are used.
- **`pdoc3`-compatible**: standard Markdown that renders correctly alongside
  the existing `pdoc3` output in `docs/generated/` without any special config.
- **Deep-linkable**: every tool/resource/prompt name is an H2 with a stable
  slug anchor (e.g. `tools.md#list_connectors`).

Formatting is deliberately modeled on the
[`mcpdocs-gen`](https://github.com/smytsyk/mcpdocs) static HTML output — same
sections, same per-tool shape (description → parameters table → JSON schema) —
but emitted as Markdown rather than HTML so it can slot into an existing docs
site.

Usage:

```
uv run python scripts/generate_mcp_markdown.py [--output docs/mcp-generated]
```

Or via the project's poe task:

```
poe mcp-docs-md
```
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT = Path("docs/mcp-generated")
DEFAULT_SERVER_SPEC = "airbyte/mcp/server.py:app"


def _run_fastmcp_inspect(server_spec: str, report_path: Path) -> dict[str, Any]:
    """Invoke `fastmcp inspect` and return the parsed JSON report."""
    fastmcp_bin = shutil.which("fastmcp")
    if fastmcp_bin is None:
        raise RuntimeError(
            "`fastmcp` CLI not found on PATH. Install project dev deps first "
            "(e.g. `uv sync --group dev`) and re-run from the repo root."
        )
    subprocess.run(
        [
            fastmcp_bin,
            "inspect",
            server_spec,
            "--format",
            "fastmcp",
            "--output",
            str(report_path),
        ],
        check=True,
    )
    return json.loads(report_path.read_text(encoding="utf-8"))


def _fmt_type(schema: dict[str, Any]) -> str:
    """Render a JSON-schema fragment as a short, human-readable type string."""
    for key in ("anyOf", "oneOf"):
        if key in schema:
            return " | ".join(_fmt_type(s) for s in schema[key])
    if "enum" in schema:
        return "enum(" + ", ".join(json.dumps(v) for v in schema["enum"]) + ")"
    t = schema.get("type")
    if t == "array":
        items = schema.get("items", {})
        return f"array<{_fmt_type(items)}>" if items else "array"
    if isinstance(t, list):
        return " | ".join(str(x) for x in t)
    return str(t) if t else "any"


def _escape_table_cell(value: str) -> str:
    """Make a string safe to embed in a single GFM table cell."""
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _fmt_default(schema: dict[str, Any]) -> str:
    """Render a schema's `default` value as a compact Markdown code span."""
    if "default" not in schema:
        return "—"
    default = schema["default"]
    if default is None:
        return "`null`"
    return f"`{json.dumps(default)}`"


def _frontmatter(title: str, sidebar_label: str, description: str) -> str:
    """Build a YAML front-matter block for a Docusaurus page."""
    esc_desc = description.replace("\n", " ").replace('"', '\\"').strip()
    return (
        "---\n"
        f"title: {title}\n"
        f"sidebar_label: {sidebar_label}\n"
        f'description: "{esc_desc}"\n'
        "---\n\n"
    )


def _json_block(label: str, obj: Any) -> str:  # noqa: ANN401
    """Render an object inside a collapsible `<details>` JSON code block."""
    return (
        f"<details>\n<summary>{label}</summary>\n\n"
        "```json\n" + json.dumps(obj, indent=2) + "\n```\n\n</details>\n\n"
    )


def _render_parameters_table(input_schema: dict[str, Any]) -> str:
    """Render a GFM parameters table for a tool's `input_schema`."""
    properties = input_schema.get("properties") or {}
    if not properties:
        return "_No parameters._\n\n"
    required = set(input_schema.get("required") or [])
    lines = [
        "| Name | Type | Required | Default | Description |",
        "| --- | --- | --- | --- | --- |",
    ]
    for name, prop in properties.items():
        desc = _escape_table_cell(prop.get("description", ""))
        # Union types contain literal `|` chars which break GFM table rendering
        # even inside backticks in some parsers; escape defensively.
        type_cell = _fmt_type(prop).replace("|", "\\|")
        lines.append(
            f"| `{name}` | `{type_cell}` | "
            f"{'yes' if name in required else 'no'} | "
            f"{_fmt_default(prop)} | {desc} |"
        )
    return "\n".join(lines) + "\n\n"


def _render_tool(tool: dict[str, Any]) -> str:
    """Render a single tool as a Markdown section."""
    name = tool["name"]
    parts: list[str] = [f"## `{name}` {{#{name}}}\n\n"]
    if description := tool.get("description"):
        parts.append(description.strip() + "\n\n")
    if tags := tool.get("tags"):
        parts.append("**Tags:** " + ", ".join(f"`{t}`" for t in tags) + "\n\n")
    parts.extend(
        [
            "### Parameters\n\n",
            _render_parameters_table(tool.get("input_schema") or {}),
        ]
    )
    if input_schema := tool.get("input_schema"):
        parts.append(_json_block("Show input JSON schema", input_schema))
    if output_schema := tool.get("output_schema"):
        parts.append(_json_block("Show output JSON schema", output_schema))
    return "".join(parts)


def _render_resource(resource: dict[str, Any]) -> str:
    """Render a single resource as a Markdown section."""
    name = resource["name"]
    parts: list[str] = [f"## `{name}` {{#{name}}}\n\n"]
    if description := resource.get("description"):
        parts.append(description.strip() + "\n\n")
    meta_lines: list[str] = []
    if uri := resource.get("uri"):
        meta_lines.append(f"- **URI:** `{uri}`")
    if uri_template := resource.get("uri_template"):
        meta_lines.append(f"- **URI template:** `{uri_template}`")
    if mime := resource.get("mime_type"):
        meta_lines.append(f"- **MIME type:** `{mime}`")
    if tags := resource.get("tags"):
        meta_lines.append("- **Tags:** " + ", ".join(f"`{t}`" for t in tags))
    if meta_lines:
        parts.append("\n".join(meta_lines) + "\n\n")
    return "".join(parts)


def _render_prompt(prompt: dict[str, Any]) -> str:
    """Render a single prompt as a Markdown section."""
    name = prompt["name"]
    parts: list[str] = [f"## `{name}` {{#{name}}}\n\n"]
    if description := prompt.get("description"):
        parts.append(description.strip() + "\n\n")
    args = prompt.get("arguments") or []
    if args:
        parts.extend(
            [
                "### Arguments\n\n",
                "| Name | Required | Description |\n| --- | --- | --- |\n",
            ]
        )
        for arg in args:
            desc = _escape_table_cell(arg.get("description", ""))
            required = "yes" if arg.get("required") else "no"
            parts.append(f"| `{arg['name']}` | {required} | {desc} |\n")
        parts.append("\n")
    else:
        parts.append("_No arguments._\n\n")
    return "".join(parts)


def _render_index(report: dict[str, Any]) -> str:
    """Render the top-level overview page."""
    server = report.get("server") or {}
    server_name = server.get("name", "mcp-server")
    out = _frontmatter(
        title=f"{server_name} — MCP server",
        sidebar_label="Overview",
        description=(server.get("instructions") or "").splitlines()[0]
        or f"Auto-generated docs for the {server_name} MCP server.",
    )
    out += f"# `{server_name}`\n\n"
    if version := server.get("version"):
        out += f"**Version:** `{version}`  \n"
    if proto := server.get("protocol_version"):
        out += f"**Protocol version:** `{proto}`  \n"
    if fastmcp_version := server.get("fastmcp_version"):
        out += f"**FastMCP version:** `{fastmcp_version}`  \n"
    out += "\n"
    if instructions := server.get("instructions"):
        out += instructions.strip() + "\n\n"
    out += "## Contents\n\n"
    counts = {
        "tools": len(report.get("tools") or []),
        "resources": (len(report.get("resources") or []) + len(report.get("templates") or [])),
        "prompts": len(report.get("prompts") or []),
    }
    out += (
        f"- [Tools](./tools.md) — {counts['tools']}\n"
        f"- [Resources](./resources.md) — {counts['resources']}\n"
        f"- [Prompts](./prompts.md) — {counts['prompts']}\n\n"
    )
    out += (
        "> These pages are generated from the live `fastmcp inspect` report. "
        "Regenerate with `poe mcp-docs-md`.\n"
    )
    return out


def _render_tools_page(report: dict[str, Any]) -> str:
    """Render the tools page."""
    tools = report.get("tools") or []
    out = _frontmatter(
        title="Tools",
        sidebar_label="Tools",
        description=f"All {len(tools)} MCP tools exposed by this server.",
    )
    out += "# Tools\n\n"
    if not tools:
        out += "_No tools are exposed by this server._\n"
        return out
    out += f"This server exposes **{len(tools)}** tool(s).\n\n"
    out += "**Index:** "
    out += ", ".join(f"[`{t['name']}`](#{t['name']})" for t in tools) + "\n\n"
    for tool in tools:
        out += _render_tool(tool)
    return out


def _render_resources_page(report: dict[str, Any]) -> str:
    """Render the resources + resource-templates page."""
    resources = report.get("resources") or []
    templates = report.get("templates") or []
    total = len(resources) + len(templates)
    out = _frontmatter(
        title="Resources",
        sidebar_label="Resources",
        description=f"All {total} MCP resource(s) and resource template(s).",
    )
    out += "# Resources\n\n"
    if not resources and not templates:
        out += "_No resources or resource templates are exposed by this server._\n"
        return out
    if resources:
        out += f"## Concrete resources ({len(resources)})\n\n"
        for resource in resources:
            out += _render_resource(resource)
    if templates:
        out += f"## Resource templates ({len(templates)})\n\n"
        for template in templates:
            out += _render_resource(template)
    return out


def _render_prompts_page(report: dict[str, Any]) -> str:
    """Render the prompts page."""
    prompts = report.get("prompts") or []
    out = _frontmatter(
        title="Prompts",
        sidebar_label="Prompts",
        description=f"All {len(prompts)} MCP prompt(s).",
    )
    out += "# Prompts\n\n"
    if not prompts:
        out += "_No prompts are exposed by this server._\n"
        return out
    out += f"This server exposes **{len(prompts)}** prompt(s).\n\n"
    for prompt in prompts:
        out += _render_prompt(prompt)
    return out


# Paths we refuse to `rmtree` even if the user passes them as --output, to
# avoid cases like `--output /` or `--output $HOME` accidentally nuking data.
_FORBIDDEN_OUTPUT_PATHS = frozenset(
    {
        Path("/"),
        Path.home(),
        Path.cwd(),
    }
)


def _prepare_output_dir(output: Path) -> None:
    """Reset (or create) an output directory, with a minimal safety guard."""
    resolved = output.resolve()
    if resolved in {p.resolve() for p in _FORBIDDEN_OUTPUT_PATHS}:
        raise RuntimeError(
            f"Refusing to rmtree suspicious output path {resolved}. "
            "Pass --output pointing at a dedicated subdirectory."
        )
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)


def generate(server_spec: str, output: Path) -> None:
    """Run `fastmcp inspect`, render Markdown, and write files to `output/`."""
    with tempfile.TemporaryDirectory() as tmp:
        report_path = Path(tmp) / "mcp-inspect.json"
        print(f"Running `fastmcp inspect {server_spec}`...")
        report = _run_fastmcp_inspect(server_spec, report_path)

    _prepare_output_dir(output)

    pages: dict[str, str] = {
        "index.md": _render_index(report),
        "tools.md": _render_tools_page(report),
        "resources.md": _render_resources_page(report),
        "prompts.md": _render_prompts_page(report),
    }
    for name, content in pages.items():
        (output / name).write_text(content, encoding="utf-8")
        print(f"  wrote {output / name}")

    print(
        f"Done. {len(report.get('tools') or [])} tool(s), "
        f"{len(report.get('resources') or []) + len(report.get('templates') or [])} "
        f"resource(s), {len(report.get('prompts') or [])} prompt(s) documented."
    )


def main() -> int:
    """CLI entrypoint for the Markdown MCP docs generator."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--server-spec",
        default=DEFAULT_SERVER_SPEC,
        help=f"FastMCP server spec to inspect, e.g. '{DEFAULT_SERVER_SPEC}' (default).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output directory for generated Markdown (default: {DEFAULT_OUTPUT}).",
    )
    args = parser.parse_args()
    try:
        generate(server_spec=args.server_spec, output=args.output)
    except (subprocess.CalledProcessError, RuntimeError) as ex:
        print(f"MCP docs generation failed: {ex}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
