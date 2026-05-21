#!/usr/bin/env python3
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Generate Markdown documentation for the Airbyte Replication MCP server.

Runs `fastmcp inspect` against the default `airbyte/mcp/server.py:app` spec
(override with `--server-spec`) to obtain the full FastMCP protocol surface
(tools, resources, resource templates, prompts) as a JSON report, then
renders it into one Markdown file **per MCP module** under
`docs/mcp-generated/`, plus an `index.md` overview.

The per-module grouping uses the `mcp_module` annotation that
`fastmcp_extensions.mcp_tool` attaches to every registered tool (derived from
the Python file the tool is defined in — e.g. tools in `airbyte/mcp/cloud.py`
get `mcp_module="cloud"`). Prompts and resources fall back to `meta.mcp_module`
when present, and otherwise to an import-based lookup against
`fastmcp_extensions.decorators._REGISTERED_*`; anything still unresolved lands
in `misc.md`.

Inside each module file, content is grouped by primitive with L2 headings:

```
# cloud module

## Tools (35)
### deploy_source_to_cloud
...
## Prompts (N)
### some_prompt
...
## Resources (N)
### some_resource
...
```

The output is designed to be:

- **`pdoc`/`pdoc3`-includable**: each `<module>.md` is a self-contained body
  intended to be spliced into the corresponding Python module's docstring via
  pdoc's `.. include::` directive, so the generated tool docs render inline on
  the module's pdoc page. Per-module pages intentionally emit **no** YAML
  front-matter (pdoc's Markdown renderer would surface it as body text);
  only `index.md` carries front-matter.
- **Docusaurus-hostable**: `index.md` starts with YAML front-matter (`title`,
  `sidebar_label`, `description`); module pages rely on Docusaurus'
  first-H1-as-title inference. The body is plain CommonMark + GFM tables +
  `<details><summary>` blocks for collapsible JSON schemas. No MDX-only
  components are used.
- **Deep-linkable**: every tool/resource/prompt name gets an HTML anchor
  (`<a id="name"></a>`) above its H3, so links like
  `cloud.md#deploy_source_to_cloud` resolve in both pdoc and Docusaurus.

Formatting is modeled on the
[`mcpdocs-gen`](https://github.com/smytsyk/mcpdocs) static HTML output — same
per-tool shape (description → parameters table → JSON schema) — but emitted as
Markdown, one file per MCP module, so the output can slot into both
`pdoc`-rendered per-module pages and an external Docusaurus site.

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
import importlib
import json
import shutil
import subprocess
import sys
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT = Path("docs/mcp-generated")
DEFAULT_SERVER_SPEC = "airbyte/mcp/server.py:app"
MISC_MODULE = "misc"
# Upper bound on how long `fastmcp inspect` may take before we fail the build.
# 120s is generous: local runs finish in ~10s, but CI / cold caches occasionally
# spend longer on the server-module import (e.g. re-resolving wheels). Anything
# beyond this almost certainly indicates a hang (blocking import, stalled
# network I/O during registration) rather than real work, so failing fast is
# preferable to an indefinitely stuck `poe docs-generate` / `poe mcp-docs-md`.
_FASTMCP_INSPECT_TIMEOUT_SEC = 120
# Repo root anchor for path-safety checks. `__file__` is always the on-disk
# location of this script, so `parent.parent` reliably points at the repo root
# regardless of the caller's `cwd`. We use this instead of `Path.cwd()` when
# resolving repo-relative defaults like `DEFAULT_OUTPUT` so that
# `poe mcp-docs-md` works even when invoked from a subdirectory.
_REPO_ROOT = Path(__file__).resolve().parent.parent


def _run_fastmcp_inspect(server_spec: str, report_path: Path) -> dict[str, Any]:
    """Invoke `fastmcp inspect` and return the parsed JSON report."""
    fastmcp_bin = shutil.which("fastmcp")
    if fastmcp_bin is None:
        raise RuntimeError(
            "`fastmcp` CLI not found on PATH. Install project dev deps first "
            "(e.g. `uv sync --group dev`) and re-run from the repo root."
        )
    try:
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
            timeout=_FASTMCP_INSPECT_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as ex:
        msg = (
            f"`fastmcp inspect {server_spec}` timed out after "
            f"{_FASTMCP_INSPECT_TIMEOUT_SEC}s. The server module likely hangs "
            "on import (blocking network I/O during tool registration?). "
            "Re-run with the server imported manually to investigate."
        )
        raise RuntimeError(msg) from ex
    return json.loads(report_path.read_text(encoding="utf-8"))


def _resolve_extra_module_map(server_spec: str) -> dict[str, str]:
    """Best-effort import-based lookup of `mcp_module` for prompts/resources.

    `fastmcp_extensions`'s `mcp_tool` decorator embeds `mcp_module` in the MCP
    tool `annotations` dict, which the inspect JSON surfaces directly. But
    `mcp_prompt` and `mcp_resource` store `mcp_module` on the library's
    internal `_REGISTERED_*` lists only — it is not re-emitted as an MCP
    annotation, so it doesn't appear in the inspect JSON.

    To still recover that information, we import the server module and read
    those internal lists. If that fails (not a `fastmcp_extensions`-based
    server, import errors, etc.), we silently return an empty map and the
    caller falls back to `MISC_MODULE`.

    Returns a map of `name/uri -> mcp_module` covering both prompts and
    resources.
    """
    file_part = server_spec.split(":", 1)[0]
    module_name = file_part.removesuffix(".py").replace("/", ".")
    mapping: dict[str, str] = {}
    # The iteration sits inside the same `try` as the import so any shape
    # drift in the private `_REGISTERED_*` tuples (e.g. an added third element,
    # or `ann` becoming a dataclass instead of a dict) falls back to an empty
    # mapping — preserving this helper's documented best-effort semantics —
    # rather than aborting doc generation.
    try:
        importlib.import_module(module_name)
        # Import private lists from fastmcp_extensions: these are the only
        # place `mcp_module` is recorded for prompts/resources, so we accept
        # the private-name coupling.
        from fastmcp_extensions.decorators import (  # noqa: PLC0415
            _REGISTERED_PROMPTS,  # noqa: PLC2701
            _REGISTERED_RESOURCES,  # noqa: PLC2701
        )

        for _fn, ann in _REGISTERED_PROMPTS:
            if name := ann.get("name"):
                mapping[name] = ann.get("mcp_module") or MISC_MODULE
        for _fn, ann in _REGISTERED_RESOURCES:
            mcp_module = ann.get("mcp_module") or MISC_MODULE
            if uri := ann.get("uri"):
                mapping[uri] = mcp_module
                # FastMCP exposes the URI stem as the resource `name` in
                # inspect output; index by that too so lookup by either key
                # works.
                mapping[uri.rsplit("/", 1)[-1]] = mcp_module
    except Exception:
        return {}
    return mapping


def _get_module(item: dict[str, Any], fallback_map: dict[str, str]) -> str:
    """Extract the `mcp_module` for a tool / resource / prompt."""
    annotations = item.get("annotations") or {}
    if mcp_module := annotations.get("mcp_module"):
        return str(mcp_module)
    meta = item.get("meta") or {}
    if mcp_module := meta.get("mcp_module"):
        return str(mcp_module)
    name = item.get("name")
    uri = item.get("uri") or item.get("uri_template")
    for key in (name, uri):
        if key and key in fallback_map:
            return fallback_map[key]
    return MISC_MODULE


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


# MCP tool annotation hints (per the MCP spec): we render a badge for every
# hint whose value is `True`, using a stable, human-readable label. The four
# standardised hints come from
# https://modelcontextprotocol.io/specification/server/tools#tool-annotations.
_HINT_LABELS: dict[str, str] = {
    "readOnlyHint": "read-only",
    "destructiveHint": "destructive",
    "idempotentHint": "idempotent",
    "openWorldHint": "open-world",
}


def _render_hint_badges(annotations: dict[str, Any] | None) -> str:
    """Render MCP tool-annotation hints as inline `code` badges.

    Only hints whose value is explicitly `True` are rendered — an unset or
    `False` hint is omitted. The MCP spec treats hints as advisory, so
    "absence" and "false" are equivalent for documentation purposes.

    Also surfaces the optional human-readable `annotations.title` (distinct
    from the top-level `title` field) when present, so e.g.
    `annotations.title == "Deploy a source to Airbyte Cloud"` shows up in
    the rendered doc.
    """
    if not annotations:
        return ""
    lines: list[str] = []
    badges = [f"`{label}`" for key, label in _HINT_LABELS.items() if annotations.get(key) is True]
    if badges:
        lines.append("**Hints:** " + " · ".join(badges))
    if title := annotations.get("title"):
        lines.append(f"**Title:** {title}")
    return ("\n\n".join(lines) + "\n\n") if lines else ""


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
    """Render a single tool as L3 under its module's `## Tools` section."""
    name = tool["name"]
    # Plain text in the heading (no backticks) so pdoc's TOC extractor
    # produces a clean sidebar nav entry. The HTML anchor above the heading
    # is what we deep-link to.
    parts: list[str] = [f'<a id="{name}"></a>\n\n### {name}\n\n']
    parts.append(_render_hint_badges(tool.get("annotations")))
    if description := tool.get("description"):
        parts.append(description.strip() + "\n\n")
    if tags := tool.get("tags"):
        parts.append("**Tags:** " + ", ".join(f"`{t}`" for t in tags) + "\n\n")
    parts.extend(
        [
            "#### Parameters\n\n",
            _render_parameters_table(tool.get("input_schema") or {}),
        ]
    )
    if input_schema := tool.get("input_schema"):
        parts.append(_json_block("Show input JSON schema", input_schema))
    if output_schema := tool.get("output_schema"):
        parts.append(_json_block("Show output JSON schema", output_schema))
    return "".join(parts)


def _render_resource(resource: dict[str, Any]) -> str:
    """Render a single resource as L3 under its module's `## Resources` section."""
    name = resource["name"]
    parts: list[str] = [f'<a id="{name}"></a>\n\n### {name}\n\n']
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
    """Render a single prompt as L3 under its module's `## Prompts` section."""
    name = prompt["name"]
    parts: list[str] = [f'<a id="{name}"></a>\n\n### {name}\n\n']
    if description := prompt.get("description"):
        parts.append(description.strip() + "\n\n")
    args = prompt.get("arguments") or []
    if args:
        parts.extend(
            [
                "#### Arguments\n\n",
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


# -----------------------------------------------------------------------------
# Bucketing + per-module pages
# -----------------------------------------------------------------------------


class _ModuleBucket:
    """Accumulator for a single mcp_module's tools / prompts / resources."""

    def __init__(self, name: str) -> None:
        """Create an empty bucket for the given mcp_module name."""
        self.name = name
        self.tools: list[dict[str, Any]] = []
        self.prompts: list[dict[str, Any]] = []
        self.resources: list[dict[str, Any]] = []  # concrete + templates

    @property
    def total(self) -> int:
        """Total count of MCP primitives in this bucket."""
        return len(self.tools) + len(self.prompts) + len(self.resources)


def _bucket_by_module(
    report: dict[str, Any],
    fallback_map: dict[str, str],
) -> OrderedDict[str, _ModuleBucket]:
    """Group report items by mcp_module, preserving first-seen order."""
    buckets: OrderedDict[str, _ModuleBucket] = OrderedDict()

    def get(mcp_module: str) -> _ModuleBucket:
        if mcp_module not in buckets:
            buckets[mcp_module] = _ModuleBucket(mcp_module)
        return buckets[mcp_module]

    for tool in report.get("tools") or []:
        get(_get_module(tool, fallback_map)).tools.append(tool)
    for prompt in report.get("prompts") or []:
        get(_get_module(prompt, fallback_map)).prompts.append(prompt)
    for resource in report.get("resources") or []:
        get(_get_module(resource, fallback_map)).resources.append(resource)
    for template in report.get("templates") or []:
        get(_get_module(template, fallback_map)).resources.append(template)

    # Alpha-sort each bucket's primitives (case-insensitive) so the rendered
    # pages, left-nav entries, and deep-link IDs are in a stable, predictable
    # order across regenerations instead of reflecting server registration
    # order (which is effectively arbitrary).
    def _sort_key(item: dict[str, Any]) -> str:
        return str(item.get("name") or item.get("uri") or "").lower()

    for bucket in buckets.values():
        bucket.tools.sort(key=_sort_key)
        bucket.prompts.sort(key=_sort_key)
        bucket.resources.sort(key=_sort_key)

    # Also sort the module-level ordering so `index.md`'s module table and the
    # order of files on disk are alphabetical (the `misc` bucket, which is a
    # catch-all, is always pinned last).
    sorted_buckets: OrderedDict[str, _ModuleBucket] = OrderedDict()
    for name in sorted(buckets, key=lambda n: (n == MISC_MODULE, n.lower())):
        sorted_buckets[name] = buckets[name]
    return sorted_buckets


def _render_module_page(bucket: _ModuleBucket, server_name: str) -> str:
    """Render a single `<module>.md` page with L2 Tools/Prompts/Resources sections.

    No YAML front-matter is emitted on module pages: these files are consumed
    by pdoc3 via the `.. include::` directive (pdoc's Markdown renderer does
    not strip front-matter and would emit it as body text). Docusaurus infers
    the page title from the first H1, which we always emit here.
    """
    # Headings are plain text (no backticks) so pdoc's TOC extractor yields
    # clean nav entries; cosmetic backticks inside headings produced
    # unbalanced `<code>` tags in the generated TOC HTML.
    parts: list[str] = [
        f"# {bucket.name} module\n\n",
        (
            f"MCP primitives registered by the `{bucket.name}` module "
            f"of the `{server_name}` server: "
            f"**{len(bucket.tools)}** tool(s), "
            f"**{len(bucket.prompts)}** prompt(s), "
            f"**{len(bucket.resources)}** resource(s).\n\n"
        ),
    ]
    if bucket.tools:
        # The left-nav sidebar already lists every tool under this H2 via the
        # TOC, so we intentionally omit the inline "Index: …" row that we used
        # to emit here.
        parts.append(f"## Tools ({len(bucket.tools)})\n\n")
        parts.extend(_render_tool(tool) for tool in bucket.tools)
    if bucket.prompts:
        parts.append(f"## Prompts ({len(bucket.prompts)})\n\n")
        parts.extend(_render_prompt(prompt) for prompt in bucket.prompts)
    if bucket.resources:
        parts.append(f"## Resources ({len(bucket.resources)})\n\n")
        parts.extend(_render_resource(resource) for resource in bucket.resources)
    return "".join(parts)


def _render_index(
    report: dict[str, Any],
    buckets: OrderedDict[str, _ModuleBucket],
) -> str:
    """Render the top-level overview page."""
    server = report.get("server") or {}
    server_name = server.get("name", "mcp-server")
    # `splitlines()` on an empty string returns `[]`, so we can't index [0].
    first_instruction_line = next(iter((server.get("instructions") or "").splitlines()), "")
    out = _frontmatter(
        title=f"{server_name} — MCP server",
        sidebar_label="Overview",
        description=first_instruction_line
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
    total_tools = sum(len(b.tools) for b in buckets.values())
    total_prompts = sum(len(b.prompts) for b in buckets.values())
    total_resources = sum(len(b.resources) for b in buckets.values())
    out += (
        "## Totals\n\n"
        f"- **Tools:** {total_tools}\n"
        f"- **Prompts:** {total_prompts}\n"
        f"- **Resources:** {total_resources}\n\n"
    )
    out += "## Modules\n\n"
    out += "| Module | Tools | Prompts | Resources |\n"
    out += "| --- | ---: | ---: | ---: |\n"
    for name, bucket in buckets.items():
        out += (
            f"| [`{name}`](./{name}.md) | {len(bucket.tools)} | "
            f"{len(bucket.prompts)} | {len(bucket.resources)} |\n"
        )
    out += "\n"
    out += (
        "> These pages are generated from the live `fastmcp inspect` report. "
        "Regenerate with `poe mcp-docs-md`.\n"
    )
    return out


def _resolve_output_dir(output: Path) -> Path:
    """Resolve an `--output` path against the repo root when it's relative.

    `DEFAULT_OUTPUT` is a repo-relative path, so anchoring relative inputs to
    `_REPO_ROOT` (rather than `Path.cwd()`) means `poe mcp-docs-md` works
    regardless of where the task is invoked from — a contributor running the
    task from inside `docs/` still writes to `<repo>/docs/mcp-generated/`.
    Absolute paths are honoured as-given (the safety guard below still
    rejects any absolute path that escapes the repo root).
    """
    return (output if output.is_absolute() else _REPO_ROOT / output).resolve()


def _prepare_output_dir(output: Path) -> Path:
    """Reset (or create) an output directory, with a strict safety guard.

    The script unconditionally `rmtree`s `output` before regenerating, so we
    need to be careful about what callers can point `--output` at. We require
    the resolved output path to live **strictly inside** the repo root — this
    rules out `/`, `$HOME`, `--output ..`, and any absolute path outside the
    repo, while still letting the default `docs/mcp-generated/` work. The
    repo root itself is also rejected so we never nuke the whole repo.

    Returns the *resolved* (absolute, repo-root-anchored) path so callers can
    use a single canonical location for subsequent file writes — avoids a
    footgun where preparing a resolved dir but writing via the raw `output`
    would silently target a different, non-existent path when cwd differs
    from the repo root.
    """
    resolved = _resolve_output_dir(output)
    if resolved == _REPO_ROOT or not resolved.is_relative_to(_REPO_ROOT):
        raise RuntimeError(
            f"Refusing to rmtree output path {resolved}: must be a dedicated "
            f"subdirectory strictly inside the repo root ({_REPO_ROOT}). "
            f"Pass --output pointing at e.g. `docs/mcp-generated`."
        )
    if resolved.exists():
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def generate(server_spec: str, output: Path) -> None:
    """Run `fastmcp inspect`, render Markdown, and write files to `output/`."""
    with tempfile.TemporaryDirectory() as tmp:
        report_path = Path(tmp) / "mcp-inspect.json"
        print(f"Running `fastmcp inspect {server_spec}`...")
        report = _run_fastmcp_inspect(server_spec, report_path)

    fallback_map = _resolve_extra_module_map(server_spec)
    buckets = _bucket_by_module(report, fallback_map)

    # Use the resolved path returned by `_prepare_output_dir` for subsequent
    # writes: when called from a subdirectory, the raw `output` is
    # cwd-relative and would target a non-existent directory.
    resolved_output = _prepare_output_dir(output)

    server_name = (report.get("server") or {}).get("name", "mcp-server")
    pages: dict[str, str] = {"index.md": _render_index(report, buckets)}
    for name, bucket in buckets.items():
        pages[f"{name}.md"] = _render_module_page(bucket, server_name)

    for name, content in pages.items():
        (resolved_output / name).write_text(content, encoding="utf-8")
        print(f"  wrote {resolved_output / name}")

    print(
        f"Done. {len(buckets)} module(s) documented — "
        f"{sum(len(b.tools) for b in buckets.values())} tool(s), "
        f"{sum(len(b.resources) for b in buckets.values())} resource(s), "
        f"{sum(len(b.prompts) for b in buckets.values())} prompt(s)."
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
