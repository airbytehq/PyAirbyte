#!/usr/bin/env python3

# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Generate docs for all public modules in PyAirbyte and save them to docs/generated.

Usage:
    poetry run python docs/generate.py

"""

from __future__ import annotations

import importlib.util
import pathlib
import shutil
import sys

import pdoc
import pdoc.render_helpers


def _regenerate_mcp_markdown() -> None:
    """Regenerate `docs/mcp-generated/` before pdoc runs.

    The `airbyte.mcp.{cloud,local,registry,prompts}` modules pull the
    per-module Markdown files from `docs/mcp-generated/` via pdoc's
    `.. include::` directive. That directory is git-ignored, so on a clean
    checkout pdoc would fail to resolve the include unless we regenerate it
    here. Running the generator from inside `docs-generate` makes the full
    docs build reproducible from a fresh clone (and matches the standalone
    `poe mcp-docs-md` task).

    If generation fails (e.g. `fastmcp` is not installed, or the MCP server
    import fails), we print a warning and continue: pdoc will still build,
    and the include directive will just surface the missing file.

    We load the generator via `importlib.util` from its on-disk path rather
    than a plain `from generate_mcp_markdown import ...`: the generator
    lives under `scripts/` (not on `sys.path`), and a static import would
    also trip `deptry` into flagging `generate_mcp_markdown` as a missing
    external dependency.
    """
    script = pathlib.Path(__file__).parent.parent / "scripts" / "generate_mcp_markdown.py"
    if not script.exists():
        print(f"[docs-generate] MCP markdown generator not found at {script}; skipping.")
        return
    try:
        spec = importlib.util.spec_from_file_location("_mcp_markdown_gen", script)
        if spec is None or spec.loader is None:
            msg = f"Could not load spec for {script}"
            raise RuntimeError(msg)  # noqa: TRY301
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        print("[docs-generate] Regenerating docs/mcp-generated/ ...")
        module.generate(
            server_spec=module.DEFAULT_SERVER_SPEC,
            output=module.DEFAULT_OUTPUT,
        )
    except Exception as ex:
        print(
            f"[docs-generate] WARNING: failed to regenerate MCP Markdown docs: {ex}. "
            "pdoc will continue, but module pages may show missing include warnings.",
            file=sys.stderr,
        )


def run() -> None:
    """Generate docs for all public modules in PyAirbyte and save them to docs/generated."""
    public_modules = ["airbyte", "airbyte/cli/pyab.py"]

    # Regenerate MCP Markdown first so the `.. include::` directives in the
    # MCP module docstrings resolve on a clean checkout (docs/mcp-generated/
    # is git-ignored).
    _regenerate_mcp_markdown()

    # recursively delete the docs/generated folder if it exists
    if pathlib.Path("docs/generated").exists():
        shutil.rmtree("docs/generated")

    # pdoc's default sidebar TOC depth is 2 (H1 + H2 only), which hides the
    # per-tool H3 anchors produced by our MCP Markdown generator. Bump to 3 so
    # individual tools / prompts / resources show up in the left nav. This
    # monkey-patches the module-level `markdown_extensions` dict because pdoc
    # 16's `configure()` does not expose markdown extension options.
    # pyrefly: ignore[unsupported-operation]
    pdoc.render_helpers.markdown_extensions["toc"] = {"depth": 3}

    pdoc.render.configure(
        template_directory=pathlib.Path("docs/templates"),
        show_source=True,
        search=True,
        logo="https://docs.airbyte.com/img/pyairbyte-logo-dark.png",
        favicon="https://docs.airbyte.com/img/favicon.png",
        mermaid=True,
        docformat="google",
    )
    pdoc.pdoc(
        *public_modules,
        output_directory=pathlib.Path("docs/generated"),
    )


if __name__ == "__main__":
    run()
