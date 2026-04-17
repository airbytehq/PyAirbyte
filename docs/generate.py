#!/usr/bin/env python3

# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Generate docs for all public modules in PyAirbyte and save them to docs/generated.

Usage:
    poetry run python docs/generate.py

"""

from __future__ import annotations

import pathlib
import shutil

import pdoc
import pdoc.render_helpers


def run() -> None:
    """Generate docs for all public modules in PyAirbyte and save them to docs/generated."""
    public_modules = ["airbyte", "airbyte/cli/pyab.py"]

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
