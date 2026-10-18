#!/usr/bin/env python3

# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Generate docs for all public modules in PyAirbyte and save them to docs/generated.

Usage:
    poetry run python docs/generate.py

"""

from __future__ import annotations

import importlib.util
import pathlib
import pkgutil
import re
import shutil

import pdoc
import pdoc.render_helpers

import airbyte as ab


# Public (non-underscore) modules that are deliberately left out of the API docs.
DOCS_EXCLUDED_MODULES = frozenset(
    {
        # MCP server process entry points; connectivity is documented in `airbyte.mcp`.
        "airbyte.mcp.http_main",
        "airbyte.mcp.server",
        "airbyte.version",
    }
)
DOCS_EXCLUDED_PACKAGES = ("airbyte.cli.smoke_test_source",)


def _raise_import_error(module_name: str) -> None:
    raise ImportError(f"Failed to import `{module_name}` while discovering docs modules.")


def discover_public_modules() -> list[str]:
    """Return the names of all public `airbyte` modules to document.

    Discovery walks the package on disk, so it does not depend on which submodules a package
    imports or lists in `__all__`. Any module with an underscore-prefixed path segment is
    private and skipped, as are `DOCS_EXCLUDED_MODULES` and `DOCS_EXCLUDED_PACKAGES`.
    """
    module_names = [
        ab.__name__,
        *(
            module.name
            for module in pkgutil.walk_packages(
                ab.__path__,
                prefix=f"{ab.__name__}.",
                onerror=_raise_import_error,
            )
        ),
    ]
    return sorted(
        name
        for name in module_names
        if not any(part.startswith("_") for part in name.split("."))
        and name not in DOCS_EXCLUDED_MODULES
        and not any(
            name == package or name.startswith(f"{package}.") for package in DOCS_EXCLUDED_PACKAGES
        )
    )


def _regenerate_mcp_markdown() -> None:
    """Regenerate `docs/mcp-generated/` before pdoc runs.

    The `airbyte.mcp.{cloud,local,interactive,registry,guidance}` modules pull the
    per-module Markdown files from `docs/mcp-generated/` via pdoc's
    `.. include::` directive. That directory is git-ignored, so on a clean
    checkout pdoc would fail to resolve the include unless we regenerate it
    here. Running the generator from inside `docs-generate` makes the full
    docs build reproducible from a fresh clone (and matches the standalone
    `poe mcp-docs-md` task).

    We load the generator via `importlib.util` from its on-disk path rather
    than a plain `from generate_mcp_markdown import ...`: the generator
    lives under `scripts/` (not on `sys.path`), and a static import would
    also trip `deptry` into flagging `generate_mcp_markdown` as a missing
    external dependency.
    """
    script = pathlib.Path(__file__).parent.parent / "scripts" / "generate_mcp_markdown.py"
    if not script.exists():
        raise RuntimeError(f"MCP markdown generator not found at {script}")
    spec = importlib.util.spec_from_file_location("_mcp_markdown_gen", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load spec for {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    print("[docs-generate] Regenerating docs/mcp-generated/ ...")
    module.generate(
        server_spec=module.DEFAULT_SERVER_SPEC,
        output=module.DEFAULT_OUTPUT,
    )


_INCLUDE_DIRECTIVE = re.compile(r"^\s*\.\.\s+include::\s+(\S+)\s*$", re.MULTILINE)


def _display_path(path: pathlib.Path, root: pathlib.Path) -> str:
    """Return `path` relative to `root`, or absolute if it falls outside `root`."""
    if path.is_relative_to(root):
        return str(path.relative_to(root))
    return str(path)


def _validate_includes(root: pathlib.Path) -> None:
    """Raise if a reStructuredText include in an Airbyte source is missing."""
    resolved_root = root.resolve()
    missing: list[str] = []
    for source in sorted((resolved_root / "airbyte").rglob("*.py")):
        for match in _INCLUDE_DIRECTIVE.finditer(source.read_text(encoding="utf-8")):
            target = (source.parent / match.group(1)).resolve()
            if not target.exists():
                missing.append(
                    f"{_display_path(source, resolved_root)} includes missing "
                    f"{_display_path(target, resolved_root)}"
                )
    if missing:
        raise RuntimeError("Unresolved documentation includes:\n" + "\n".join(missing))


def run() -> None:
    """Generate docs for all public modules in PyAirbyte and save them to docs/generated."""
    # Regenerate MCP Markdown first so the `.. include::` directives in the
    # MCP module docstrings resolve on a clean checkout (docs/mcp-generated/
    # is git-ignored).
    _regenerate_mcp_markdown()
    _validate_includes(pathlib.Path(__file__).parent.parent)

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
        *discover_public_modules(),
        output_directory=pathlib.Path("docs/generated"),
    )


if __name__ == "__main__":
    run()
