# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Validate generated MCP documentation includes and public module exports."""

from __future__ import annotations

import pkgutil
from pathlib import Path

import pytest

import airbyte.mcp
from docs.generate import _validate_includes


# These process entry points have connectivity docs in `airbyte.mcp`'s docstring.
DOCS_EXCLUDED_MODULES = frozenset({"http_main", "server"})


def test_validate_includes_raises_for_missing_target(tmp_path: Path) -> None:
    source = tmp_path / "airbyte" / "mcp" / "module.py"
    source.parent.mkdir(parents=True)
    source.write_text(".. include:: ../../docs/mcp-generated/missing.md\n")

    with pytest.raises(RuntimeError, match="missing.md"):
        _validate_includes(tmp_path)


def test_validate_includes_handles_target_outside_root(tmp_path: Path) -> None:
    source = tmp_path / "airbyte" / "mcp" / "module.py"
    source.parent.mkdir(parents=True)
    source.write_text(".. include:: ../../../../../outside.md\n")

    with pytest.raises(RuntimeError, match="Unresolved documentation includes"):
        _validate_includes(tmp_path)


def test_validate_includes_passes_for_existing_target(tmp_path: Path) -> None:
    source = tmp_path / "airbyte" / "mcp" / "module.py"
    target = tmp_path / "docs" / "mcp-generated" / "module.md"
    source.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    source.write_text(".. include:: ../../docs/mcp-generated/module.md\n")
    target.write_text("# module\n")

    _validate_includes(tmp_path)


def test_existing_includes_name_generated_mcp_modules() -> None:
    repo_root = Path(__file__).parents[2]
    generated_modules = {
        "agents",
        "cloud",
        "local",
        "interactive",
        "registry",
        "prompts",
    }
    for source in (repo_root / "airbyte").rglob("*.py"):
        for line in source.read_text(encoding="utf-8").splitlines():
            if ".. include::" not in line:
                continue
            target = line.split(".. include::", 1)[1].strip()
            target_path = Path(target)
            assert target_path.parts[-2] == "mcp-generated"
            assert target_path.stem in generated_modules


def test_mcp_all_covers_public_submodules() -> None:
    public_modules = {
        module.name
        for module in pkgutil.iter_modules(airbyte.mcp.__path__)
        if not module.name.startswith("_")
    }
    exported_modules = set(airbyte.mcp.__all__)
    assert public_modules - DOCS_EXCLUDED_MODULES <= exported_modules
    assert DOCS_EXCLUDED_MODULES.isdisjoint(exported_modules)
