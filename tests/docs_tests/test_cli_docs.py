# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Smoke tests for the `airbyte` CLI reference generator."""

from __future__ import annotations

from pathlib import Path

import pytest

from docs.generate_cli import generate_cli_reference, generate_cli_submodule_references


@pytest.mark.filterwarnings("ignore")
def test_generate_cli_reference_writes_markdown(tmp_path: Path) -> None:
    """`generate_cli_reference` writes a non-empty Markdown file mentioning `airbyte`."""
    output_path = tmp_path / "cli-reference.md"

    returned_path = generate_cli_reference(output_path)

    assert returned_path == output_path
    assert output_path.exists()

    content = output_path.read_text()
    assert content.strip(), "Generated CLI reference is empty"
    assert "airbyte" in content, "Generated CLI reference is missing the command name"
    assert "workspaces" in content, "Generated CLI reference is missing command groups"


@pytest.mark.filterwarnings("ignore")
def test_generate_cli_submodule_references_writes_group_markdown(
    tmp_path: Path,
) -> None:
    """`generate_cli_submodule_references` writes Markdown for each command group."""
    written = generate_cli_submodule_references(tmp_path)

    assert {path.name for path in written} == {
        "connections.md",
        "destinations.md",
        "jobs.md",
        "sources.md",
        "workspaces.md",
    }
    assert all(path.read_text().strip() for path in written)
