# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Validate public modules do not import generated Airbyte API models directly."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).parents[2]
PUBLIC_MODULE_RESTRICTED_IMPORTS = {
    Path("airbyte/cli"): ("airbyte_api", "airbyte._util.api_imports"),
    Path("airbyte/mcp"): ("airbyte_api", "airbyte._util.api_imports"),
    Path("airbyte/cloud"): ("airbyte_api", "airbyte._util.api_imports"),
}
PUBLIC_MODULE_RESTRICTED_REFERENCES = {
    Path("airbyte/cli"): ("api_util.models",),
    Path("airbyte/mcp"): ("api_util.models",),
    Path("airbyte/cloud"): ("api_util.models",),
}


def _python_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(path.rglob("*.py"))


def _imported_modules(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        pytest.fail(f"Syntax error parsing {path}: {exc}")
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)
            imported_modules.extend(
                f"{node.module}.{alias.name}" for alias in node.names
            )
    return imported_modules


def _attribute_references(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        pytest.fail(f"Syntax error parsing {path}: {exc}")
    references: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            references.append(_attribute_reference(node))
    return references


def _api_util_model_reference_aliases(path: Path) -> tuple[str, ...]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        pytest.fail(f"Syntax error parsing {path}: {exc}")
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "airbyte._util.api_util":
                    aliases.add(f"{alias.asname or alias.name}.models")
        elif isinstance(node, ast.ImportFrom) and node.module == "airbyte._util":
            for alias in node.names:
                if alias.name == "api_util":
                    aliases.add(f"{alias.asname or alias.name}.models")
    return tuple(sorted(aliases))


def _attribute_reference(node: ast.Attribute) -> str:
    parts: list[str] = []
    current: ast.AST = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


def _is_restricted_import(imported_module: str, restricted_import: str) -> bool:
    return imported_module == restricted_import or imported_module.startswith(
        f"{restricted_import}."
    )


@pytest.mark.linting
@pytest.mark.parametrize(
    ("source", "expected_reference"),
    [
        pytest.param(
            "from airbyte._util import api_util as au\nvalue = au.models.JobTypeEnum.SYNC\n",
            "au.models",
            id="from_import_alias",
        ),
        pytest.param(
            "import airbyte._util.api_util as au\nvalue = au.models.JobTypeEnum.SYNC\n",
            "au.models",
            id="import_alias",
        ),
    ],
)
def test_api_util_model_reference_aliases(
    tmp_path: Path,
    source: str,
    expected_reference: str,
) -> None:
    """Verify aliases for `api_util.models` references are restricted."""
    file_path = tmp_path / "module.py"
    file_path.write_text(source, encoding="utf-8")

    assert expected_reference in _api_util_model_reference_aliases(file_path)


@pytest.mark.linting
@pytest.mark.parametrize(
    ("relative_path", "restricted_imports"),
    [
        pytest.param(relative_path, restricted_imports, id=relative_path.as_posix())
        for relative_path, restricted_imports in PUBLIC_MODULE_RESTRICTED_IMPORTS.items()
    ],
)
def test_public_modules_do_not_import_generated_api_model_modules(
    relative_path: Path,
    restricted_imports: tuple[str, ...],
) -> None:
    violations: list[str] = []
    for file_path in _python_files(REPO_ROOT / relative_path):
        for imported_module in _imported_modules(file_path):
            for restricted_import in restricted_imports:
                if _is_restricted_import(imported_module, restricted_import):
                    violations.append(
                        f"{file_path.relative_to(REPO_ROOT)} imports {imported_module}"
                    )

    assert not violations, (
        "Public CLI, MCP, and cloud modules must not import generated Airbyte API "
        "models directly. Wrap generated API models in PyAirbyte-owned response models "
        "before exposing them through public or presentation-layer modules.\n"
        + "\n".join(violations)
    )


@pytest.mark.linting
@pytest.mark.parametrize(
    ("relative_path", "restricted_references"),
    [
        pytest.param(relative_path, restricted_references, id=relative_path.as_posix())
        for relative_path, restricted_references in PUBLIC_MODULE_RESTRICTED_REFERENCES.items()
    ],
)
def test_public_modules_do_not_reference_generated_api_model_namespaces(
    relative_path: Path,
    restricted_references: tuple[str, ...],
) -> None:
    """Check that public modules don't reach through internal model namespaces."""
    violations: list[str] = []
    for file_path in _python_files(REPO_ROOT / relative_path):
        file_restricted_references = (
            *restricted_references,
            *_api_util_model_reference_aliases(file_path),
        )
        for reference in _attribute_references(file_path):
            for restricted_reference in file_restricted_references:
                if _is_restricted_import(reference, restricted_reference):
                    violations.append(
                        f"{file_path.relative_to(REPO_ROOT)} references {reference}"
                    )
                    break
    assert not violations, (
        "Public CLI, MCP, and cloud modules must not reference generated Airbyte API "
        "model namespaces through internal utilities. Keep generated API models behind "
        "internal helpers or PyAirbyte-owned response models.\n" + "\n".join(violations)
    )
