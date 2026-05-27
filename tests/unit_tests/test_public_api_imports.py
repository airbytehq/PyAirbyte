# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import ast
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).parents[2]
RESTRICTED_IMPORTS_BY_PATH = {
    Path("airbyte/cli"): ("airbyte_api", "airbyte._util.api_imports"),
    Path("airbyte/mcp"): ("airbyte_api", "airbyte._util.api_imports"),
    Path("airbyte/cloud/client.py"): ("airbyte_api", "airbyte._util.api_imports"),
    Path("airbyte/cloud/workspaces.py"): ("airbyte_api", "airbyte._util.api_imports"),
}


def _python_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(path.rglob("*.py"))


def _imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
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


def _is_restricted_import(imported_module: str, restricted_import: str) -> bool:
    return imported_module == restricted_import or imported_module.startswith(
        f"{restricted_import}."
    )


@pytest.mark.parametrize(
    ("relative_path", "restricted_imports"),
    [
        pytest.param(relative_path, restricted_imports, id=relative_path.as_posix())
        for relative_path, restricted_imports in RESTRICTED_IMPORTS_BY_PATH.items()
    ],
)
def test_public_modules_do_not_import_generated_api_models(
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
        "Public CLI, MCP, and workspace modules must not import generated Airbyte API "
        "models directly. Wrap generated API models in PyAirbyte-owned response models "
        "before exposing them through public or presentation-layer modules.\n"
        + "\n".join(violations)
    )
