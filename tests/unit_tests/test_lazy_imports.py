# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Guard the lazy-loaded `airbyte` namespace and the import cost of common entry points."""

from __future__ import annotations

import json
import subprocess
import sys
from types import ModuleType

import pytest

import airbyte
import airbyte.caches


HEAVY_MODULES = (
    "airbyte_api",
    "airbyte_cdk",
    "duckdb",
    "fastmcp",
    "google.cloud.bigquery",
    "google.cloud.secretmanager_v1",
    "pandas",
    "pyarrow",
    "snowflake",
    "sqlalchemy",
)

# Lazy `lazy_loader.load()` proxies sit in `sys.modules` before first use, so only modules
# whose code has actually executed (plain `ModuleType`) count as loaded.
_LOADED_MODULES_SCRIPT = """
import json, sys, types
{statement}
loaded = [
    name for name in {heavy_modules!r}
    if type(sys.modules.get(name)) is types.ModuleType
]
print(json.dumps(loaded))
"""


def _loaded_heavy_modules(statement: str) -> set[str]:
    script = _LOADED_MODULES_SCRIPT.format(
        statement=statement, heavy_modules=HEAVY_MODULES
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=True,
        text=True,
    )
    return set(json.loads(result.stdout.splitlines()[-1]))


@pytest.mark.parametrize(
    ("statement", "allowed"),
    [
        pytest.param("import airbyte", set(), id="import-airbyte"),
        pytest.param("import airbyte.caches", set(), id="import-caches"),
        pytest.param(
            "import airbyte as ab; ab.get_source",
            {"pandas", "pyarrow", "sqlalchemy"},
            id="get-source",
        ),
    ],
)
def test_heavy_modules_not_loaded(statement: str, allowed: set[str]) -> None:
    assert _loaded_heavy_modules(statement) <= allowed


def test_eager_import_resolves_all_stub_names(monkeypatch: pytest.MonkeyPatch) -> None:
    """`EAGER_IMPORT=1` makes `lazy_loader` import every stub entry, so a broken one fails."""
    monkeypatch.setenv("EAGER_IMPORT", "1")
    _loaded_heavy_modules("import airbyte, airbyte.caches")


@pytest.mark.parametrize("module", [airbyte, airbyte.caches], ids=lambda m: m.__name__)
def test_all_names_resolve_and_are_listed_by_dir(module: ModuleType) -> None:
    exported = module.__all__
    assert exported
    for name in exported:
        assert getattr(module, name) is not None
    assert set(exported) <= set(dir(module))


@pytest.mark.parametrize(
    ("package", "name"),
    [
        (airbyte, "caches"),
        (airbyte, "cloud"),
        (airbyte, "mcp"),
        (airbyte, "secrets"),
        (airbyte.caches, "base"),
        (airbyte.caches, "duckdb"),
        (airbyte.caches, "util"),
    ],
    ids=lambda value: value if isinstance(value, str) else value.__name__,
)
def test_submodules_resolve_lazily(package: ModuleType, name: str) -> None:
    assert name in dir(package)
    assert name not in package.__all__
    assert getattr(package, name).__name__ == f"{package.__name__}.{name}"
