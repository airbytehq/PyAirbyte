#!/usr/bin/env python3
"""Reproduce PyAirbyte issue #290 before/after the executable discovery fix."""
from __future__ import annotations

import os

os.environ["AIRBYTE_NO_UV"] = "true"

import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "tests/integration_tests/fixtures/source-wrong-exe"
CONNECTOR_NAME = "source-wrong-exe"


def main() -> int:
    os.chdir(REPO_ROOT)
    sys.path.insert(0, str(REPO_ROOT))

    from airbyte._executors.python import VenvExecutor  # noqa: PLC0415

    install_root = Path(tempfile.mkdtemp(prefix="pyairbyte-issue-290-"))
    executor = VenvExecutor(
        name=CONNECTOR_NAME,
        pip_url=str(FIXTURE_DIR),
        install_root=install_root,
    )

    print("Installing connector with mismatched console script name...")
    executor.install()
    executor.ensure_installation()

    script_name = executor._resolve_console_script_name()  # noqa: SLF001
    connector_path = executor._get_connector_path()  # noqa: SLF001

    print(f"Discovered console script: {script_name}")
    print(f"Connector executable path:   {connector_path}")
    print(f"Executable exists:           {connector_path.exists()}")

    if script_name == "wrong-script-name" and connector_path.exists():
        print("\nIssue #290 fix verified.")
        return 0

    print("\nUnexpected state — executable discovery may still be broken.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
