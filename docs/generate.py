# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Generate docs for all public modules in AirbyteLib and save them to docs/generated.

Usage:
    poetry run python docs/generate.py

"""


from __future__ import annotations

import os
import pathlib
import shutil

import pdoc

import airbyte as ab


def run() -> None:
    """Generate docs for all public modules in AirbyteLib and save them to docs/generated."""
    public_modules = ["airbyte"]

    # recursively delete the docs/generated folder if it exists
    if pathlib.Path("docs/generated").exists():
        shutil.rmtree("docs/generated")

    # All files and folders that don't start with "_" are treated as public.
    for submodule in os.listdir("airbyte"):
        submodule_path = pathlib.Path(f"airbyte/{submodule}")
        if not submodule.startswith("_"):
            public_modules.append(submodule_path)
            if submodule_path.is_file():
                continue

            for subsubmodule in os.listdir(submodule_path):
                subsubmodule_path = submodule_path / subsubmodule
                if not subsubmodule.startswith("_"):
                    public_modules.append(subsubmodule_path)

    pdoc.render.configure(
        template_directory="docs",
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
