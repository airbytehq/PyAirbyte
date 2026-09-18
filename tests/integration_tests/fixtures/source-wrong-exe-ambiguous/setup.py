#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from setuptools import setup

setup(
    name="airbyte-source-wrong-exe",
    version="0.0.1",
    description="Test Source with ambiguous executable names",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=["source_wrong_exe_ambiguous"],
    entry_points={
        "console_scripts": [
            "helper-script-a=source_wrong_exe_ambiguous.run:run",
            "helper-script-b=source_wrong_exe_ambiguous.run:run",
        ],
    },
)
