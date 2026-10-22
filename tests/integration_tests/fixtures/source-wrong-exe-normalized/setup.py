#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from setuptools import setup

setup(
    name="Airbyte_Source_Wrong_Exe",
    version="0.0.1",
    description="Test Source with normalized distribution name",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=["source_wrong_exe_normalized"],
    entry_points={
        "console_scripts": [
            "normalized-script-name=source_wrong_exe_normalized.run:run",
        ],
    },
)
