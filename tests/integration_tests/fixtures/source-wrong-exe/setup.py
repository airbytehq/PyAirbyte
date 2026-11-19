#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from setuptools import setup

# Intentionally mismatched console script name — regression fixture for issue #290.
setup(
    name="airbyte-source-wrong-exe",
    version="0.0.1",
    description="Test Source with mismatched executable name",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=["source_wrong_exe"],
    entry_points={
        "console_scripts": [
            "wrong-script-name=source_wrong_exe.run:run",
        ],
    },
)
