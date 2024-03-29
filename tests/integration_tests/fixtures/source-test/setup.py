#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from setuptools import setup

setup(
    name="airbyte-source-test",
    version="0.0.1",
    description="Test Source",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=["source_test"],
    entry_points={
        "console_scripts": [
            "source-test=source_test.run:run",
        ],
    },
)
