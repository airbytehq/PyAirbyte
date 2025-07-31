"""Semantic version utilities for PyAirbyte."""

import sys
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from airbyte.logs import warn_once


def check_python_version_compatibility(
    package_name: str,
    requires_python: str | None,
) -> bool | None:
    """Check if current Python version is compatible with package requirements.

    Returns True if confirmed, False if incompatible, or None if no determination can be made.

    Args:
        package_name: Name of the package being checked
        requires_python: The requires_python constraint from PyPI (e.g., "<3.12,>=3.10")
    """
    if not requires_python:
        return None

    current_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )

    spec_set = SpecifierSet(requires_python)
    current_ver = Version(current_version)

    if current_ver not in spec_set:
        warn_once(
            f"Python version compatibility warning for '{package_name}': "
            f"Current Python {current_version} may not be compatible with "
            f"package requirement '{requires_python}'. "
            f"Installation will proceed but may fail.",
            with_stack=False,
        )
        return False
    return True
