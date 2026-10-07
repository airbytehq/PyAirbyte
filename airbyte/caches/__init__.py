# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Base module for all caches."""

from __future__ import annotations

import pkgutil

import lazy_loader


# Public names are declared once in `__init__.pyi` and loaded on first attribute access, so
# importing one cache module doesn't import every cache backend (BigQuery, Snowflake, etc.).
__getattr__, __dir__, _lazy_names = lazy_loader.attach_stub(__name__, __file__)

_SUBMODULE_NAMES = {module.name for module in pkgutil.iter_modules(__path__)}
__all__ = [name for name in _lazy_names if name not in _SUBMODULE_NAMES]
