# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Utility functions for working with caches."""

from __future__ import annotations

from pathlib import Path

import ulid

from airbyte import exceptions as exc
from airbyte.caches.duckdb import DuckDBCache


def get_default_cache() -> DuckDBCache:
    """Get a local cache for storing data, using the default database path.

    Cache files are stored in the `.cache` directory, relative to the current
    working directory.
    """
    cache_dir = Path("./.cache/default_cache")
    return DuckDBCache(
        db_path=cache_dir / "default_cache.duckdb",
        cache_dir=cache_dir,
    )


def new_local_cache(
    cache_name: str | None = None,
    cache_dir: str | Path | None = None,
    *,
    cleanup: bool = True,
) -> DuckDBCache:
    """Get a local cache for storing data, using a name string to seed the path.

    Args:
        cache_name: Name to use for the cache. Defaults to None.
        cache_dir: Root directory to store the cache in. Defaults to None.
        cleanup: Whether to clean up temporary files. Defaults to True.

    Cache files are stored in the `.cache` directory, relative to the current
    working directory.
    """
    if cache_name:
        if " " in cache_name:
            raise exc.PyAirbyteInputError(
                message="Cache name cannot contain spaces.",
                input_value=cache_name,
            )

        if not cache_name.replace("_", "").isalnum():
            raise exc.PyAirbyteInputError(
                message="Cache name can only contain alphanumeric characters and underscores.",
                input_value=cache_name,
            )

    cache_name = cache_name or str(ulid.ULID())
    cache_dir = cache_dir or Path(f"./.cache/{cache_name}")
    if not isinstance(cache_dir, Path):
        cache_dir = Path(cache_dir)

    return DuckDBCache(
        db_path=cache_dir / f"db_{cache_name}.duckdb",
        cache_dir=cache_dir,
        cleanup=cleanup,
    )
