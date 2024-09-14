# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""A cache implementation for Google Colab users, with files stored in Google Drive.

This is a thin wrapper around the default `DuckDBCache` implementation, streamlining
the process of mounting Google Drive and setting up a persistent cache in Google Colab.

Unlike the default `DuckDBCache`, this implementation will easily persist data across multiple
Colab sessions.

## Usage Examples

The default `get_colab_cache` arguments are suitable for most use cases:

```python
from airbyte.caches.colab import get_colab_cache

colab_cache = get_colab_cache()
```

Or you can call `get_colab_cache` with custom arguments:

```python
custom_cache = get_colab_cache(
    cache_name="my_custom_cache",
    sub_dir="Airbyte/custom_cache",
    drive_name="My Company Drive",
)
```
"""

from __future__ import annotations

from pathlib import Path

from airbyte.caches.duckdb import DuckDBCache


try:
    from google.colab import drive
except ImportError:
    # The `GoogleColabCache` class is only available in Google Colab.
    drive = None

MY_DRIVE = "MyDrive"
"""The default name of the user's personal Google Drive."""

GOOGLE_DRIVE_DEFAULT_MOUNT_PATH = "/content/drive"
"""The recommended path to mount Google Drive to."""


def get_colab_cache(
    cache_name: str = "colab_cache",
    sub_dir: str = "Airbyte/cache",
    drive_name: str = MY_DRIVE,
    mount_path: str = GOOGLE_DRIVE_DEFAULT_MOUNT_PATH,
) -> DuckDBCache:
    """Get a local cache for storing data, using the default database path.

    Unlike the default `DuckDBCache`, this implementation will easily persist data across multiple
    Colab sessions.

    Please note that Google Colab may prompt you to authenticate with your Google account to access
    your Google Drive. When prompted, click the link and follow the instructions.

    Colab will require access to read and write files in your Google Drive, so please be sure to
    grant the necessary permissions when prompted.

    All arguments are optional and have default values that are suitable for most use cases.

    Args:
        cache_name: The name to use for the cache. Defaults to "colab_cache". Override this if you
            want to use a different database for different projects.
        sub_dir: The subdirectory to store the cache in. Defaults to "Airbyte/cache". Override this
            if you want to store the cache in a different subdirectory than the default.
        drive_name: The name of the Google Drive to use. Defaults to "MyDrive". Override this if you
            want to store data in a shared drive instead of your personal drive.
        mount_path: The path to mount Google Drive to. Defaults to "/content/drive". Override this
            if you want to mount Google Drive to a different path (not recommended).
    """
    if not drive:
        msg = (
            "The `GoogleColabCache` class is only available in Google Colab. "
            "Please run this code in a Google Colab notebook."
        )
        raise ImportError(msg)

    drive.mount(mount_path)
    print(f"Successfully mounted Google Drive at `{mount_path}`.")
    drive_root = (
        Path(mount_path) / drive_name
        if drive_name == MY_DRIVE
        else Path(mount_path) / "Shareddrives" / drive_name
    )

    cache_dir = drive_root / sub_dir
    cache_dir.mkdir(parents=True, exist_ok=True)
    db_file_path = cache_dir / f"{cache_name}.duckdb"

    print(f"Creating persistent PyAirbyte cache in Google Drive: `{db_file_path}`.")
    return DuckDBCache(
        db_path=db_file_path,
        cache_dir=cache_dir,
    )
