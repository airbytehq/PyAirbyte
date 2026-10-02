# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `as_temp_files` file permissions."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from airbyte._util.temp_files import as_temp_files


def test_temp_config_can_be_rewritten() -> None:
    """Connectors with `config_migrations` rewrite the config file in place."""
    with as_temp_files([{"api_key": "before"}]) as (path,):
        Path(path).write_text(json.dumps({"api_key": "after"}), encoding="utf-8")
        assert json.loads(Path(path).read_text(encoding="utf-8")) == {
            "api_key": "after"
        }


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_group_and_other_cannot_write() -> None:
    """Owner write only -- the file holds credentials."""
    with as_temp_files([{"api_key": "value"}]) as (path,):
        mode = Path(path).stat().st_mode
        assert mode & stat.S_IWUSR, "owner must be able to write"
        assert not mode & stat.S_IWGRP, "group must not be able to write"
        assert not mode & stat.S_IWOTH, "other must not be able to write"
