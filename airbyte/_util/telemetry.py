# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Telemetry implementation for PyAirbyte."""
from __future__ import annotations

import datetime
import os
from contextlib import suppress
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import requests

from airbyte.version import get_version


# This key corresponds globally to the "PyAirbyte" application
PYAIRBYTE_APP_TRACKING_KEY = (
    os.environ.get("AIRBYTE_TRACKING_KEY", "") or "cukeSffc0G6gFQehKDhhzSurDzVSZ2OP"
)


class SourceType(str, Enum):
    VENV = "venv"
    LOCAL_INSTALL = "local_install"


@dataclass
class CacheTelemetryInfo:
    type: str


streaming_cache_info = CacheTelemetryInfo("streaming")


class SyncState(str, Enum):
    STARTED = "started"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


@dataclass
class SourceTelemetryInfo:
    name: str
    type: SourceType
    version: str | None


def get_flags() -> dict[str, Any]:
    flags: dict[str, bool] = {
        "CI": bool(os.environ.get("CI")),
        "GOOGLE_COLAB": bool(os.environ.get("COLAB_GPU")),
    }
    # Drop flags where value is False
    return {k: v for k, v in flags.items() if v is False}


def get_notebook_name_hash() -> str | None:
    notebook_name = os.environ.get("AIRBYTE_NOTEBOOK_NAME", None)
    if notebook_name:
        return str(hash(notebook_name))

    return None


def get_python_script_name_hash() -> str | None:
    script_name = os.environ.get("AIRBYTE_PYTHON_SCRIPT_NAME", None)
    if script_name:
        return str(hash(os.path.basename(script_name)))  # noqa: PTH119

    return None


def get_application_hash() -> str | None:
    return get_notebook_name_hash() or get_python_script_name_hash() or None


def get_python_version() -> str:
    return f"{os.environ.get('AIRBYTE_PYTHON_VERSION', 'unknown')} ({os.environ.get('AIRBYTE_PYTHON_INTERPRETER', 'unknown')})"


def get_os() -> str:
    return os.environ.get("AIRBYTE_OS", "unknown")


def send_telemetry(
    source_info: SourceTelemetryInfo,
    cache_info: CacheTelemetryInfo,
    state: SyncState,
    number_of_records: int | None = None,
) -> None:
    # If DO_NOT_TRACK is set, we don't send any telemetry
    if os.environ.get("DO_NOT_TRACK"):
        return

    current_time: str = datetime.datetime.utcnow().isoformat()  # noqa: DTZ003 # prefer now() over utcnow()
    payload: dict[str, Any] = {
        "anonymousId": "airbyte-lib-user",
        "event": "sync",
        "properties": {
            "version": get_version(),
            "python_version": get_python_version(),
            "os": get_os(),
            "source": asdict(source_info),
            "state": state,
            "cache": asdict(cache_info),
            # explicitly set to 0.0.0.0 to avoid leaking IP addresses
            "ip": "0.0.0.0",
            "flags": get_flags(),
            "application_hash": get_application_hash(),
        },
        "timestamp": current_time,
    }
    if number_of_records is not None:
        payload["properties"]["number_of_records"] = number_of_records

    # Suppress exceptions if host is unreachable or network is unavailable
    with suppress(Exception):
        # Do not handle the response, we don't want to block the execution
        _ = requests.post(
            "https://api.segment.io/v1/track",
            auth=(PYAIRBYTE_APP_TRACKING_KEY, ""),
            json=payload,
        )
