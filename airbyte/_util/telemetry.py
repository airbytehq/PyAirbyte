# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Telemetry implementation for PyAirbyte.

We track some basic telemetry to help us understand how PyAirbyte is used. You can opt-out of
telemetry at any time by setting the environment variable DO_NOT_TRACK to any value.

If you are able to provide telemetry, it is greatly appreciated. Telemetry helps us understand how
the library is used, what features are working. We also use this telemetry to prioritize bug fixes
and improvements to the connectors themselves, focusing first on connectors that are (1) most used
and (2) report the most sync failures as a percentage of total attempted syncs.

Your privacy and security are our priority. We do not track any PII (personally identifiable
information), nor do we track anything that _could_ contain PII without first hashing the data
using a one-way hash algorithm. We only track the minimum information necessary to understand how
PyAirbyte is used, and to dedupe users to determine how many users or use cases there are.


Here is what is tracked:
- The version of PyAirbyte.
- The Python version.
- The OS.
- The source type (venv or local install).
- The source name and version number.
- The state of the sync (started, failed, succeeded).
- The cache type (Snowflake, Postgres, etc.).
- The number of records processed.
- The application hash, which is a hash of either the notebook name or Python script name.
- Flags to help us understand if PyAirbyte is running on CI, Google Colab, or another environment.

"""
from __future__ import annotations

import datetime
import hashlib
import os
import sys
from contextlib import suppress
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from platform import freedesktop_os_release, python_implementation, python_version, system
from typing import Any

import requests
import ulid

from airbyte.version import get_version


HASH_SEED = "PyAirbyte:"
"""Additional seed for randomizing one-way hashed strings."""


def one_way_hash(string_to_hash: Any) -> str:
    """Return a one-way hash of the given string.

    To ensure a unique domain of hashes, we prepend a seed to the string before hashing.
    """
    return hashlib.sha256((HASH_SEED + str(string_to_hash)).encode()).hexdigest()


PYAIRBYTE_APP_TRACKING_KEY = (
    os.environ.get("AIRBYTE_TRACKING_KEY", "") or "cukeSffc0G6gFQehKDhhzSurDzVSZ2OP"
)
"""This key corresponds globally to the "PyAirbyte" application."""

PYAIRBYTE_SESSION_ID = str(ulid.ULID())
"""Unique identifier for the current invocation of PyAirbyte.

This is used to determine the order of operations within a specific session.
It is not a unique identifier for the user.
"""

COLAB_SESSION_URL = "https://colab.research.google.com/get_session"
"""URL to get the current Google Colab session information."""


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


def get_colab_release_version() -> str | None:
    if "COLAB_RELEASE_TAG" in os.environ:
        return os.environ["COLAB_RELEASE_TAG"]

    return None


def is_ci() -> bool:
    return "CI" in os.environ


def is_colab() -> bool:
    return bool(get_colab_release_version())


def is_jupyter() -> bool:
    """Return True if running in a Jupyter notebook or qtconsole."""
    try:
        shell = get_ipython().__class__.__name__
    except NameError:
        return False  # If 'get_ipython' undefined, we're probably in a standard Python interpreter.

    if shell == "ZMQInteractiveShell":
        return True  # Jupyter notebook or qtconsole.

    if shell == "TerminalInteractiveShell":
        return False  # Terminal running IPython

    return False  # Other type (?)


def get_flags() -> dict[str, Any]:
    flags: dict[str, bool] = {
        "CI": is_ci(),
        "GOOGLE_COLAB": is_colab(),
    }
    # Drop these flags if value is False
    return {k: v for k, v in flags.items() if v is False}


def get_notebook_name_hash() -> str | None:
    if is_colab():
        session_info = requests.get(COLAB_SESSION_URL).json()
        if session_info and "name" in session_info:
            return one_way_hash(session_info["name"])

    notebook_name = os.environ.get("AIRBYTE_NOTEBOOK_NAME", None)
    if notebook_name:
        return one_way_hash(notebook_name)

    return None


def get_vscode_notebook_name_hash() -> str | None:
    with suppress(Exception):
        import IPython

        return Path(IPython.extract_module_locals()[1]["__vsc_ipynb_file__"]).name

    return None


def get_python_script_name_hash() -> str | None:
    try:
        script_name: str = sys.argv[0]  # When running a python script, this is the script name.
    except Exception:
        return None

    if script_name:
        return one_way_hash(Path(script_name).name)

    return None


def get_application_hash() -> str | None:
    return (
        get_notebook_name_hash()
        or get_python_script_name_hash()
        or get_vscode_notebook_name_hash()
        or None
    )


def get_python_version() -> str:
    return f"{python_version()} ({python_implementation()})"


def get_os() -> str:
    if is_colab():
        return f"Google Colab ({get_colab_release_version()})"

    return f"{system()} ({freedesktop_os_release()})"


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
            "state": state,
            "source": asdict(source_info),
            "cache": asdict(cache_info),
            # explicitly set to 0.0.0.0 to avoid leaking IP addresses
            "application_hash": get_application_hash(),
            "ip": "0.0.0.0",
            "flags": get_flags(),
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
