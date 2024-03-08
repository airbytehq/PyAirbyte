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
from contextlib import suppress
from dataclasses import asdict, dataclass
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import requests
import ulid

from airbyte import exceptions as exc
from airbyte._util import meta
from airbyte.version import get_version


if TYPE_CHECKING:
    from airbyte.caches.base import CacheBase
    from airbyte.sources.base import Source


HASH_SEED = "PyAirbyte:"
"""Additional seed for randomizing one-way hashed strings."""


PYAIRBYTE_APP_TRACKING_KEY = (
    os.environ.get("AIRBYTE_TRACKING_KEY", "") or "cukeSffc0G6gFQehKDhhzSurDzVSZ2OP"
)
"""This key corresponds globally to the "PyAirbyte" application."""


PYAIRBYTE_SESSION_ID = str(ulid.ULID())
"""Unique identifier for the current invocation of PyAirbyte.

This is used to determine the order of operations within a specific session.
It is not a unique identifier for the user.
"""


DO_NOT_TRACK = "DO_NOT_TRACK"
"""Environment variable to opt-out of telemetry."""


class SyncState(str, Enum):
    STARTED = "started"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


@dataclass
class CacheTelemetryInfo:
    type: str

    @classmethod
    def from_cache(cls, cache: CacheBase | None) -> CacheTelemetryInfo:
        if not cache:
            return cls(type="streaming")

        return cls(type=type(cache).__name__)


@dataclass
class SourceTelemetryInfo:
    name: str
    executor_type: str
    version: str | None

    @classmethod
    def from_source(cls, source: Source) -> SourceTelemetryInfo:
        return cls(
            name=source.name,
            executor_type=type(source.executor).__name__,
            version=source.executor.reported_version,
        )


def one_way_hash(
    string_to_hash: Any,  # noqa: ANN401  # Allow Any type
    /,
) -> str:
    """Return a one-way hash of the given string.

    To ensure a unique domain of hashes, we prepend a seed to the string before hashing.
    """
    return hashlib.sha256((HASH_SEED + str(string_to_hash)).encode()).hexdigest()


@lru_cache
def get_env_flags() -> dict[str, Any]:
    flags: dict[str, bool | str] = {
        "CI": meta.is_ci(),
        "NOTEBOOK_RUNTIME": (
            "GOOGLE_COLAB"
            if meta.is_colab()
            else "JUPYTER"
            if meta.is_jupyter()
            else "VS_CODE"
            if meta.is_vscode_notebook()
            else False
        ),
    }
    # Drop these flags if value is False or None
    return {k: v for k, v in flags.items() if v is not None and v is not False}


def send_telemetry(
    source: Source,
    cache: CacheBase | None,
    state: SyncState,
    number_of_records: int | None = None,
    exception: Exception | None = None,
) -> None:
    # If DO_NOT_TRACK is set, we don't send any telemetry
    if os.environ.get(DO_NOT_TRACK):
        return

    payload_props: dict[str, str | int | dict] = {
        "session_id": PYAIRBYTE_SESSION_ID,
        "source": asdict(SourceTelemetryInfo.from_source(source)),
        "cache": asdict(CacheTelemetryInfo.from_cache(cache)),
        "state": state,
        "version": get_version(),
        "python_version": meta.get_python_version(),
        "os": meta.get_os(),
        "application_hash": one_way_hash(meta.get_application_name()),
        "flags": get_env_flags(),
    }
    if exception:
        if isinstance(exception, exc.AirbyteError):
            payload_props["exception"] = exception.safe_logging_dict()
        else:
            payload_props["exception"] = {"class": type(exception).__name__}

    if number_of_records is not None:
        payload_props["number_of_records"] = number_of_records

    # Suppress exceptions if host is unreachable or network is unavailable
    with suppress(Exception):
        # Do not handle the response, we don't want to block the execution
        _ = requests.post(
            "https://api.segment.io/v1/track",
            auth=(PYAIRBYTE_APP_TRACKING_KEY, ""),
            json={
                "anonymousId": "airbyte-lib-user",
                "event": "sync",
                "properties": payload_props,
                "timestamp": datetime.datetime.utcnow().isoformat(),  # noqa: DTZ003
            },
        )
