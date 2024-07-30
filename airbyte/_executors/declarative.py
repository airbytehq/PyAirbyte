# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Support for declarative yaml source testing."""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import IO, TYPE_CHECKING, cast

import pydantic

from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource

from airbyte._executors.base import Executor
from airbyte.exceptions import PyAirbyteInternalError


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte._message_iterators import AirbyteMessageIterator


def _suppress_cdk_pydantic_deprecation_warnings() -> None:
    """Suppress deprecation warnings from Pydantic in the CDK.

    CDK has deprecated uses of `json()` and `parse_obj()`, and we don't want users
    to see these warnings.
    """
    warnings.filterwarnings(
        "ignore",
        category=pydantic.warnings.PydanticDeprecatedSince20,
    )


class DeclarativeExecutor(Executor):
    """An executor for declarative sources."""

    def __init__(
        self,
        manifest: dict | Path,
    ) -> None:
        """Initialize a declarative executor.

        - If `manifest` is a path, it will be read as a json file.
        - If `manifest` is a string, it will be parsed as an HTTP path.
        - If `manifest` is a dict, it will be used as is.
        """
        _suppress_cdk_pydantic_deprecation_warnings()
        self._manifest_dict: dict
        if isinstance(manifest, Path):
            self._manifest_dict = cast(dict, json.loads(manifest.read_text()))

        elif isinstance(manifest, dict):
            self._manifest_dict = manifest

        if not isinstance(self._manifest_dict, dict):
            raise PyAirbyteInternalError(message="Manifest must be a dict.")

        self.declarative_source = ManifestDeclarativeSource(source_config=self._manifest_dict)

        # TODO: Consider adding version detection
        # https://github.com/airbytehq/airbyte/issues/318
        self.reported_version: str | None = None

    @property
    def _cli(self) -> list[str]:
        """Not applicable."""
        return []  # N/A

    def execute(
        self,
        args: list[str],
        *,
        stdin: IO[str] | AirbyteMessageIterator | None = None,
    ) -> Iterator[str]:
        """Execute the declarative source."""
        _ = stdin  # Not used
        source_entrypoint = AirbyteEntrypoint(self.declarative_source)
        parsed_args = source_entrypoint.parse_args(args)
        yield from source_entrypoint.run(parsed_args)

    def ensure_installation(self, *, auto_fix: bool = True) -> None:
        """No-op. The declarative source is included with PyAirbyte."""
        _ = auto_fix
        pass

    def install(self) -> None:
        """No-op. The declarative source is included with PyAirbyte."""
        pass

    def uninstall(self) -> None:
        """No-op. The declarative source is included with PyAirbyte."""
        pass
