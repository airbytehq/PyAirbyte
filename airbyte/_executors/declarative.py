# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Support for declarative yaml source testing."""

from __future__ import annotations

import hashlib
import warnings
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, cast

import pydantic
import yaml

from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)

from airbyte._executors.base import Executor


if TYPE_CHECKING:
    from argparse import Namespace
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
        name: str,
        manifest: dict | Path,
        components_py: str | Path | None = None,
        components_py_checksum: str | None = None,
    ) -> None:
        """Initialize a declarative executor.

        - If `manifest` is a path, it will be read as a json file.
        - If `manifest` is a string, it will be parsed as an HTTP path.
        - If `manifest` is a dict, it will be used as is.
        - If `components_py` is provided, components will be injected into the source.
        - If `components_py_checksum` is not provided, it will be calculated automatically.
        """
        _suppress_cdk_pydantic_deprecation_warnings()

        self.name = name
        self._manifest_dict: dict
        if isinstance(manifest, Path):
            self._manifest_dict = cast("dict", yaml.safe_load(manifest.read_text()))

        elif isinstance(manifest, dict):
            self._manifest_dict = manifest

        config_dict: dict[str, Any] = {}
        if components_py:
            if isinstance(components_py, Path):
                components_py = components_py.read_text()

            if components_py_checksum is None:
                components_py_checksum = hashlib.md5(components_py.encode()).hexdigest()

            config_dict["__injected_components_py"] = components_py
            config_dict["__injected_components_py_checksums"] = {
                "md5": components_py_checksum,
            }

        self.declarative_source = ConcurrentDeclarativeSource(
            config=config_dict,
            source_config=self._manifest_dict,
        )

        self.reported_version: str | None = self._manifest_dict.get("version", None)

    def get_installed_version(
        self,
        *,
        raise_on_error: bool = False,
        recheck: bool = False,
    ) -> str | None:
        """Detect the version of the connector installed."""
        _ = raise_on_error, recheck  # Not used
        return self.reported_version

    @property
    def _cli(self) -> list[str]:
        """Not applicable."""
        return []  # N/A

    def execute(
        self,
        args: list[str],
        *,
        stdin: IO[str] | AirbyteMessageIterator | None = None,
        suppress_stderr: bool = False,
    ) -> Iterator[str]:
        """Execute the declarative source."""
        _ = stdin, suppress_stderr  # Not used
        source_entrypoint = AirbyteEntrypoint(self.declarative_source)

        mapped_args: list[str] = self.map_cli_args(args)
        parsed_args: Namespace = source_entrypoint.parse_args(mapped_args)
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
