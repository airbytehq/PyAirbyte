# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Support for declarative yaml source testing."""

from __future__ import annotations

import hashlib
import json
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

        self.reported_version: str | None = self._manifest_dict.get("version", None)
        self._config_dict = config_dict

    @property
    def declarative_source(self) -> ConcurrentDeclarativeSource:
        """Get the declarative source object.

        Notes:
        1. Since Sep 2025, the declarative source class used is `ConcurrentDeclarativeSource`.
        2. The `ConcurrentDeclarativeSource` object sometimes doesn't want to be read from twice,
           likely due to threads being already shut down after a successful read.
        3. Rather than cache the source object, we recreate it each time we need it, to
           avoid any issues with re-using the same object.
        """
        return ConcurrentDeclarativeSource(
            config=self._config_dict,
            source_config=self._manifest_dict,
        )

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

    def _load_config_from_args(self, args: list[str]) -> None:
        """Merge the config passed as `--config <path>` into the executor config.

        `ConcurrentDeclarativeSource` resolves `{{ config[...] }}` interpolations
        against the config it is constructed with. Without this, the source is
        built with an effectively empty config and every interpolation fails, e.g.
        `'dict object' has no attribute 'api_key'`, or `SelectiveAuthenticator`
        raising "The path from `authenticator_selection_path` is not found in the
        config."

        Injected component keys already in `_config_dict` take precedence, so
        `__injected_components_py` is never overwritten by user config.
        """
        if "--config" not in args:
            return

        config_index = args.index("--config") + 1
        if config_index >= len(args):
            return

        try:
            config_path = Path(args[config_index])
            user_config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # A malformed or unreadable config is reported downstream by the
            # entrypoint with a much better error message than we could give here.
            return

        if isinstance(user_config, dict):
            merged: dict[str, Any] = dict(user_config)
            merged.update(self._config_dict)
            self._config_dict = merged

    def execute(
        self,
        args: list[str],
        *,
        stdin: IO[str] | AirbyteMessageIterator | None = None,
        suppress_stderr: bool = False,
    ) -> Iterator[str]:
        """Execute the declarative source."""
        _ = stdin, suppress_stderr  # Not used
        self._load_config_from_args(args)
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
