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
from airbyte_cdk.sources.source import Source

from airbyte._executors.base import Executor


if TYPE_CHECKING:
    from argparse import Namespace
    from collections.abc import Iterator

    from airbyte_cdk.models import AirbyteStateMessage, ConfiguredAirbyteCatalog

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

    @staticmethod
    def _path_from_args(args: list[str], flag: str) -> Path | None:
        """The readable file named by `flag`, or None."""
        if flag not in args:
            return None
        index = args.index(flag) + 1
        if index >= len(args):
            return None
        path = Path(args[index])
        return path if path.is_file() else None

    def _config_from_args(self, args: list[str]) -> dict[str, Any]:
        """Read the connector config from the `--config <path>` CLI arg.

        Returns an empty dict when the arg is absent (as for `spec`), when the
        referenced file cannot be read, or when it does not contain a JSON object.
        Argument parsing and validation remain the responsibility of the CDK
        entrypoint.
        """
        config_path = self._path_from_args(args, "--config")
        if config_path is None:
            return {}

        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if not isinstance(loaded, dict):
            return {}
        return loaded

    def _state_from_args(self, args: list[str]) -> list[AirbyteStateMessage] | None:
        """Read incremental state from the `--state <path>` CLI arg.

        Returns None when the arg is absent or unreadable, matching
        `_config_from_args`: argument validation belongs to the CDK entrypoint.
        """
        path = self._path_from_args(args, "--state")
        if path is None:
            return None
        try:
            return Source.read_state(str(path))
        except (OSError, ValueError):
            return None

    def _catalog_from_args(self, args: list[str]) -> ConfiguredAirbyteCatalog | None:
        """Read the configured catalog from the `--catalog <path>` CLI arg."""
        path = self._path_from_args(args, "--catalog")
        if path is None:
            return None
        try:
            return Source.read_catalog(str(path))
        except (OSError, ValueError):
            return None

    def _build_declarative_source(
        self,
        config: dict[str, Any] | None = None,
        *,
        state: list[AirbyteStateMessage] | None = None,
        catalog: ConfiguredAirbyteCatalog | None = None,
    ) -> ConcurrentDeclarativeSource:
        """Build the declarative source, merging `config` over any injected components.

        Notes:
        1. Since Sep 2025, the declarative source class used is `ConcurrentDeclarativeSource`.
        2. The `ConcurrentDeclarativeSource` object sometimes doesn't want to be read from twice,
           likely due to threads being already shut down after a successful read.
        3. Rather than cache the source object, we recreate it each time we need it, to
           avoid any issues with re-using the same object.
        """
        return ConcurrentDeclarativeSource(
            config={**self._config_dict, **(config or {})},
            source_config=self._manifest_dict,
            catalog=catalog,
            state=state,
        )

    @property
    def declarative_source(self) -> ConcurrentDeclarativeSource:
        """The declarative source object, without connector config applied."""
        return self._build_declarative_source()

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
        # Config, state and catalog are constructor args: the declarative source
        # resolves interpolations and builds its state manager/cursors at
        # construction, and its `read()` ignores the `state` it is passed.
        source_entrypoint = AirbyteEntrypoint(
            self._build_declarative_source(
                self._config_from_args(args),
                state=self._state_from_args(args),
                catalog=self._catalog_from_args(args),
            )
        )

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
