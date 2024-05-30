# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Support for declarative yaml source testing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, cast

from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource

from airbyte._executor import Executor
from airbyte.sources.base import Source


if TYPE_CHECKING:
    from collections.abc import Iterator


class DeclarativeExecutor(Executor):
    """An executor for declarative sources."""

    def __init__(
        self,
        manifest: str | dict | Path,
    ) -> None:
        """Initialize a declarative executor.

        - If `manifest` is a path, it will be read as a json file.
        - If `manifest` is a string, it will be parsed as an HTTP path.
        - If `manifest` is a dict, it will be used as is.
        """
        self._manifest_dict: dict
        if isinstance(manifest, Path):
            self._manifest_dict = cast(dict, json.loads(manifest.read_text()))

        elif isinstance(manifest, str):
            # TODO: Implement HTTP path parsing
            raise NotImplementedError("HTTP path parsing is not yet implemented.")

        elif isinstance(manifest, dict):
            self._manifest_dict = manifest

        if not isinstance(self._manifest_dict, dict):
            raise ValueError("Manifest must be a dict.")

        self.declarative_source = ManifestDeclarativeSource(source_config=self._manifest_dict)

    def execute(self, args: list[str]) -> Iterator[str]:
        """Execute the declarative source."""
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


class DeclarativeSource(Source):
    """A declarative source using Airbyte's Yaml low-code/no-code framework."""

    def __init__(
        self,
        manifest: str | dict | Path,
    ) -> None:
        """Initialize a declarative source.

        Sample usages:
        ```python
        manifest_path = "path/to/manifest.yaml"

        source_a = DeclarativeSource(manifest=Path(manifest_path))
        source_b = DeclarativeSource(manifest=Path(manifest_path).read_text())
        source_c = DeclarativeSource(manifest=yaml.load(Path(manifest_path).read_text()))
        ```

        Args:
            manifest: The manifest for the declarative source. This can be a path to a yaml file, a
            yaml string, or a dict.
        """
        # TODO: Conform manifest to a dict or str (TBD)
        self.manifest = manifest

        # Initialize the source using the base class implementation
        super().__init__(
            name="Declarative",  # TODO: Get name from manifest
            config={  # TODO: Put 'real' config here
                "manifest": manifest,
            },
            executor=DeclarativeExecutor(manifest),
        )
