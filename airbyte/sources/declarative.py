# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Support for declarative yaml source testing."""

from __future__ import annotations

from typing import TYPE_CHECKING

from source_declarative_manifest import run

from airbyte._executor import Executor
from airbyte.sources.base import Source


if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


class DeclarativeExecutor(Executor):
    """An executor for declarative sources."""

    def __init__(
        self,
        manifest: str | dict | Path,
    ) -> None:
        """Initialize a declarative executor."""
        self.manifest = manifest

    def execute(self, args: list[str]) -> Iterator[str]:
        """Execute the declarative source."""
        return run(args)

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
