# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Support for declarative yaml source testing."""

from __future__ import annotations

import contextlib
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
from airbyte_cdk.sources.declarative.retrievers.simple_retriever import SimpleRetriever
from airbyte_cdk.sources.types import StreamSlice

from airbyte import exceptions as exc
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


def _unwrap_to_declarative_stream(stream: object) -> object:
    """Unwrap a concurrent stream wrapper to access the underlying declarative stream.

    This function uses duck-typing to navigate through various wrapper layers that may
    exist around declarative streams, depending on the CDK version. It tries common
    wrapper attribute names and returns the first object that has a 'retriever' attribute.

    Args:
        stream: A stream object that may be wrapped (e.g., AbstractStream wrapper).

    Returns:
        The underlying declarative stream object with a retriever attribute.

    Raises:
        NotImplementedError: If unable to locate a declarative stream with a retriever.
    """
    if hasattr(stream, "retriever"):
        return stream

    wrapper_attrs = [
        "declarative_stream",
        "wrapped_stream",
        "stream",
        "_stream",
        "underlying_stream",
        "inner",
    ]

    for attr_name in wrapper_attrs:
        if hasattr(stream, attr_name):
            unwrapped = getattr(stream, attr_name)
            if unwrapped is not None and hasattr(unwrapped, "retriever"):
                return unwrapped

    for branch_attr in ["full_refresh_stream", "incremental_stream"]:
        if hasattr(stream, branch_attr):
            branch_stream = getattr(stream, branch_attr)
            if branch_stream is not None and hasattr(branch_stream, "retriever"):
                return branch_stream

    stream_type = type(stream).__name__
    raise NotImplementedError(
        f"Unable to locate declarative stream with retriever from {stream_type}. "
        f"fetch_record() requires access to the stream's retriever component."
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

    def fetch_record(  # noqa: PLR0914, PLR0912, PLR0915
        self,
        stream_name: str,
        primary_key_value: str,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Fetch a single record by primary key from a declarative stream.

        This method uses the already-instantiated streams from the declarative source
        to access the stream's retriever and make an HTTP GET request by appending
        the primary key value to the stream's base path (e.g., /users/123).

        Args:
            stream_name: The name of the stream to fetch from.
            primary_key_value: The primary key value as a string.
            config: Optional config overrides to merge with the executor's config.

        Returns:
            The fetched record as a dictionary.

        Raises:
            exc.AirbyteStreamNotFoundError: If the stream is not found.
            exc.AirbyteRecordNotFoundError: If the record is not found (empty response).
            NotImplementedError: If the stream does not use SimpleRetriever.
        """
        merged_config = {**self._config_dict, **(config or {})}

        streams = self.declarative_source.streams(merged_config)

        target_stream = None
        for stream in streams:
            stream_name_attr = getattr(stream, "name", None)
            if stream_name_attr == stream_name:
                target_stream = stream
                break
            try:
                unwrapped = _unwrap_to_declarative_stream(stream)
                if getattr(unwrapped, "name", None) == stream_name:
                    target_stream = stream
                    break
            except NotImplementedError:
                continue

        if target_stream is None:
            available_streams = []
            for s in streams:
                name = getattr(s, "name", None)
                if name:
                    available_streams.append(name)
            raise exc.AirbyteStreamNotFoundError(
                stream_name=stream_name,
                connector_name=self.name,
                available_streams=available_streams,
                message=f"Stream '{stream_name}' not found in source.",
            )

        declarative_stream = _unwrap_to_declarative_stream(target_stream)

        retriever = declarative_stream.retriever  # type: ignore[attr-defined]

        if not isinstance(retriever, SimpleRetriever):
            raise NotImplementedError(
                f"Stream '{stream_name}' uses {type(retriever).__name__}, but fetch_record() "
                "only supports SimpleRetriever."
            )

        empty_slice = StreamSlice(partition={}, cursor_slice={})
        base_path = retriever.requester.get_path(
            stream_state={},
            stream_slice=empty_slice,
            next_page_token=None,
        )

        if base_path:
            fetch_path = f"{base_path.rstrip('/')}/{primary_key_value}"
        else:
            fetch_path = primary_key_value

        response = retriever.requester.send_request(
            path=fetch_path,
            stream_state={},
            stream_slice=empty_slice,
            next_page_token=None,
            request_headers=retriever._request_headers(  # noqa: SLF001
                stream_slice=empty_slice,
                next_page_token=None,
            ),
            request_params=retriever._request_params(  # noqa: SLF001
                stream_slice=empty_slice,
                next_page_token=None,
            ),
            request_body_data=retriever._request_body_data(  # noqa: SLF001
                stream_slice=empty_slice,
                next_page_token=None,
            ),
            request_body_json=retriever._request_body_json(  # noqa: SLF001
                stream_slice=empty_slice,
                next_page_token=None,
            ),
        )

        if response is None:
            msg = (
                f"No response received when fetching record with primary key "
                f"'{primary_key_value}' from stream '{stream_name}'."
            )
            raise exc.AirbyteRecordNotFoundError(
                stream_name=stream_name,
                primary_key_value=primary_key_value,
                connector_name=self.name,
                message=msg,
            )

        records_schema = {}
        if hasattr(declarative_stream, "schema_loader"):
            schema_loader = declarative_stream.schema_loader
            if hasattr(schema_loader, "get_json_schema"):
                with contextlib.suppress(Exception):
                    records_schema = schema_loader.get_json_schema()

        records = list(
            retriever.record_selector.select_records(
                response=response,
                stream_state={},
                records_schema=records_schema,
                stream_slice=empty_slice,
                next_page_token=None,
            )
        )

        if not records:
            try:
                response_json = response.json()
                if isinstance(response_json, dict) and response_json:
                    return response_json
            except Exception:
                pass

            msg = (
                f"Record with primary key '{primary_key_value}' "
                f"not found in stream '{stream_name}'."
            )
            raise exc.AirbyteRecordNotFoundError(
                stream_name=stream_name,
                primary_key_value=primary_key_value,
                connector_name=self.name,
                message=msg,
            )

        first_record = records[0]
        if hasattr(first_record, "data"):
            return dict(first_record.data)  # type: ignore[arg-type]
        return dict(first_record)  # type: ignore[arg-type]
