# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Serialization formats for HTTP cache."""

from __future__ import annotations

import json
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

from mitmproxy.io import io


if TYPE_CHECKING:
    from pathlib import Path

T_SerializedData = dict[str, Any]


class Serializer(Protocol):
    """Protocol for serializers."""

    def serialize(self, data: T_SerializedData, path: Path) -> None:
        """Serialize data to a file.

        Args:
            data: The data to serialize.
            path: The path to write the serialized data to.
        """
        ...

    def deserialize(self, path: Path) -> T_SerializedData:
        """Deserialize data from a file.

        Args:
            path: The path to read the serialized data from.

        Returns:
            The deserialized data.
        """
        ...


class SerializationFormat(str, Enum):
    """The format to use for serializing HTTP cache data."""

    JSON = "json"
    """Human-readable JSON format."""

    NATIVE = "native"
    """Native mitmproxy format, interoperable with mitmproxy tools."""


class JsonSerializer:
    """Serializer that uses JSON format."""

    def serialize(self, data: T_SerializedData, path: Path) -> None:
        """Serialize data to a JSON file.

        Args:
            data: The data to serialize.
            path: The path to write the serialized data to.
        """
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def deserialize(self, path: Path) -> T_SerializedData:
        """Deserialize data from a JSON file.

        Args:
            path: The path to read the serialized data from.

        Returns:
            The deserialized data.
        """
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)


class NativeSerializer:
    """Serializer that uses mitmproxy's native format."""

    def serialize(self, data: T_SerializedData, path: Path) -> None:
        """Serialize data to a mitmproxy-compatible file.

        Args:
            data: The data to serialize.
            path: The path to write the serialized data to.
        """
        path.parent.mkdir(parents=True, exist_ok=True)

        if not str(path).endswith(".mitm"):
            path = path.with_suffix(".mitm")

        with path.open("wb") as f:
            fw = io.FlowWriter(f)
            for flow in data.get("flows", []):
                fw.add(flow)

    def deserialize(self, path: Path) -> T_SerializedData:
        """Deserialize data from a mitmproxy-compatible file.

        Args:
            path: The path to read the serialized data from.

        Returns:
            The deserialized data.
        """
        if not str(path).endswith(".mitm"):
            path = path.with_suffix(".mitm")

        if not path.exists():
            return {"flows": []}

        with path.open("rb") as f:
            fr = io.FlowReader(f)
            flows = list(fr.stream())

        return {"flows": flows}
