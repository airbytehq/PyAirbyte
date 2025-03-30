# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Serialization formats for HTTP cache."""

from __future__ import annotations

import json
import pickle
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol


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

    BINARY = "binary"
    """Binary format using pickle."""


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


class BinarySerializer:
    """Serializer that uses binary format (pickle)."""

    def serialize(self, data: T_SerializedData, path: Path) -> None:
        """Serialize data to a binary file using pickle.

        Args:
            data: The data to serialize.
            path: The path to write the serialized data to.
        """
        with path.open("wb") as f:
            pickle.dump(data, f)

    def deserialize(self, path: Path) -> T_SerializedData:
        """Deserialize data from a binary file using pickle.

        Args:
            path: The path to read the serialized data from.

        Returns:
            The deserialized data.
        """
        with path.open("rb") as f:
            return pickle.load(f)
