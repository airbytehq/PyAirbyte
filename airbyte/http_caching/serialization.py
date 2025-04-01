# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Serialization formats for HTTP cache."""

from __future__ import annotations

import json
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, cast

from mitmproxy.io import io


if TYPE_CHECKING:
    from pathlib import Path

    from mitmproxy.http import HTTPFlow


logger = logging.getLogger(__name__)


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

        try:
            with path.open("rb") as f:
                fr = io.FlowReader(f)
                flows = list(fr.stream())

                if flows and len(flows) > 0:
                    flow = flows[0]
                    http_flow = cast("HTTPFlow", flow)
                    return {
                        "type": "http",
                        "request": http_flow.request.get_state() if http_flow.request else {},
                        "response": http_flow.response.get_state() if http_flow.response else None,
                        "error": http_flow.error.get_state()
                        if hasattr(http_flow, "error") and http_flow.error
                        else None,
                    }
        except Exception as e:
            logger.warning(f"Error reading flow file {path}: {e}")
            return {"type": "http", "request": {}, "response": None, "error": None}

        return {"type": "http", "request": {}, "response": None, "error": None}
