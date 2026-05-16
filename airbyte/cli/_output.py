# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Output helpers for CLI commands."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, ClassVar, Protocol, TypeAlias

import orjson
from pydantic import BaseModel


class _DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]


JsonOutputValue: TypeAlias = (
    str
    | dict[str, Any]
    | list[dict[str, Any]]
    | BaseModel
    | list[BaseModel]
    | _DataclassInstance
    | list[_DataclassInstance | dict[str, Any]]
)


def _json_default(value: object) -> object:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Enum):
        return value.value
    raise TypeError


def json_output(value: JsonOutputValue) -> None:
    """Print a JSON response to stdout."""
    print(orjson.dumps(value, default=_json_default, option=orjson.OPT_INDENT_2).decode())
