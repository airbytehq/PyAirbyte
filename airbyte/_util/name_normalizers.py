# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Name normalizer classes."""

from __future__ import annotations

import abc
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytz

from airbyte._processors.sql.base import AB_EXTRACTED_AT_COLUMN, AB_LOADED_AT_COLUMN, AB_META_COLUMN


if TYPE_CHECKING:
    from collections.abc import Iterable

    from airbyte_protocol.models import (
        AirbyteRecordMessage,
    )


class NameNormalizerBase(abc.ABC):
    """Abstract base class for name normalizers."""

    @staticmethod
    @abc.abstractmethod
    def normalize(name: str) -> str:
        """Return the normalized name."""
        ...

    @classmethod
    def normalize_set(cls, str_iter: Iterable[str]) -> set[str]:
        """Converts string iterable to a set of lower case strings."""
        return {cls.normalize(s) for s in str_iter}

    @classmethod
    def normalize_list(cls, str_iter: Iterable[str]) -> list[str]:
        """Converts string iterable to a list of lower case strings."""
        return [cls.normalize(s) for s in str_iter]

    @classmethod
    def check_matched(cls, name1: str, name2: str) -> bool:
        """Return True if the two names match after each is normalized."""
        return cls.normalize(name1) == cls.normalize(name2)

    @classmethod
    def check_normalized(cls, name: str) -> bool:
        """Return True if the name is already normalized."""
        return cls.normalize(name) == name


class LowerCaseNormalizer(NameNormalizerBase):
    """A name normalizer that converts names to lower case."""

    @staticmethod
    def normalize(name: str) -> str:
        """Return the normalized name."""
        return name.lower().replace(" ", "_").replace("-", "_")


class StreamRecord(dict[str, Any]):
    """The StreamRecord class is a case-aware, case-insensitive dictionary implementation.

    It has these behaviors:
    - When a key is retrieved, deleted, or checked for existence, it is always checked in a
      case-insensitive manner.
    - The original case is stored in a separate dictionary, so that the original case can be
      retrieved when needed.

    This behavior mirrors how a case-aware, case-insensitive SQL database would handle column
    references.

    There are two ways this class can store keys internally:
    - If normalize_keys is True, the keys are normalized using the given normalizer.
    - If normalize_keys is False, the original case of the keys is stored.

    In regards to missing values, the dictionary accepts an 'expected_keys' input. When set, the
    dictionary will be initialized with the given keys. If a key is not found in the input data, it
    will be initialized with a value of None. When provided, the 'expected_keys' input will also
    determine the original case of the keys.
    """

    def _display_case(self, key: str) -> str:
        """Return the original case of the key."""
        return self._pretty_case_keys[self._normalizer.normalize(key)]

    def _index_case(self, key: str) -> str:
        """Return the internal case of the key.

        If normalize_keys is True, return the normalized key.
        Otherwise, return the original case of the key.
        """
        if self._normalize_keys:
            return self._normalizer.normalize(key)

        return self._display_case(key)

    @classmethod
    def from_record_message(
        cls,
        record_message: AirbyteRecordMessage,
        *,
        prune_extra_fields: bool,
        normalize_keys: bool = True,
        normalizer: type[NameNormalizerBase] | None = None,
        expected_keys: list[str] | None = None,
    ) -> StreamRecord:
        """Return a StreamRecord from a RecordMessage."""
        data_dict: dict[str, Any] = record_message.data.copy()
        data_dict[AB_EXTRACTED_AT_COLUMN] = record_message.emitted_at
        data_dict[AB_LOADED_AT_COLUMN] = datetime.now(tz=pytz.utc)
        data_dict[AB_META_COLUMN] = {}

        return cls(
            from_dict=data_dict,
            prune_extra_fields=prune_extra_fields,
            normalize_keys=normalize_keys,
            normalizer=normalizer,
            expected_keys=expected_keys,
        )

    def __init__(
        self,
        from_dict: dict,
        *,
        prune_extra_fields: bool,
        normalize_keys: bool = True,
        normalizer: type[NameNormalizerBase] | None = None,
        expected_keys: list[str] | None = None,
    ) -> None:
        """Initialize the dictionary with the given data.

        Args:
        - normalize_keys: If `True`, the keys will be normalized using the given normalizer.
        - expected_keys: If provided, the dictionary will be initialized with these given keys.
        - expected_keys: If provided and `prune_extra_fields` is True, then unexpected fields
          will be removed. This option is ignored if `expected_keys` is not provided.
        """
        # If no normalizer is provided, use LowerCaseNormalizer.
        self._normalize_keys = normalize_keys
        self._normalizer: type[NameNormalizerBase] = normalizer or LowerCaseNormalizer

        # If no expected keys are provided, use all keys from the input dictionary.
        if not expected_keys:
            expected_keys = list(from_dict.keys())
            prune_extra_fields = False  # No expected keys provided.

        # Store a lookup from normalized keys to pretty cased (originally cased) keys.
        self._pretty_case_keys: dict[str, str] = {
            self._normalizer.normalize(pretty_case.lower()): pretty_case
            for pretty_case in expected_keys
        }

        if normalize_keys:
            index_keys = [self._normalizer.normalize(key) for key in expected_keys]
        else:
            index_keys = expected_keys

        self.update({k: None for k in index_keys})  # Start by initializing all values to None
        for k, v in from_dict.items():
            index_cased_key = self._index_case(k)
            if prune_extra_fields and index_cased_key not in index_keys:
                # Dropping undeclared field
                continue

            self[index_cased_key] = v

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        if super().__contains__(key):
            return super().__getitem__(key)

        if super().__contains__(self._index_case(key)):
            return super().__getitem__(self._index_case(key))

        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        if super().__contains__(key):
            super().__setitem__(key, value)
            return

        if super().__contains__(self._index_case(key)):
            super().__setitem__(self._index_case(key), value)
            return

        # Store the pretty cased (originally cased) key:
        self._pretty_case_keys[self._normalizer.normalize(key)] = key

        # Store the data with the normalized key:
        super().__setitem__(self._index_case(key), value)

    def __delitem__(self, key: str) -> None:
        if super().__contains__(key):
            super().__delitem__(key)
            return

        if super().__contains__(self._index_case(key)):
            super().__delitem__(self._index_case(key))
            return

        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        assert isinstance(key, str), "Key must be a string."
        return super().__contains__(key) or super().__contains__(self._index_case(key))

    def __iter__(self) -> Any:  # noqa: ANN401
        return iter(super().__iter__())

    def __len__(self) -> int:
        return super().__len__()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, StreamRecord):
            return dict(self) == dict(other)

        if isinstance(other, dict):
            return {k.lower(): v for k, v in self.items()} == {
                k.lower(): v for k, v in other.items()
            }
        return False


__all__ = [
    "NameNormalizerBase",
    "LowerCaseNormalizer",
    "StreamRecord",
]
