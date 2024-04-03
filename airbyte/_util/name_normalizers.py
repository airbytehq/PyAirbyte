# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Name normalizer classes."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterable


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


__all__ = [
    "NameNormalizerBase",
    "LowerCaseNormalizer",
]
