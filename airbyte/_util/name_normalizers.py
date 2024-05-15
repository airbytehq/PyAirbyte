# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Name normalizer classes."""

from __future__ import annotations

import abc
import re
from typing import TYPE_CHECKING

from airbyte import exceptions as exc


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
        """Return the normalized name.

        - Any non-alphanumeric characters are replaced with underscores.
        - Any sequence of 3+ underscores is replaced with a double underscore.
        - Leading and trailing underscores are removed.
        - "%" is replaced with "pct".
        - "#" is replaced with "num".
        - "+" is replaced with "_plus_".

        Examples:
        - "Hello World!" -> "hello_world"
        - "Hello, World!" -> "hello__world"
        - "Hello - World" -> "hello__world"
        - "___Hello, World___" -> "hello_world"
        - "Average Sales (%)" -> "average_sales__pct"
        - "Average Sales (#)" -> "average_sales__num"
        - "+1" -> "plus_1"
        """
        result = name
        # Replace "%" or "#" with "pct" or "num".
        result = result.replace("%", "pct").replace("#", "num").replace("+", "_plus_")

        # Replace all non-alphanumeric characters with underscores.
        result = re.sub("[^A-Za-z0-9]", "_", result.lower())

        # Replace 3+ underscores with a double underscore.
        result = re.sub("___+", "__", result)

        # Remove leading and trailing underscores.
        result = result.rstrip("_").lstrip("_")

        # Check if name starts with a number and prepend "c" if it does.
        if result and result[0].isdigit():
            # Most databases do not allow identifiers to start with a number.
            result = f"c{result}"

        if not result:
            raise exc.PyAirbyteNameNormalizationError(
                message="Name cannot be empty after normalization.",
                raw_name=name,
                normalization_result=result,
            )

        return result


__all__ = [
    "NameNormalizerBase",
    "LowerCaseNormalizer",
]
