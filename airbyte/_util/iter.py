# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Utility functions for working with iterables."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, TypeVar

from airbyte import exceptions as exc


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


T = TypeVar("T")


class EmptyIterableError(ValueError):
    """An error raised when an iterable is unexpectedly empty."""


class MultipleMatchesError(ValueError):
    """An error raised when an iterable unexpectedly has more than one item."""


def exactly_one(
    iterable: Iterable[T],
) -> T:
    """Return the only item in an iterable.

    If there is not exactly one item, raises `EmptyIterableError` or `MultipleMatchesError`, both
    of which are subclasses of `ValueError`.
    """
    it: Iterator[T] = iter(iterable)
    try:
        result: T = next(it)
    except StopIteration:
        raise EmptyIterableError from None

    try:
        next(it)
    except StopIteration:
        return result

    raise MultipleMatchesError


def exactly_one_resource(
    iterable: Iterable[T],
) -> T:
    """Return the only item in an iterable.

    If there is not exactly one item, raises either `AirbyteMissingResourceError` or
    `AirbyteMultipleResourcesError`.
    """
    try:
        return exactly_one(iterable)
    except EmptyIterableError:
        raise exc.AirbyteMissingResourceError(
            message="Expected exactly one resource, but found none.",
        ) from None
    except MultipleMatchesError:
        raise exc.AirbyteMultipleResourcesError(
            message="Expected exactly one resource, but found multiple.",
        ) from None


def no_existing_resources(
    iterable: Iterable[T],
) -> None:
    """Raise an error if any resource exists in an iterable.

    Raises `AirbyteResourceAlreadyExistsError` if any resource exists in the iterable.
    """
    try:
        exactly_one(iterable)
    except EmptyIterableError:
        return

    raise exc.AirbyteResourceAlreadyExistsError
