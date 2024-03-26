# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A set of duck-typed classes for working with the Airbyte API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    import requests


class AirbyteApiResponseDuckType(Protocol):
    """Used for duck-typing various Airbyte API responses."""

    content_type: str
    r"""HTTP response content type for this operation"""
    status_code: int
    r"""HTTP response status code for this operation"""
    raw_response: requests.Response
    r"""Raw HTTP response; suitable for custom response parsing"""
