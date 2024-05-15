# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Resources for working with Airbyte Cloud in PyAirbyte."""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Protocol


AllowedAny = Any  # When Any is allowed


# Define interface for generic resource info response
class ResourceInfoResponse(Protocol):
    """An interface for resource info responses from the Airbyte Cloud API.

    This interface is used to define the expected structure of resource info responses
    from the Airbyte Cloud API.
    """


# Decorator that makes sure the resource info is fetched before calling a method
def requires_fetch(func: Callable[..., Any]) -> Callable[..., Any]:
    """A decorator that fetches the resource info before calling the decorated method.

    This decorator is used to ensure that the resource info is fetched before calling a method
    that requires the resource info.
    """

    @wraps(func)
    def wrapper(
        self: ICloudResource,
        *args: AllowedAny,
        **kwargs: AllowedAny,
    ) -> AllowedAny:
        if not self._resource_info:
            self._resource_info = self._fetch_resource_info()

        return func(self, *args, **kwargs)

    return wrapper


class ICloudResource(Protocol):
    """A resource in Airbyte Cloud.

    You can use a resource object to retrieve information about the resource and manage the
    resource.
    """

    _resource_info: ResourceInfoResponse | None

    def _fetch_resource_info(self) -> ResourceInfoResponse:
        """Populate the resource with data from the API."""
        ...
