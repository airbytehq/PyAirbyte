# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""___"""

from __future__ import annotations

import warnings
from typing import Any, cast

from airbyte import exceptions as exc
from airbyte.secrets.base import SecretManager, SecretSourceEnum, SecretString
from airbyte.secrets.config import _get_secret_sources


def get_secret(
    secret_name: str,
    /,
    *,
    sources: list[SecretManager | SecretSourceEnum] | None = None,
    allow_prompt: bool = True,
    **kwargs: dict[str, Any],
) -> SecretString:
    """Get a secret from the environment.

    The optional `sources` argument of enum type `SecretSourceEnum` or list of `SecretSourceEnum`
    options. If left blank, all available sources will be checked. If a list of `SecretSourceEnum`
    entries is passed, then the sources will be checked using the provided ordering.

    If `allow_prompt` is `True` or if SecretSourceEnum.PROMPT is declared in the `source` arg, then
    the user will be prompted to enter the secret if it is not found in any of the other sources.
    """
    if "source" in kwargs:
        warnings.warn(
            message="The `source` argument is deprecated. Use the `sources` argument instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        sources = kwargs.pop("source")

    available_sources: dict[str, SecretManager] = {}
    for available_source in _get_secret_sources():
        # Add available sources to the dict. Order matters.
        available_sources[available_source.name] = available_source

    if sources is None:
        # If ANY is in the list, then we don't need to check any other sources.
        # This is the default behavior.
        sources = list(available_sources.values())

    elif not isinstance(sources, list):
        sources = [sources]

    # Replace any SecretSourceEnum strings with the matching SecretManager object
    for source in sources:
        if isinstance(source, SecretSourceEnum):
            if source not in available_sources:
                raise exc.PyAirbyteInputError(
                    guidance="Invalid secret source name.",
                    input_value=source,
                    context={
                        "Available Sources": list(available_sources.keys()),
                    },
                )

            sources[sources.index(source)] = available_sources[source]

    secret_managers = cast(list[SecretManager], sources)

    if SecretSourceEnum.PROMPT in secret_managers:
        prompt_source = secret_managers.pop(
            secret_managers.index(SecretSourceEnum.PROMPT),
        )

        if allow_prompt:
            # Always check prompt last. Add it to the end of the list.
            secret_managers.append(prompt_source)

    for secret_mgr in secret_managers:
        val = secret_mgr.get_secret(secret_name)
        if val:
            return SecretString(val)

    raise exc.PyAirbyteSecretNotFoundError(
        secret_name=secret_name,
        sources=[str(s) for s in available_sources],
    )
