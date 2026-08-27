# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Shared argument handling for Agents lookups by ID or name."""

from __future__ import annotations

from airbyte.exceptions import PyAirbyteInputError


def resolve_id_or_name(
    *,
    id_args: dict[str, str | None],
    name: str | None,
) -> str | None:
    """Return the ID to look up, or `None` when the lookup is by name.

    The keys of `id_args` are synonyms for the same ID, so callers may pass a short `id`
    alongside a specific alias such as `connector_id`. Exactly one of the ID aliases or
    `name` is required, and conflicting ID values are rejected.
    """
    provided = {key: value for key, value in id_args.items() if value}
    id_arg_names = " or ".join(f"`{key}`" for key in id_args)

    if len(set(provided.values())) > 1:
        raise PyAirbyteInputError(
            message=f"{id_arg_names} were given conflicting values.",
            guidance="These arguments are synonyms, so pass only one of them.",
            context={"provided": sorted(provided)},
        )

    if bool(provided) == bool(name):
        raise PyAirbyteInputError(
            message=f"Exactly one of {id_arg_names} or `name` is required.",
            guidance=f"Provide either {id_arg_names} or `name`, but not both.",
        )

    return next(iter(provided.values()), None)
