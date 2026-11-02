# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Private helpers for validating and updating Cloud connection settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte._util import api_util
from airbyte.exceptions import AirbyteLibInputError


if TYPE_CHECKING:
    from airbyte.cloud.connections import CloudConnection
    from airbyte.cloud.models import StreamConfigOverride


def _validate_update_connection_args(
    *,
    enabled: bool | None,
    cron_expression: str | None,
    interval_hours: int | None,
    manual_schedule: bool | None,
    override_stream_configs: list[StreamConfigOverride] | None,
) -> None:
    """Validate the connection settings before any API writes."""
    if not any(
        (
            enabled is not None,
            cron_expression is not None,
            interval_hours is not None,
            manual_schedule is not None,
            bool(override_stream_configs),
        )
    ):
        raise AirbyteLibInputError(
            message=(
                "At least one setting must be provided: 'enabled', 'interval_hours', "
                "'cron_expression', 'manual_schedule', or 'override_stream_configs'."
            ),
        )

    if sum((interval_hours is not None, cron_expression is not None, manual_schedule is True)) > 1:
        raise AirbyteLibInputError(
            message=(
                "Cannot specify more than one of 'interval_hours', 'cron_expression', "
                "and 'manual_schedule=True'."
            ),
        )

    if interval_hours is not None:
        api_util.validate_interval_hours(interval_hours)


def _set_connection_schedule(
    connection: CloudConnection,
    *,
    cron_expression: str | None,
    interval_hours: int | None,
    manual_schedule: bool | None,
) -> str | None:
    """Set the requested connection schedule and return its summary."""
    if cron_expression is not None:
        connection.set_schedule(cron_expression=cron_expression)
        return f"schedule set to '{cron_expression}'"
    if interval_hours is not None:
        connection.set_schedule(interval_hours=interval_hours)
        return f"schedule set to 'every {interval_hours} hours'"
    if manual_schedule is True:
        connection.set_manual_schedule()
        return "schedule set to 'manual'"
    return None
