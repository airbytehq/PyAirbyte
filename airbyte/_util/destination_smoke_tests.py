# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Shared implementation for destination smoke tests.

This module provides the core logic for running smoke tests against destination
connectors. It is used by both the CLI (`pyab destination-smoke-test`) and the
MCP tool (`destination_smoke_test`).

Smoke tests send synthetic data from the built-in smoke test source to a
destination connector and report whether the destination accepted the data
without errors.

When the destination has a compatible cache implementation, readback
introspection is automatically performed to produce stats on the written
data: table row counts, column names/types, and per-column null/non-null
counts.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import BaseModel

from airbyte import get_source
from airbyte.exceptions import PyAirbyteInputError
from airbyte.shared.sql_processor import TableStatistics  # noqa: TC001  # Pydantic needs at runtime


logger = logging.getLogger(__name__)


NAMESPACE_PREFIX = "zz_deleteme"
"""Prefix for auto-generated smoke test namespaces.

The `zz_` prefix sorts last alphabetically; `deleteme` signals the
namespace is safe for automated cleanup.
"""

DEFAULT_NAMESPACE_SUFFIX = "smoke_test"
"""Default suffix appended when no explicit suffix is provided."""


if TYPE_CHECKING:
    from airbyte.destinations.base import Destination
    from airbyte.sources.base import Source


def generate_namespace(
    *,
    namespace_suffix: str | None = None,
) -> str:
    """Generate a smoke-test namespace.

    Format: `zz_deleteme_yyyymmdd_hhmm_<suffix>`.
    The `zz_` prefix sorts last alphabetically and the `deleteme`
    token acts as a guard for automated cleanup scripts.

    If `namespace_suffix` is not provided, `smoke_test` is used as the
    default suffix.
    """
    suffix = namespace_suffix or DEFAULT_NAMESPACE_SUFFIX
    now = datetime.now(tz=timezone.utc)
    ts = now.strftime("%Y%m%d_%H%M")
    return f"{NAMESPACE_PREFIX}_{ts}_{suffix}"


# ---------------------------------------------------------------------------
# Smoke test result model
# ---------------------------------------------------------------------------


class DestinationSmokeTestResult(BaseModel):
    """Result of a destination smoke test run."""

    success: bool
    """Whether the smoke test passed (destination accepted all data without errors)."""

    destination: str
    """The destination connector name."""

    namespace: str
    """The namespace used for this smoke test run."""

    records_delivered: int
    """Total number of records delivered to the destination."""

    scenarios_requested: str
    """Which scenarios were requested ('all' or a comma-separated list)."""

    elapsed_seconds: float
    """Time taken for the smoke test in seconds."""

    error: str | None = None
    """Error message if the smoke test failed."""

    table_statistics: dict[str, TableStatistics] | None = None
    """Map of stream name to table statistics (row counts, columns, stats).

    Populated when the destination has a compatible cache, regardless of
    write success (to support partial-success inspection). `None` when
    the destination does not have a compatible cache.
    """

    tables_not_found: dict[str, str] | None = None
    """Stream names whose expected tables were not found in the destination.

    Maps stream name to the expected (normalized) table name that was
    looked up but not found. Populated alongside `table_statistics`.
    `None` when readback was not performed.
    """


def get_smoke_test_source(
    *,
    scenarios: str | list[str] = "fast",
    namespace: str | None = None,
    custom_scenarios: list[dict[str, Any]] | None = None,
    custom_scenarios_file: str | None = None,
) -> Source:
    """Create a smoke test source with the given configuration.

    The smoke test source generates synthetic data across predefined scenarios
    that cover common destination failure patterns.

    `scenarios` controls which scenarios to run:

    - `'fast'` (default): runs all fast (non-high-volume) predefined scenarios,
      excluding `large_batch_stream`.
    - `'all'`: runs every predefined scenario including `large_batch_stream`.
    - A comma-separated string or list of specific scenario names.

    `custom_scenarios` is an optional list of scenario dicts to inject directly.

    `namespace` is an optional namespace to set on all streams. When provided,
    the destination will write data into this namespace (schema, database, etc.).

    `custom_scenarios_file` is an optional path to a JSON or YAML file containing
    additional scenario definitions. Each scenario should have `name`, `json_schema`,
    and optionally `records` and `primary_key`.
    """
    # Normalize empty list to "fast" (default)
    if isinstance(scenarios, list) and not scenarios:
        scenarios = "fast"

    scenarios_str = ",".join(scenarios) if isinstance(scenarios, list) else scenarios
    keyword = scenarios_str.strip().lower()
    is_all = keyword == "all"
    is_fast = keyword == "fast"

    if is_all:
        source_config: dict[str, Any] = {
            "all_fast_streams": True,
            "all_slow_streams": True,
        }
    elif is_fast:
        source_config: dict[str, Any] = {
            "all_fast_streams": True,
            "all_slow_streams": False,
        }
    else:
        source_config: dict[str, Any] = {
            "all_fast_streams": False,
            "all_slow_streams": False,
        }
        if isinstance(scenarios, list):
            source_config["scenario_filter"] = [s.strip() for s in scenarios if s.strip()]
        else:
            source_config["scenario_filter"] = [
                s.strip() for s in scenarios.split(",") if s.strip()
            ]

    # Handle custom scenarios passed as a list of dicts (MCP path)
    if custom_scenarios:
        source_config["custom_scenarios"] = custom_scenarios

    # Handle custom scenarios from a file path (CLI path)
    if custom_scenarios_file:
        custom_path = Path(custom_scenarios_file)
        if not custom_path.exists():
            raise PyAirbyteInputError(
                message="Custom scenarios file not found.",
                input_value=str(custom_path),
            )
        loaded = yaml.safe_load(custom_path.read_text(encoding="utf-8"))
        if isinstance(loaded, list):
            file_scenarios = loaded
        elif isinstance(loaded, dict) and "custom_scenarios" in loaded:
            file_scenarios = loaded["custom_scenarios"]
        else:
            raise PyAirbyteInputError(
                message=(
                    "Custom scenarios file must contain a list of scenarios "
                    "or a dict with a 'custom_scenarios' key."
                ),
                input_value=str(custom_path),
            )
        # Merge with any directly-provided custom scenarios
        existing = source_config.get("custom_scenarios", [])
        source_config["custom_scenarios"] = existing + file_scenarios

    if namespace:
        source_config["namespace"] = namespace

    return get_source(
        name="source-smoke-test",
        config=source_config,
        streams="*",
        local_executable="source-smoke-test",
    )


def _sanitize_error(ex: Exception) -> str:
    """Extract an error message from an exception without leaking secrets.

    Uses `get_message()` when available (PyAirbyte exceptions) to avoid
    including full config/context in the error string.
    """
    if hasattr(ex, "get_message"):
        return f"{type(ex).__name__}: {ex.get_message()}"
    return f"{type(ex).__name__}: {ex}"


def run_destination_smoke_test(
    *,
    destination: Destination,
    scenarios: str | list[str] = "fast",
    namespace_suffix: str | None = None,
    reuse_namespace: str | None = None,
    custom_scenarios: list[dict[str, Any]] | None = None,
    custom_scenarios_file: str | None = None,
) -> DestinationSmokeTestResult:
    """Run a smoke test against a destination connector.

    Sends synthetic test data from the smoke test source to the specified
    destination and returns a structured result.

    When the destination has a compatible cache implementation, readback
    introspection is automatically performed (even on write failure, to
    support partial-success inspection).
    The readback produces stats on the written data (table row counts,
    column names/types, and per-column null/non-null counts) and is
    included in the result as `table_statistics` and `tables_not_found`.

    `destination` is a resolved `Destination` object ready for writing.

    `scenarios` controls which predefined scenarios to run:

    - `'fast'` (default): runs all fast (non-high-volume) predefined scenarios.
    - `'all'`: runs every scenario including `large_batch_stream`.
    - A comma-separated string or list of specific scenario names.

    `namespace_suffix` is an optional suffix appended to the auto-generated
    namespace. Defaults to `smoke_test` when not provided
    (e.g. `zz_deleteme_20260318_2256_smoke_test`).

    `reuse_namespace` is an exact namespace string to reuse from a previous
    run. When set, no new namespace is generated.

    `custom_scenarios` is an optional list of scenario dicts to inject.

    `custom_scenarios_file` is an optional path to a JSON/YAML file with
    additional scenario definitions.
    """
    # Determine namespace
    namespace = reuse_namespace or generate_namespace(
        namespace_suffix=namespace_suffix,
    )

    source_obj = get_smoke_test_source(
        scenarios=scenarios,
        namespace=namespace,
        custom_scenarios=custom_scenarios,
        custom_scenarios_file=custom_scenarios_file,
    )

    # Capture stream names for readback before the write consumes the source
    stream_names = source_obj.get_selected_streams()

    # Normalize scenarios to a display string
    if isinstance(scenarios, list):
        scenarios_str = ",".join(scenarios) if scenarios else "fast"
    else:
        scenarios_str = scenarios

    start_time = time.monotonic()
    success = False
    error_message: str | None = None
    records_delivered = 0
    try:
        write_result = destination.write(
            source_data=source_obj,
            cache=False,
            state_cache=False,
        )
        records_delivered = write_result.processed_records
        success = True
    except Exception as ex:
        error_message = _sanitize_error(ex)

    elapsed = time.monotonic() - start_time

    # Perform readback introspection (runs even on write failure for partial-success support)
    table_statistics: dict[str, TableStatistics] | None = None
    tables_not_found: dict[str, str] | None = None
    if destination.is_cache_supported:
        try:
            cache = destination.get_sql_cache(schema_name=namespace)
            table_statistics = cache.fetch_table_statistics(stream_names)
            tables_not_found = {
                name: cache.processor.get_sql_table_name(name)
                for name in stream_names
                if name not in table_statistics
            }
        except Exception:
            logger.warning(
                "Readback failed for destination '%s'.",
                destination.name,
                exc_info=True,
            )
    else:
        logger.info(
            "Skipping table and column statistics retrieval for "
            "destination '%s' because no SQL interface mapping has been "
            "defined.",
            destination.name,
        )

    return DestinationSmokeTestResult(
        success=success,
        destination=destination.name,
        namespace=namespace,
        records_delivered=records_delivered,
        scenarios_requested=scenarios_str,
        elapsed_seconds=round(elapsed, 2),
        error=error_message,
        table_statistics=table_statistics,
        tables_not_found=tables_not_found,
    )
