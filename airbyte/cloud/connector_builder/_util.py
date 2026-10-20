# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Internal helpers for Connector Builder projects: manifest text, specs, and readiness."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from airbyte import exceptions as exc
from airbyte.cloud.connector_builder._models import (
    ConnectorBuilderReadinessCheck,
    ConnectorBuilderStreamReadiness,
    ManifestTextEditMode,
    ReadinessCheckResult,
)


_MANIFEST_KEYS = frozenset({"streams", "definitions", "check"})
_YAML_LINE_WIDTH = 2**31 - 1
_SECRETS_MASK = "**********"
"""Placeholder Cloud returns instead of a stored secret value."""
CONNECTOR_BUILDER_TEST_DESTINATION_NAME = "Connector Builder Test (No-op)"
"""Name of the no-op destination that full syncs find or create by default."""
TEST_READ_MAX_RECORDS = 5_000
"""Most records read per stream by `_run_readiness_checks()`."""
TEST_READ_MAX_PAGES = 20
TEST_READ_MAX_SLICES = 20


def unwrap_builder_manifest(value: object) -> dict[str, Any] | None:
    """Return the connector manifest from a builder manifest payload.

    Some builder projects store their draft as a `DeclarativeSourceManifest`
    envelope (`{"manifest": {...}, "spec": ..., "version": ...}`) rather than
    the bare manifest; unwrap that envelope so the result matches the shape of
    `manifest`.
    """
    if not isinstance(value, dict):
        return None
    nested = value.get("manifest")
    if isinstance(nested, dict) and not any(key in value for key in _MANIFEST_KEYS):
        return nested
    return value


def render_manifest_text(manifest: dict[str, Any]) -> str:
    """Render a manifest as deterministic YAML text, without line wrapping.

    Airbyte Cloud stores the parsed manifest, not its original text, so comments and
    formatting are not preserved. Text edits match against this rendering.
    """
    return yaml.safe_dump(
        manifest,
        sort_keys=False,
        allow_unicode=True,
        width=_YAML_LINE_WIDTH,
    )


def apply_manifest_text_edit(
    current_text: str,
    *,
    edit_mode: ManifestTextEditMode,
    new_text: str,
    old_text: str | None = None,
) -> str:
    """Return `current_text` after applying one text edit.

    Args:
        current_text: The text to edit.
        edit_mode: See `ManifestTextEditMode`.
        new_text: The whole new text for `replace_full`, else the replacement for `old_text`.
        old_text: The exact text to replace. Required unless `edit_mode` is `replace_full`.
    """
    if edit_mode == "replace_full":
        if old_text is not None:
            raise exc.AirbyteLibInputError(
                message="`old_text` is not allowed when `edit_mode` is `replace_full`.",
            )
        return new_text

    if not old_text:
        raise exc.AirbyteLibInputError(
            message=f"`old_text` is required when `edit_mode` is `{edit_mode}`.",
        )
    match_count = current_text.count(old_text)
    if match_count == 0:
        raise exc.AirbyteLibInputError(
            message="`old_text` was not found in the draft manifest text.",
            guidance="Copy `old_text` verbatim from the latest draft manifest text.",
        )
    if edit_mode == "replace_text" and match_count > 1:
        raise exc.AirbyteLibInputError(
            message=f"`old_text` matches {match_count} places in the draft manifest text.",
            guidance=(
                "Include more surrounding text in `old_text` so it matches once, "
                "or use `replace_all` to replace every match."
            ),
            context={"match_count": match_count},
        )
    return current_text.replace(old_text, new_text)


def parse_manifest_yaml(manifest_yaml: dict[str, Any] | Path | str) -> dict[str, Any]:
    """Return a manifest dict from a dict, a YAML file path, or YAML text."""
    if isinstance(manifest_yaml, Path):
        manifest_yaml = manifest_yaml.read_text()
    if isinstance(manifest_yaml, str):
        manifest_yaml = yaml.safe_load(manifest_yaml)
    if not isinstance(manifest_yaml, dict):
        raise exc.AirbyteLibInputError(message="The manifest must be a YAML mapping.")
    return manifest_yaml


def builder_testing_values_spec(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build the `spec` object that the testing-values API expects, as the Builder UI does."""
    manifest_spec = manifest.get("spec") or {}
    return {
        "documentationUrl": manifest_spec.get("documentation_url"),
        "connectionSpecification": manifest_spec.get("connection_specification") or {},
        "advancedAuth": manifest_spec.get("advanced_auth"),
    }


def publish_spec(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build the connector spec for publishing, with the API's camelCase field names."""
    return {**(manifest.get("spec") or {}), **builder_testing_values_spec(manifest)}


def draft_from_project_data(project_data: dict[str, Any]) -> dict[str, Any] | None:
    if not project_data["builderProject"].get("hasDraft"):
        return None
    declarative_manifest = project_data.get("declarativeManifest") or {}
    return unwrap_builder_manifest(declarative_manifest.get("manifest"))


def _result(passed: bool) -> ReadinessCheckResult:  # noqa: FBT001
    return "passed" if passed else "failed"


def readiness_next_steps(
    checks: list[ConnectorBuilderReadinessCheck],
    streams: list[ConnectorBuilderStreamReadiness],
    pending_streams: list[str],
    not_attempted_streams: list[str],
) -> list[str]:
    steps = [check.description for check in checks[:4] if check.result != "passed"]
    failed = [stream.stream_name for stream in streams if stream.failed]
    capped = [
        stream.stream_name
        for stream in streams
        if stream.error is None and not stream.http_errors and not stream.read_to_end
    ]
    if failed:
        steps.append(f"Fix the failed streams and test them again: {', '.join(failed)}")
    if pending_streams:
        steps.append(
            "These streams were still reading at the timeout, so their results were dropped. "
            "Retry them with a longer `timeout_seconds`, or check them with the full sync: "
            f"{', '.join(pending_streams)}"
        )
    if not_attempted_streams and not failed:
        steps.append(
            "Run the report again with the not-attempted streams, skipping any already "
            f"validated or unchanged since: {', '.join(not_attempted_streams)}"
        )
    if capped:
        steps.append(
            "These streams hit a test-read cap, so later pages weren't read; "
            f"check them with a full sync: {', '.join(capped)}"
        )
    warning_streams = [stream.stream_name for stream in streams if stream.warnings]
    if warning_streams:
        steps.append(f"Report these streams' `warnings` to the user: {', '.join(warning_streams)}")
    return steps


def readiness_checks(
    project_data: dict[str, Any],
    draft: dict[str, Any],
    streams: list[ConnectorBuilderStreamReadiness],
    pending_streams: list[str],
    url: str,
) -> list[ConnectorBuilderReadinessCheck]:
    spec = (draft.get("spec") or {}).get("connection_specification")
    takes_no_inputs = isinstance(spec, dict) and not spec.get("properties")
    testing_values = project_data.get("testingValues") or {}
    source_definition_id = project_data["builderProject"].get("sourceDefinitionId")
    failed = [stream.stream_name for stream in streams if stream.failed]
    capped = [
        stream.stream_name
        for stream in streams
        if stream.error is None and not stream.http_errors and not stream.read_to_end
    ]
    not_read = [
        stream.stream_name for stream in streams if stream.error is not None or stream.http_errors
    ]
    return [
        ConnectorBuilderReadinessCheck(
            description="Save a draft manifest",
            result=_result(bool(project_data["builderProject"].get("hasDraft"))),
        ),
        ConnectorBuilderReadinessCheck(
            description="Define the connection spec (user inputs)",
            result=_result(isinstance(spec, dict)),
        ),
        ConnectorBuilderReadinessCheck(
            description="Define at least one stream",
            result=_result(bool(draft.get("streams")) or bool(draft.get("dynamic_streams"))),
        ),
        ConnectorBuilderReadinessCheck(
            description="Set testing values (enter secrets in the Connector Builder UI)",
            result=_result(bool(testing_values) or takes_no_inputs),
            detail=url,
        ),
        ConnectorBuilderReadinessCheck(
            description="Test-read the named streams without errors",
            result=_result(not failed and not pending_streams),
            detail="; ".join(
                part
                for part in (
                    f"failed: {', '.join(failed)}" if failed else "",
                    f"pending: {', '.join(pending_streams)}" if pending_streams else "",
                )
                if part
            )
            or None,
        ),
        ConnectorBuilderReadinessCheck(
            description="Read the named streams to the end within the test-read caps",
            result=_result(not pending_streams and all(stream.read_to_end for stream in streams)),
            detail="; ".join(
                part
                for part in (
                    (
                        f"hit a cap: {', '.join(capped)}. Check these with a full sync "
                        "after publishing."
                    )
                    if capped
                    else "",
                    f"not read: {', '.join(not_read)} (test read failed)" if not_read else "",
                )
                if part
            )
            or None,
        ),
        ConnectorBuilderReadinessCheck(
            description="Publish the custom source definition",
            result=_result(source_definition_id is not None),
            detail=source_definition_id,
        ),
        ConnectorBuilderReadinessCheck(
            description="Run a full sync of every stream to a no-op destination",
            result="unknown",
            detail="Not tracked by this report. Run it after publishing.",
        ),
    ]


def drop_masked_secrets(value: Any) -> Any:  # noqa: ANN401
    """Return a copy of `value` without the masked secret fields Cloud returns."""
    if isinstance(value, dict):
        return {
            key: drop_masked_secrets(nested)
            for key, nested in value.items()
            if nested != _SECRETS_MASK
        }
    if isinstance(value, list):
        return [drop_masked_secrets(nested) for nested in value]
    return value


def spec_missing_required(schema: Any, config: dict[str, Any]) -> bool:  # noqa: ANN401
    """Return whether `config` lacks a required top-level field that has no default."""
    if not isinstance(schema, dict):
        return False
    properties = schema.get("properties") or {}
    return any(
        name not in config and "default" not in (properties.get(name) or {})
        for name in schema.get("required") or []
    )


def spec_has_secrets(schema: Any) -> bool:  # noqa: ANN401
    """Return whether a connection spec marks any field `airbyte_secret`."""
    if isinstance(schema, dict):
        return schema.get("airbyte_secret") is True or any(
            spec_has_secrets(nested) for nested in schema.values()
        )
    if isinstance(schema, list):
        return any(spec_has_secrets(nested) for nested in schema)
    return False
