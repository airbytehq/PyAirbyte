# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""The `CloudConnectorBuilderProject` class."""

from __future__ import annotations

import difflib
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import TYPE_CHECKING, Any, Literal, overload

import requests

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte._util.api_util import get_web_url_root
from airbyte.cloud.connector_builder._models import (
    ConnectorBuilderFullSync,
    ConnectorBuilderProjectInfo,
    ConnectorBuilderReadinessReport,
    ConnectorBuilderResolvedManifest,
    ConnectorBuilderStreamReadiness,
    ConnectorBuilderStreamReadResult,
    ManifestTextEditMode,
)
from airbyte.cloud.connector_builder._util import (
    CONNECTOR_BUILDER_TEST_DESTINATION_NAME,
    TEST_READ_MAX_PAGES,
    TEST_READ_MAX_RECORDS,
    TEST_READ_MAX_SLICES,
    apply_manifest_text_edit,
    builder_testing_values_spec,
    draft_from_project_data,
    drop_masked_secrets,
    parse_manifest_yaml,
    publish_spec,
    readiness_checks,
    readiness_next_steps,
    render_manifest_text,
    spec_has_secrets,
    spec_missing_required,
)


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from airbyte.cloud.connectors import CloudSource, CustomCloudSourceDefinition
    from airbyte.cloud.workspaces import CloudWorkspace


_READINESS_MAX_WORKERS = 5
"""Most streams `_run_readiness_checks()` reads at once."""
_MIN_READ_TIMEOUT = 1.0
"""Shortest HTTP timeout for a test read started just before the readiness deadline."""


def _check_test_read_limits(
    record_limit: int,
    page_limit: int,
    slice_limit: int,
) -> None:
    """Reject test-read limits outside Airbyte Cloud's supported range."""
    limits = (
        ("record_limit", record_limit, TEST_READ_MAX_RECORDS),
        ("page_limit", page_limit, TEST_READ_MAX_PAGES),
        ("slice_limit", slice_limit, TEST_READ_MAX_SLICES),
    )
    if any(not 1 <= value <= maximum for _, value, maximum in limits):
        raise exc.AirbyteLibInputError(
            message="Test-read limits are out of range.",
            guidance=(
                f"Airbyte Cloud accepts record_limit 1 to {TEST_READ_MAX_RECORDS:,}, "
                f"page_limit 1 to {TEST_READ_MAX_PAGES}, and "
                f"slice_limit 1 to {TEST_READ_MAX_SLICES}."
            ),
            context={name: value for name, value, _ in limits},
        )


class CloudConnectorBuilderProject:
    """A Connector Builder project in an Airbyte Cloud workspace."""

    def __init__(
        self,
        *,
        workspace: CloudWorkspace,
        builder_project_id: str,
        owner_workspace_id: str | None = None,
    ) -> None:
        """Initialize a Connector Builder project object. Does not call the API.

        Args:
            workspace: The workspace whose credentials are used.
            builder_project_id: The Connector Builder project ID.
            owner_workspace_id: The workspace that owns the project, if it differs from
                `workspace` (for example, a definition shared from another workspace).
        """
        self.workspace = workspace
        self.builder_project_id = builder_project_id
        self.owner_workspace_id = owner_workspace_id or workspace.workspace_id

    @property
    def connector_builder_project_url(self) -> str:
        """The project's Connector Builder URL, where users enter testing values and secrets."""
        return (
            f"{get_web_url_root(self.workspace.api_root)}/workspaces/"
            f"{self.owner_workspace_id}/connector-builder/edit/{self.builder_project_id}"
        )

    def _get_project_data(self) -> dict[str, Any]:
        """Fetch the project metadata and draft manifest from the API."""
        return api_util.get_connector_builder_project(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )

    @property
    def name(self) -> str:
        """The project name."""
        return self._get_project_data()["builderProject"]["name"]

    @property
    def source_definition_id(self) -> str | None:
        """The ID of the custom source definition, or `None` if never published."""
        return self._get_project_data()["builderProject"].get("sourceDefinitionId")

    def _published_manifest(self, source_definition_id: str | None) -> dict[str, Any] | None:
        if source_definition_id is None:
            return None
        return self.workspace.get_custom_source_definition(
            source_definition_id,
            definition_type="yaml",
        ).manifest

    @property
    def published_manifest_dict(self) -> dict[str, Any] | None:
        """The active manifest of the published definition, or `None` if never published."""
        return self._published_manifest(self.source_definition_id)

    def _get_info(self) -> ConnectorBuilderProjectInfo:
        """Read the project once and return its IDs, URL, draft, and published manifest."""
        project_data = self._get_project_data()
        draft_manifest = draft_from_project_data(project_data)
        source_definition_id = project_data["builderProject"].get("sourceDefinitionId")
        return ConnectorBuilderProjectInfo(
            builder_project_id=self.builder_project_id,
            definition_id=source_definition_id,
            connector_builder_project_url=self.connector_builder_project_url,
            name=project_data["builderProject"]["name"],
            has_draft=draft_manifest is not None,
            draft_manifest_dict=draft_manifest,
            draft_manifest_yaml=(
                render_manifest_text(draft_manifest) if draft_manifest is not None else None
            ),
            published_manifest_dict=self._published_manifest(source_definition_id),
        )

    @property
    def draft_manifest_dict(self) -> dict[str, Any] | None:
        """The unpublished draft manifest, or `None` if the project has no draft."""
        return draft_from_project_data(self._get_project_data())

    def _require_draft_manifest(self, project_data: dict[str, Any] | None = None) -> dict[str, Any]:
        manifest = draft_from_project_data(project_data or self._get_project_data())
        if manifest is None:
            raise exc.AirbyteLibInputError(
                message="This Connector Builder project has no draft manifest.",
                guidance="Save a draft manifest to the project first.",
                context={"builder_project_id": self.builder_project_id},
            )
        return manifest

    def replace_draft_manifest(
        self,
        manifest_yaml: dict[str, Any] | Path | str,
        *,
        pre_validate: bool = True,
    ) -> None:
        """Replace the project's draft manifest. Does not publish anything.

        Args:
            manifest_yaml: The manifest, as a dict, a YAML file path, or YAML text.
            pre_validate: Whether to validate the manifest client-side before saving.
        """
        manifest = parse_manifest_yaml(manifest_yaml)
        if pre_validate:
            api_util.validate_yaml_manifest(manifest, raise_on_error=True)

        api_util.update_connector_builder_project(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            name=self.name,
            draft_manifest=manifest,
            components_file_content=None,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )

    @property
    def draft_manifest_yaml(self) -> str | None:
        """The draft manifest rendered by `render_manifest_text()`, or `None` if no draft."""
        manifest = self.draft_manifest_dict
        return render_manifest_text(manifest) if manifest is not None else None

    def _update_draft_manifest_yaml(
        self,
        *,
        edit_mode: ManifestTextEditMode,
        new_text: str,
        old_text: str | None = None,
        pre_validate: bool = True,
    ) -> str:
        """Edit the draft manifest as text and save it. Does not publish anything.

        Edits match against `draft_manifest_yaml`. Returns a unified diff from the previous
        rendering to the rendering of the saved draft.

        Args:
            edit_mode: See `ManifestTextEditMode`.
            new_text: The whole new manifest for `replace_full`, else the replacement text.
            old_text: The exact text to replace. Required unless `edit_mode` is `replace_full`.
            pre_validate: Whether to validate the edited manifest client-side before saving.
        """
        current_text = self.draft_manifest_yaml
        if current_text is None and edit_mode != "replace_full":
            raise exc.AirbyteLibInputError(
                message="This Connector Builder project has no draft manifest to edit.",
                guidance="Use `replace_full` to save a whole manifest first.",
                context={"builder_project_id": self.builder_project_id},
            )
        edited_text = apply_manifest_text_edit(
            current_text or "",
            edit_mode=edit_mode,
            new_text=new_text,
            old_text=old_text,
        )
        self.replace_draft_manifest(edited_text, pre_validate=pre_validate)

        saved_text = self.draft_manifest_yaml or ""
        return "".join(
            difflib.unified_diff(
                (current_text or "").splitlines(keepends=True),
                saved_text.splitlines(keepends=True),
                fromfile="previous",
                tofile="saved",
            )
        )

    def replace_testing_values(
        self,
        testing_values: dict[str, Any],
    ) -> None:
        """Replace the project's testing values, which Cloud stores in its secret storage.

        The spec comes from the draft manifest, or the published one when there's no draft.
        Fields marked `airbyte_secret` in the spec are stored as secrets.
        """
        project_data = self._get_project_data()
        manifest = draft_from_project_data(project_data) or self._published_manifest(
            project_data["builderProject"].get("sourceDefinitionId")
        )
        if manifest is None:
            manifest = self._require_draft_manifest(project_data)
        api_util.update_connector_builder_project_testing_values(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            testing_values=testing_values,
            spec=builder_testing_values_spec(manifest),
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )

    def run_stream_test_read(
        self,
        stream_name: str,
        *,
        record_limit: int = 100,
        page_limit: int = 5,
        slice_limit: int = 5,
    ) -> ConnectorBuilderStreamReadResult:
        """Test-read one stream of the draft manifest on Airbyte Cloud.

        Uses the project's saved testing values as the connector config.

        Args:
            stream_name: The stream to read.
            record_limit: Maximum records to return (1 to 5,000).
            page_limit: Maximum pages to fetch per slice (1 to 20).
            slice_limit: Maximum slices to read (1 to 20).
        """
        _check_test_read_limits(record_limit, page_limit, slice_limit)
        return self._run_stream_test_read(
            self._require_draft_manifest(),
            stream_name,
            record_limit=record_limit,
            page_limit=page_limit,
            slice_limit=slice_limit,
        )

    def _run_stream_test_read(
        self,
        manifest: dict[str, Any],
        stream_name: str,
        *,
        record_limit: int,
        page_limit: int,
        slice_limit: int,
        timeout_seconds: float | None = None,
    ) -> ConnectorBuilderStreamReadResult:
        response = api_util.read_connector_builder_project_stream(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            manifest=manifest,
            stream_name=stream_name,
            record_limit=record_limit,
            page_limit=page_limit,
            slice_limit=slice_limit,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
            timeout_seconds=timeout_seconds,
        )
        return ConnectorBuilderStreamReadResult._from_api_response(stream_name, response)  # noqa: SLF001

    def resolve_manifest(
        self,
        *,
        stream_limit: int = 5,
    ) -> ConnectorBuilderResolvedManifest:
        """Resolve the draft manifest on Airbyte Cloud, including dynamic streams.

        Uses the project's saved testing values to generate dynamic streams.

        Args:
            stream_limit: Maximum generated streams to return per dynamic stream (1 to 100).
        """
        resolved_manifest = api_util.full_resolve_connector_builder_project_manifest(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            manifest=self._require_draft_manifest(),
            stream_limit=stream_limit,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        return ConnectorBuilderResolvedManifest._from_manifest(resolved_manifest)  # noqa: SLF001

    def _run_readiness_checks(
        self,
        stream_names: list[str],
        *,
        record_limit: int = TEST_READ_MAX_RECORDS,
        page_limit: int = TEST_READ_MAX_PAGES,
        slice_limit: int = TEST_READ_MAX_SLICES,
        timeout_seconds: float | None = None,
        run_full_sync: bool = False,
        test_source_id: str | None = None,
        on_resource_created: Callable[[str], None] | None = None,
    ) -> ConnectorBuilderReadinessReport:
        """Check the draft's progress and test-read the given streams on Airbyte Cloud.

        Reads run in parallel. A stream is `failed` when its read errored, returned HTTP
        errors, or logged errors. The report lists failures and warnings without judging
        readiness; `read_to_end` indicates whether the read ended within the test-read caps.
        The readiness checks are derived from the project, so nothing is stored. A full sync
        after publishing validates the whole connector.

        Args:
            stream_names: Streams to test-read (at least 1). For
                dynamic streams, get the generated names from `resolve_manifest()`.
            record_limit: Maximum records to read per stream (1 to 5,000).
            page_limit: Maximum pages to fetch per slice (1 to 20).
            slice_limit: Maximum slices to read (1 to 20).
            timeout_seconds: Return after this many seconds with the streams finished so far;
                the rest are listed in `pending_streams` and `not_attempted_streams`. No limit
                by default.
            run_full_sync: If no named stream failed or is still pending (or none are named),
                publish the draft and start a full sync with `_run_full_sync()`.
            test_source_id: The source for the full sync. See `_run_full_sync()`.
            on_resource_created: Passed to `_run_full_sync()`.
        """
        _check_test_read_limits(record_limit, page_limit, slice_limit)
        stream_names = list(dict.fromkeys(stream_names))
        if not stream_names and not run_full_sync:
            raise exc.AirbyteLibInputError(
                message="Pass at least 1 stream name, or set `run_full_sync`.",
            )
        project_data = self._get_project_data()
        if not stream_names:
            # A full sync alone can run on the published manifest; the first publish clears
            # the draft.
            manifest = draft_from_project_data(project_data) or self._published_manifest(
                project_data["builderProject"].get("sourceDefinitionId")
            )
            if manifest is None:
                manifest = self._require_draft_manifest(project_data)
            return self._readiness_report(project_data, manifest, [], [], []).model_copy(
                update={
                    "full_sync": self._run_full_sync(
                        test_source_id=test_source_id,
                        on_resource_created=on_resource_created,
                    ),
                },
            )

        draft = self._require_draft_manifest(project_data)
        deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds

        def read(stream_name: str) -> ConnectorBuilderStreamReadiness:
            # Bound each HTTP request by the deadline so worker threads end with the call.
            request_timeout = (
                None if deadline is None else max(deadline - time.monotonic(), _MIN_READ_TIMEOUT)
            )
            try:
                result = self._run_stream_test_read(
                    draft,
                    stream_name,
                    record_limit=record_limit,
                    page_limit=page_limit,
                    slice_limit=slice_limit,
                    timeout_seconds=request_timeout,
                )
            except exc.AirbyteLibError as ex:
                error = ex.get_message()
                if ex.guidance:
                    error += f" {ex.guidance}"
                return ConnectorBuilderStreamReadiness(
                    stream_name=stream_name, failed=True, error=error
                )
            except requests.RequestException as ex:
                return ConnectorBuilderStreamReadiness(
                    stream_name=stream_name,
                    failed=True,
                    error=f"Request to Airbyte Cloud failed: {type(ex).__name__}",
                )
            return ConnectorBuilderStreamReadiness._from_stream_read(result)  # noqa: SLF001

        pool = ThreadPoolExecutor(max_workers=min(_READINESS_MAX_WORKERS, len(stream_names)))
        try:
            futures = [pool.submit(read, name) for name in stream_names]
            done, _ = wait(futures, timeout=timeout_seconds)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
        streams = [future.result() for future in futures if future in done]
        unfinished = [
            (name, future)
            for name, future in zip(stream_names, futures, strict=True)
            if future not in done
        ]
        pending_streams = [name for name, future in unfinished if not future.cancelled()]
        not_attempted_streams = [name for name, future in unfinished if future.cancelled()]
        report = self._readiness_report(
            project_data, draft, streams, pending_streams, not_attempted_streams
        )
        if run_full_sync and not (
            report.failed_streams or report.pending_streams or report.not_attempted_streams
        ):
            report.full_sync = self._run_full_sync(
                test_source_id=test_source_id,
                on_resource_created=on_resource_created,
            )
        return report

    def _readiness_report(
        self,
        project_data: dict[str, Any],
        draft: dict[str, Any],
        streams: list[ConnectorBuilderStreamReadiness],
        pending_streams: list[str],
        not_attempted_streams: list[str],
    ) -> ConnectorBuilderReadinessReport:
        remaining = pending_streams + not_attempted_streams
        checks = readiness_checks(
            project_data, draft, streams, remaining, self.connector_builder_project_url
        )
        return ConnectorBuilderReadinessReport(
            failed_streams=[stream.stream_name for stream in streams if stream.failed],
            pending_streams=pending_streams,
            not_attempted_streams=not_attempted_streams,
            checks=checks,
            next_steps=readiness_next_steps(
                checks, streams, pending_streams, not_attempted_streams
            ),
            streams=streams,
        )

    def publish(
        self,
        name: str,
        *,
        pre_validate: bool = True,
    ) -> CustomCloudSourceDefinition:
        """Publish the draft manifest as a custom source definition.

        The first publish creates the definition with `name`. If the project is already
        published, the draft becomes a new active version of the existing definition.
        """
        manifest = self._require_draft_manifest()
        source_definition_id = self.source_definition_id
        if source_definition_id is not None:
            definition = self.workspace.get_custom_source_definition(
                source_definition_id,
                definition_type="yaml",
            )
            return definition.update_definition(manifest_yaml=manifest, pre_validate=pre_validate)

        if pre_validate:
            api_util.validate_yaml_manifest(manifest, raise_on_error=True)

        source_definition_id = api_util.publish_connector_builder_project(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            name=name,
            manifest=manifest,
            spec=publish_spec(manifest),
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        return self.workspace.get_custom_source_definition(
            source_definition_id,
            definition_type="yaml",
        )

    def _get_default_test_source_name(self) -> str:
        """Default name of the source `_get_or_create_default_test_source()` finds or creates."""
        return f"{self.name} (Connector Builder test)"

    @overload
    def _get_or_create_default_test_source(
        self,
        *,
        allow_create: Literal[True] = True,
        default_source_name: str | Callable[[CloudConnectorBuilderProject], str] | None = None,
    ) -> CloudSource: ...

    @overload
    def _get_or_create_default_test_source(
        self,
        *,
        allow_create: bool,
        default_source_name: str | Callable[[CloudConnectorBuilderProject], str] | None = None,
    ) -> CloudSource | None: ...

    def _get_or_create_default_test_source(
        self,
        *,
        allow_create: bool = True,
        default_source_name: str | Callable[[CloudConnectorBuilderProject], str] | None = None,
    ) -> CloudSource | None:
        """Return the project's test source on the published definition, creating it if needed.

        A new source gets the project's non-secret testing values. When the spec has secret
        fields, the source is a draft (deferred credentials): a person enters the secrets at
        its `connector_url`, then tests and saves. Publish the project first.

        Args:
            allow_create: Whether to create the source when it doesn't exist. When False,
                returns `None` instead.
            default_source_name: The source name, or a callable that takes this project and
                returns it. Defaults to "<project name> (Connector Builder test)".
        """
        source_name = (
            default_source_name(self) if callable(default_source_name) else default_source_name
        ) or self._get_default_test_source_name()
        project_data = self._get_project_data()
        source_definition_id = project_data["builderProject"].get("sourceDefinitionId")
        if source_definition_id is None:
            if not allow_create:
                return None
            raise exc.AirbyteLibInputError(
                message="Publish the Connector Builder project before creating a test source.",
            )
        existing = next(
            (
                source
                for source in self.workspace.list_sources(name=source_name)
                if source.definition_id == source_definition_id
            ),
            None,
        )
        if existing is not None or not allow_create:
            return existing
        # The source runs the published version, so its spec decides the setup.
        manifest = self._published_manifest(source_definition_id)
        spec = ((manifest or {}).get("spec") or {}).get("connection_specification")
        config = drop_masked_secrets(project_data.get("testingValues") or {})
        return self.workspace.deploy_source(
            name=source_name,
            source=config,
            definition_id=source_definition_id,
            defer_credentials=spec_has_secrets(spec) or spec_missing_required(spec, config),
        )

    def _run_full_sync(
        self,
        *,
        test_source_id: str | None = None,
        pre_validate: bool = True,
        on_resource_created: Callable[[str], None] | None = None,
    ) -> ConnectorBuilderFullSync:
        """Publish the draft and start a full sync of every stream to a no-op destination.

        Without a draft, syncs the published version. Selects every stream the test source
        discovers, so dynamic streams follow the source's own credentials. Returns without
        waiting for the sync to finish.

        Args:
            test_source_id: Source on the published definition to sync from. Defaults to the
                source from `_get_or_create_default_test_source()`.
            pre_validate: Whether to validate the manifest client-side before publishing.
            on_resource_created: Called with the ID of each destination or connection this
                call creates, as soon as it exists.
        """
        project_data = self._get_project_data()
        has_draft = draft_from_project_data(project_data) is not None
        source_definition_id = project_data["builderProject"].get("sourceDefinitionId")
        if not has_draft and source_definition_id is None:
            self._require_draft_manifest(project_data)
        # Check the source and destination first: the first publish clears the draft.
        if test_source_id is None:
            test_source = self._get_or_create_default_test_source(allow_create=False)
            if test_source is None:
                raise exc.AirbyteLibInputError(
                    message="No test source found for this Connector Builder project.",
                    guidance=(
                        "Pass `test_source_id`, or create a source on this project's published "
                        "definition and have the user complete its settings."
                    ),
                )
            test_source_id = test_source.connector_id
        elif self.workspace.get_source(test_source_id).definition_id != source_definition_id:
            raise exc.AirbyteLibInputError(
                message="`test_source_id` isn't a source of this project's published definition.",
                context={"test_source_id": test_source_id, "definition_id": source_definition_id},
            )
        test_destination_id, destination_created = self._get_or_create_test_destination()
        if destination_created and on_resource_created is not None:
            on_resource_created(test_destination_id)
        if has_draft:
            self.publish(self.name, pre_validate=pre_validate)

        stream_names = api_util.get_source_stream_names(
            test_source_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        if not stream_names:
            raise exc.AirbyteLibInputError(
                message="The test source discovered no streams.",
                context={"test_source_id": test_source_id},
            )
        connection_name = f"{self.name} (Connector Builder full sync)"
        connection = next(
            (
                connection
                for connection in self.workspace.list_connections(name=connection_name)
                if connection.source_id == test_source_id
                and connection.destination_id == test_destination_id
            ),
            None,
        )
        connection_created = connection is None
        if connection is None:
            connection = self.workspace.deploy_connection(
                connection_name,
                source=test_source_id,
                destination=test_destination_id,
                selected_streams=stream_names,
            )
            if on_resource_created is not None:
                on_resource_created(connection.connection_id)
        else:
            connection.set_selected_streams(stream_names)
        sync_result = connection.run_sync(wait=False)
        return ConnectorBuilderFullSync(
            source_id=test_source_id,
            destination_id=test_destination_id,
            destination_created=destination_created,
            connection_id=connection.connection_id,
            connection_created=connection_created,
            job_id=sync_result.job_id,
            job_url=sync_result.job_url,
        )

    def _get_or_create_test_destination(self) -> tuple[str, bool]:
        """Return the test destination's ID and whether this call created it."""
        existing = self.workspace.list_destinations(name=CONNECTOR_BUILDER_TEST_DESTINATION_NAME)
        noop = [
            destination
            for destination in existing
            if destination.definition_id == api_util.DEV_NULL_DESTINATION_DEFINITION_ID
        ]
        if noop:
            return noop[0].connector_id, False
        if existing:
            raise exc.AirbyteLibInputError(
                message=(
                    f"Destination `{CONNECTOR_BUILDER_TEST_DESTINATION_NAME}` exists "
                    "but isn't a no-op (dev-null) destination."
                ),
                guidance="Rename or delete that destination.",
                context={"destination_ids": [destination.connector_id for destination in existing]},
            )
        from airbyte.destinations.util import get_noop_destination  # noqa: PLC0415

        return self.workspace.deploy_destination(
            name=CONNECTOR_BUILDER_TEST_DESTINATION_NAME,
            destination=get_noop_destination(),
        ).connector_id, True

    def __repr__(self) -> str:
        """Return a string representation of the project."""
        return f"CloudConnectorBuilderProject(builder_project_id={self.builder_project_id!r})"
