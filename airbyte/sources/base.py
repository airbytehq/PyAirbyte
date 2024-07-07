# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import jsonschema
import pendulum
import yaml
from rich import print
from rich.syntax import Syntax
from typing_extensions import Literal

from airbyte_protocol.models import (
    AirbyteCatalog,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationSyncMode,
    Status,
    SyncMode,
    TraceType,
    Type,
)

from airbyte import exceptions as exc
from airbyte._future_cdk.catalog_providers import CatalogProvider
from airbyte._util.telemetry import (
    EventState,
    EventType,
    log_config_validation_result,
    log_source_check_result,
    send_telemetry,
)
from airbyte._util.temp_files import as_temp_files
from airbyte.caches.util import get_default_cache
from airbyte.datasets._lazy import LazyDataset
from airbyte.progress import progress
from airbyte.records import StreamRecord
from airbyte.results import ReadResult
from airbyte.strategies import WriteStrategy
from airbyte.warnings import PyAirbyteDataLossWarning


if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator

    from airbyte_protocol.models.airbyte_protocol import AirbyteStream

    from airbyte._executor import Executor
    from airbyte._future_cdk.state_providers import StateProviderBase
    from airbyte.caches import CacheBase
    from airbyte.documents import Document


class Source:  # noqa: PLR0904  # Ignore max publish methods
    """A class representing a source that can be called."""

    def __init__(
        self,
        executor: Executor,
        name: str,
        config: dict[str, Any] | None = None,
        streams: str | list[str] | None = None,
        *,
        validate: bool = False,
    ) -> None:
        """Initialize the source.

        If config is provided, it will be validated against the spec if validate is True.
        """
        self.executor = executor
        self.name = name
        self._processed_records = 0
        self._config_dict: dict[str, Any] | None = None
        self._last_log_messages: list[str] = []
        self._discovered_catalog: AirbyteCatalog | None = None
        self._spec: ConnectorSpecification | None = None
        self._selected_stream_names: list[str] = []
        if config is not None:
            self.set_config(config, validate=validate)
        if streams is not None:
            self.select_streams(streams)

        self._deployed_api_root: str | None = None
        self._deployed_workspace_id: str | None = None
        self._deployed_source_id: str | None = None

    def set_streams(self, streams: list[str]) -> None:
        """Deprecated. See select_streams()."""
        warnings.warn(
            "The 'set_streams' method is deprecated and will be removed in a future version. "
            "Please use the 'select_streams' method instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.select_streams(streams)

    def select_all_streams(self) -> None:
        """Select all streams.

        This is a more streamlined equivalent to:
        > source.select_streams(source.get_available_streams()).
        """
        self._selected_stream_names = self.get_available_streams()

    def select_streams(self, streams: str | list[str]) -> None:
        """Select the stream names that should be read from the connector.

        Args:
        - streams: A list of stream names to select. If set to "*", all streams will be selected.

        Currently, if this is not set, all streams will be read.
        """
        if streams == "*":
            self.select_all_streams()
            return

        if isinstance(streams, str):
            # If a single stream is provided, convert it to a one-item list
            streams = [streams]

        available_streams = self.get_available_streams()
        for stream in streams:
            if stream not in available_streams:
                raise exc.AirbyteStreamNotFoundError(
                    stream_name=stream,
                    connector_name=self.name,
                    available_streams=available_streams,
                )
        self._selected_stream_names = streams

    def get_selected_streams(self) -> list[str]:
        """Get the selected streams.

        If no streams are selected, return an empty list.
        """
        return self._selected_stream_names

    def set_config(
        self,
        config: dict[str, Any],
        *,
        validate: bool = True,
    ) -> None:
        """Set the config for the connector.

        If validate is True, raise an exception if the config fails validation.

        If validate is False, validation will be deferred until check() or validate_config()
        is called.
        """
        if validate:
            self.validate_config(config)

        self._config_dict = config

    def get_config(self) -> dict[str, Any]:
        """Get the config for the connector."""
        return self._config

    @property
    def _config(self) -> dict[str, Any]:
        if self._config_dict is None:
            raise exc.AirbyteConnectorConfigurationMissingError(
                guidance="Provide via get_source() or set_config()"
            )
        return self._config_dict

    def _discover(self) -> AirbyteCatalog:
        """Call discover on the connector.

        This involves the following steps:
        - Write the config to a temporary file
        - execute the connector with discover --config <config_file>
        - Listen to the messages and return the first AirbyteCatalog that comes along.
        - Make sure the subprocess is killed when the function returns.
        """
        with as_temp_files([self._config]) as [config_file]:
            for msg in self._execute(["discover", "--config", config_file]):
                if msg.type == Type.CATALOG and msg.catalog:
                    return msg.catalog
            raise exc.AirbyteConnectorMissingCatalogError(
                log_text=self._last_log_messages,
            )

    def validate_config(self, config: dict[str, Any] | None = None) -> None:
        """Validate the config against the spec.

        If config is not provided, the already-set config will be validated.
        """
        spec = self._get_spec(force_refresh=False)
        config = self._config if config is None else config
        try:
            jsonschema.validate(config, spec.connectionSpecification)
            log_config_validation_result(
                name=self.name,
                state=EventState.SUCCEEDED,
            )
        except jsonschema.ValidationError as ex:
            validation_ex = exc.AirbyteConnectorValidationFailedError(
                message="The provided config is not valid.",
                context={
                    "error_message": ex.message,
                    "error_path": ex.path,
                    "error_instance": ex.instance,
                    "error_schema": ex.schema,
                },
            )
            log_config_validation_result(
                name=self.name,
                state=EventState.FAILED,
                exception=validation_ex,
            )
            raise validation_ex from ex

    def get_available_streams(self) -> list[str]:
        """Get the available streams from the spec."""
        return [s.name for s in self.discovered_catalog.streams]

    def _get_spec(self, *, force_refresh: bool = False) -> ConnectorSpecification:
        """Call spec on the connector.

        This involves the following steps:
        * execute the connector with spec
        * Listen to the messages and return the first AirbyteCatalog that comes along.
        * Make sure the subprocess is killed when the function returns.
        """
        if force_refresh or self._spec is None:
            for msg in self._execute(["spec"]):
                if msg.type == Type.SPEC and msg.spec:
                    self._spec = msg.spec
                    break

        if self._spec:
            return self._spec

        raise exc.AirbyteConnectorMissingSpecError(
            log_text=self._last_log_messages,
        )

    @property
    def config_spec(self) -> dict[str, Any]:
        """Generate a configuration spec for this connector, as a JSON Schema definition.

        This function generates a JSON Schema dictionary with configuration specs for the
        current connector, as a dictionary.

        Returns:
            dict: The JSON Schema configuration spec as a dictionary.
        """
        return self._get_spec(force_refresh=True).connectionSpecification

    def print_config_spec(
        self,
        format: Literal["yaml", "json"] = "yaml",  # noqa: A002
        *,
        output_file: Path | str | None = None,
    ) -> None:
        """Print the configuration spec for this connector.

        Args:
        - format: The format to print the spec in. Must be "yaml" or "json".
        - output_file: Optional. If set, the spec will be written to the given file path. Otherwise,
          it will be printed to the console.
        """
        if format not in {"yaml", "json"}:
            raise exc.PyAirbyteInputError(
                message="Invalid format. Expected 'yaml' or 'json'",
                input_value=format,
            )
        if isinstance(output_file, str):
            output_file = Path(output_file)

        if format == "yaml":
            content = yaml.dump(self.config_spec, indent=2)
        elif format == "json":
            content = json.dumps(self.config_spec, indent=2)

        if output_file:
            output_file.write_text(content)
            return

        syntax_highlighted = Syntax(content, format)
        print(syntax_highlighted)

    @property
    def _yaml_spec(self) -> str:
        """Get the spec as a yaml string.

        For now, the primary use case is for writing and debugging a valid config for a source.

        This is private for now because we probably want better polish before exposing this
        as a stable interface. This will also get easier when we have docs links with this info
        for each connector.
        """
        spec_obj: ConnectorSpecification = self._get_spec()
        spec_dict: dict[str, Any] = spec_obj.model_dump(exclude_unset=True)
        # convert to a yaml string
        return yaml.dump(spec_dict)

    @property
    def docs_url(self) -> str:
        """Get the URL to the connector's documentation."""
        # TODO: Replace with docs URL from metadata when available
        return "https://docs.airbyte.com/integrations/sources/" + self.name.lower().replace(
            "source-", ""
        )

    @property
    def discovered_catalog(self) -> AirbyteCatalog:
        """Get the raw catalog for the given streams.

        If the catalog is not yet known, we call discover to get it.
        """
        if self._discovered_catalog is None:
            self._discovered_catalog = self._discover()

        return self._discovered_catalog

    @property
    def configured_catalog(self) -> ConfiguredAirbyteCatalog:
        """Get the configured catalog for the given streams.

        If the raw catalog is not yet known, we call discover to get it.

        If no specific streams are selected, we return a catalog that syncs all available streams.

        TODO: We should consider disabling by default the streams that the connector would
        disable by default. (For instance, streams that require a premium license are sometimes
        disabled by default within the connector.)
        """
        # Ensure discovered catalog is cached before we start
        _ = self.discovered_catalog

        # Filter for selected streams if set, otherwise use all available streams:
        streams_filter: list[str] = self._selected_stream_names or self.get_available_streams()

        return ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=stream,
                    destination_sync_mode=DestinationSyncMode.overwrite,
                    primary_key=stream.source_defined_primary_key,
                    # TODO: The below assumes all sources can coalesce from incremental sync to
                    # full_table as needed. CDK supports this, so it might be safe:
                    sync_mode=SyncMode.incremental,
                )
                for stream in self.discovered_catalog.streams
                if stream.name in streams_filter
            ],
        )

    def get_stream_json_schema(self, stream_name: str) -> dict[str, Any]:
        """Return the JSON Schema spec for the specified stream name."""
        catalog: AirbyteCatalog = self.discovered_catalog
        found: list[AirbyteStream] = [
            stream for stream in catalog.streams if stream.name == stream_name
        ]

        if len(found) == 0:
            raise exc.PyAirbyteInputError(
                message="Stream name does not exist in catalog.",
                input_value=stream_name,
            )

        if len(found) > 1:
            raise exc.PyAirbyteInternalError(
                message="Duplicate streams found with the same name.",
                context={
                    "found_streams": found,
                },
            )

        return found[0].json_schema

    def get_records(self, stream: str) -> LazyDataset:
        """Read a stream from the connector.

        This involves the following steps:
        * Call discover to get the catalog
        * Generate a configured catalog that syncs the given stream in full_refresh mode
        * Write the configured catalog and the config to a temporary file
        * execute the connector with read --config <config_file> --catalog <catalog_file>
        * Listen to the messages and return the first AirbyteRecordMessages that come along.
        * Make sure the subprocess is killed when the function returns.
        """
        discovered_catalog: AirbyteCatalog = self.discovered_catalog
        configured_catalog = ConfiguredAirbyteCatalog(
            streams=[
                ConfiguredAirbyteStream(
                    stream=s,
                    sync_mode=SyncMode.full_refresh,
                    destination_sync_mode=DestinationSyncMode.overwrite,
                )
                for s in discovered_catalog.streams
                if s.name == stream
            ],
        )
        if len(configured_catalog.streams) == 0:
            raise exc.PyAirbyteInputError(
                message="Requested stream does not exist.",
                context={
                    "stream": stream,
                    "available_streams": self.get_available_streams(),
                    "connector_name": self.name,
                },
            ) from KeyError(stream)

        configured_stream = configured_catalog.streams[0]
        all_properties = cast(
            list[str], list(configured_stream.stream.json_schema["properties"].keys())
        )

        def _with_logging(records: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
            self._log_sync_start(cache=None)
            yield from records
            self._log_sync_success(cache=None)

        iterator: Iterator[dict[str, Any]] = _with_logging(
            records=(  # Generator comprehension yields StreamRecord objects for each record
                StreamRecord.from_record_message(
                    record_message=record.record,
                    expected_keys=all_properties,
                    prune_extra_fields=True,
                )
                for record in self._read_with_catalog(configured_catalog)
                if record.record
            )
        )
        return LazyDataset(
            iterator,
            stream_metadata=configured_stream,
        )

    @property
    def connector_version(self) -> str | None:
        """Return the version of the connector as reported by the executor.

        Returns None if the version cannot be determined.
        """
        return self.executor.get_installed_version()

    def get_documents(
        self,
        stream: str,
        title_property: str | None = None,
        content_properties: list[str] | None = None,
        metadata_properties: list[str] | None = None,
        *,
        render_metadata: bool = False,
    ) -> Iterable[Document]:
        """Read a stream from the connector and return the records as documents.

        If metadata_properties is not set, all properties that are not content will be added to
        the metadata.

        If render_metadata is True, metadata will be rendered in the document, as well as the
        the main content.
        """
        return self.get_records(stream).to_documents(
            title_property=title_property,
            content_properties=content_properties,
            metadata_properties=metadata_properties,
            render_metadata=render_metadata,
        )

    def check(self) -> None:
        """Call check on the connector.

        This involves the following steps:
        * Write the config to a temporary file
        * execute the connector with check --config <config_file>
        * Listen to the messages and return the first AirbyteCatalog that comes along.
        * Make sure the subprocess is killed when the function returns.
        """
        with as_temp_files([self._config]) as [config_file]:
            try:
                for msg in self._execute(["check", "--config", config_file]):
                    if msg.type == Type.CONNECTION_STATUS and msg.connectionStatus:
                        if msg.connectionStatus.status != Status.FAILED:
                            print(f"Connection check succeeded for `{self.name}`.")
                            log_source_check_result(
                                name=self.name,
                                state=EventState.SUCCEEDED,
                            )
                            return

                        log_source_check_result(
                            name=self.name,
                            state=EventState.FAILED,
                        )
                        raise exc.AirbyteConnectorCheckFailedError(
                            help_url=self.docs_url,
                            context={
                                "failure_reason": msg.connectionStatus.message,
                            },
                        )
                raise exc.AirbyteConnectorCheckFailedError(log_text=self._last_log_messages)
            except exc.AirbyteConnectorReadError as ex:
                raise exc.AirbyteConnectorCheckFailedError(
                    message="The connector failed to check the connection.",
                    log_text=ex.log_text,
                ) from ex

    def install(self) -> None:
        """Install the connector if it is not yet installed."""
        self.executor.install()
        print("For configuration instructions, see: \n" f"{self.docs_url}#reference\n")

    def uninstall(self) -> None:
        """Uninstall the connector if it is installed.

        This only works if the use_local_install flag wasn't used and installation is managed by
        PyAirbyte.
        """
        self.executor.uninstall()

    def _read_with_catalog(
        self,
        catalog: ConfiguredAirbyteCatalog,
        state: StateProviderBase | None = None,
    ) -> Iterator[AirbyteMessage]:
        """Call read on the connector.

        This involves the following steps:
        * Write the config to a temporary file
        * execute the connector with read --config <config_file> --catalog <catalog_file>
        * Listen to the messages and return the AirbyteRecordMessages that come along.
        * Send out telemetry on the performed sync (with information about which source was used and
          the type of the cache)
        """
        self._processed_records = 0  # Reset the counter before we start
        with as_temp_files(
            [
                self._config,
                catalog.model_dump_json(),
                state.to_state_input_file_text() if state else "[]",
            ]
        ) as [
            config_file,
            catalog_file,
            state_file,
        ]:
            yield from self._tally_records(
                self._execute(
                    [
                        "read",
                        "--config",
                        config_file,
                        "--catalog",
                        catalog_file,
                        "--state",
                        state_file,
                    ],
                )
            )

    def _add_to_logs(self, message: str) -> None:
        self._last_log_messages.append(message)
        self._last_log_messages = self._last_log_messages[-10:]

    def _execute(self, args: list[str]) -> Iterator[AirbyteMessage]:
        """Execute the connector with the given arguments.

        This involves the following steps:
        * Locate the right venv. It is called ".venv-<connector_name>"
        * Spawn a subprocess with .venv-<connector_name>/bin/<connector-name> <args>
        * Read the output line by line of the subprocess and serialize them AirbyteMessage objects.
          Drop if not valid.
        """
        # Fail early if the connector is not installed.
        self.executor.ensure_installation(auto_fix=False)

        try:
            self._last_log_messages = []
            for line in self.executor.execute(args):
                try:
                    message: AirbyteMessage = AirbyteMessage.model_validate_json(json_data=line)
                    if message.type is Type.RECORD:
                        self._processed_records += 1
                    if message.type == Type.LOG:
                        self._add_to_logs(message.log.message)
                    if message.type == Type.TRACE and message.trace.type == TraceType.ERROR:
                        self._add_to_logs(message.trace.error.message)
                    yield message
                except Exception:
                    self._add_to_logs(line)
        except Exception as e:
            raise exc.AirbyteConnectorReadError(
                log_text=self._last_log_messages,
            ) from e

    def _tally_records(
        self,
        messages: Iterable[AirbyteMessage],
    ) -> Generator[AirbyteMessage, Any, None]:
        """This method simply tallies the number of records processed and yields the messages."""
        self._processed_records = 0  # Reset the counter before we start
        progress.reset(len(self._selected_stream_names or []))

        for message in messages:
            yield message
            progress.log_records_read(new_total_count=self._processed_records)

    def _log_sync_start(
        self,
        *,
        cache: CacheBase | None,
    ) -> None:
        """Log the start of a sync operation."""
        print(f"Started `{self.name}` read operation at {pendulum.now().format('HH:mm:ss')}...")
        send_telemetry(
            source=self,
            cache=cache,
            state=EventState.STARTED,
            event_type=EventType.SYNC,
        )

    def _log_sync_success(
        self,
        *,
        cache: CacheBase | None,
    ) -> None:
        """Log the success of a sync operation."""
        print(f"Completed `{self.name}` read operation at {pendulum.now().format('HH:mm:ss')}.")
        send_telemetry(
            source=self,
            cache=cache,
            state=EventState.SUCCEEDED,
            number_of_records=self._processed_records,
            event_type=EventType.SYNC,
        )

    def _log_sync_failure(
        self,
        *,
        cache: CacheBase | None,
        exception: Exception,
    ) -> None:
        """Log the failure of a sync operation."""
        print(f"Failed `{self.name}` read operation at {pendulum.now().format('HH:mm:ss')}.")
        send_telemetry(
            state=EventState.FAILED,
            source=self,
            cache=cache,
            number_of_records=self._processed_records,
            exception=exception,
            event_type=EventType.SYNC,
        )

    def read(
        self,
        cache: CacheBase | None = None,
        *,
        streams: str | list[str] | None = None,
        write_strategy: str | WriteStrategy = WriteStrategy.AUTO,
        force_full_refresh: bool = False,
        skip_validation: bool = False,
    ) -> ReadResult:
        """Read from the connector and write to the cache.

        Args:
            cache: The cache to write to. If None, a default cache will be used.
            streams: Optional if already set. A list of stream names to select for reading. If set
                to "*", all streams will be selected.
            write_strategy: The strategy to use when writing to the cache. If a string, it must be
                one of "append", "upsert", "replace", or "auto". If a WriteStrategy, it must be one
                of WriteStrategy.APPEND, WriteStrategy.UPSERT, WriteStrategy.REPLACE, or
                WriteStrategy.AUTO.
            force_full_refresh: If True, the source will operate in full refresh mode. Otherwise,
                streams will be read in incremental mode if supported by the connector. This option
                must be True when using the "replace" strategy.
            skip_validation: If True, PyAirbyte will not pre-validate the input configuration before
                running the connector. This can be helpful in debugging, when you want to send
                configurations to the connector that otherwise might be rejected by JSON Schema
                validation rules.
        """
        if write_strategy == WriteStrategy.REPLACE and not force_full_refresh:
            warnings.warn(
                message=(
                    "Using `REPLACE` strategy without also setting `full_refresh_mode=True` "
                    "could result in data loss. "
                    "To silence this warning, use the following: "
                    'warnings.filterwarnings("ignore", '
                    'category="airbyte.warnings.PyAirbyteDataLossWarning")`'
                ),
                category=PyAirbyteDataLossWarning,
                stacklevel=1,
            )
        if isinstance(write_strategy, str):
            try:
                write_strategy = WriteStrategy(write_strategy)
            except ValueError:
                raise exc.PyAirbyteInputError(
                    message="Invalid strategy",
                    context={
                        "write_strategy": write_strategy,
                        "available_strategies": [s.value for s in WriteStrategy],
                    },
                ) from None

        if streams:
            self.select_streams(streams)

        if not self._selected_stream_names:
            raise exc.PyAirbyteNoStreamsSelectedError(
                connector_name=self.name,
                available_streams=self.get_available_streams(),
            )

        # Run optional validation step
        if not skip_validation:
            self.validate_config()

        # Set up cache and related resources
        if cache is None:
            cache = get_default_cache()

        # Set up state provider if not in full refresh mode
        if force_full_refresh:
            state_provider: StateProviderBase | None = None
        else:
            state_provider = cache.get_state_provider(
                source_name=self.name,
            )

        self._log_sync_start(cache=cache)

        cache_processor = cache.get_record_processor(
            source_name=self.name,
            catalog_provider=CatalogProvider(self.configured_catalog),
        )
        try:
            cache_processor.process_airbyte_messages(
                self._read_with_catalog(
                    catalog=self.configured_catalog,
                    state=state_provider,
                ),
                write_strategy=write_strategy,
            )

        # TODO: We should catch more specific exceptions here
        except Exception as ex:
            self._log_sync_failure(cache=cache, exception=ex)
            raise exc.AirbyteConnectorFailedError(
                log_text=self._last_log_messages,
            ) from ex

        self._log_sync_success(cache=cache)
        return ReadResult(
            processed_records=self._processed_records,
            cache=cache,
            processed_streams=[stream.stream.name for stream in self.configured_catalog.streams],
        )


__all__ = [
    "Source",
]
