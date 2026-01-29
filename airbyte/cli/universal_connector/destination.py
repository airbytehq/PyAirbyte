# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Universal destination implementation using PyAirbyte caches."""

from __future__ import annotations

import datetime
import json
import logging
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    DestinationSyncMode,
    Status,
    Type,
)

from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from sqlalchemy.engine import Engine

    from airbyte.caches.base import CacheBase


logger = logging.getLogger("airbyte")


DESTINATION_TYPE_DUCKDB = "duckdb"
DESTINATION_TYPE_POSTGRES = "postgres"
DESTINATION_TYPE_SNOWFLAKE = "snowflake"
DESTINATION_TYPE_BIGQUERY = "bigquery"
DESTINATION_TYPE_MOTHERDUCK = "motherduck"


class DestinationPyAirbyteUniversal(Destination):
    """Universal destination that writes to any PyAirbyte-supported cache backend."""

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:  # noqa: ARG002
        """Return the connector specification."""
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/destinations/pyairbyte-universal",
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "PyAirbyte Universal Destination Spec",
                "type": "object",
                "required": ["destination_type"],
                "properties": {
                    "destination_type": {
                        "type": "string",
                        "title": "Destination Type",
                        "description": "The type of destination to write to.",
                        "enum": ["duckdb", "postgres", "snowflake", "bigquery", "motherduck"],
                    },
                    "duckdb": {
                        "type": "object",
                        "title": "DuckDB Configuration",
                        "properties": {
                            "db_path": {"type": "string", "default": "/local/pyairbyte.duckdb"},
                            "schema_name": {"type": "string", "default": "main"},
                        },
                    },
                    "postgres": {
                        "type": "object",
                        "title": "PostgreSQL Configuration",
                        "properties": {
                            "host": {"type": "string", "default": "localhost"},
                            "port": {"type": "integer", "default": 5432},
                            "username": {"type": "string"},
                            "password": {"type": "string", "airbyte_secret": True},
                            "database": {"type": "string"},
                            "schema_name": {"type": "string", "default": "public"},
                        },
                    },
                    "snowflake": {
                        "type": "object",
                        "title": "Snowflake Configuration",
                        "properties": {
                            "account": {"type": "string"},
                            "username": {"type": "string"},
                            "password": {"type": "string", "airbyte_secret": True},
                            "warehouse": {"type": "string"},
                            "database": {"type": "string"},
                            "schema_name": {"type": "string", "default": "PUBLIC"},
                            "role": {"type": "string"},
                        },
                    },
                    "bigquery": {
                        "type": "object",
                        "title": "BigQuery Configuration",
                        "properties": {
                            "project_name": {"type": "string"},
                            "dataset_name": {"type": "string"},
                            "credentials_path": {"type": "string"},
                        },
                    },
                    "motherduck": {
                        "type": "object",
                        "title": "MotherDuck Configuration",
                        "properties": {
                            "database": {"type": "string", "default": "my_db"},
                            "schema_name": {"type": "string", "default": "main"},
                            "api_key": {"type": "string", "airbyte_secret": True},
                        },
                    },
                },
            },
        )

    def _get_cache(self, config: Mapping[str, Any]) -> CacheBase:
        """Create and return the appropriate cache based on configuration."""
        destination_type = config.get("destination_type")

        if destination_type == DESTINATION_TYPE_DUCKDB:
            duckdb_config = config.get("duckdb", {})
            return DuckDBCache(
                db_path=duckdb_config.get("db_path", "/local/pyairbyte.duckdb"),
                schema_name=duckdb_config.get("schema_name", "main"),
            )

        if destination_type == DESTINATION_TYPE_POSTGRES:
            pg_config = config.get("postgres", {})
            return PostgresCache(
                host=pg_config.get("host", "localhost"),
                port=pg_config.get("port", 5432),
                username=pg_config.get("username", "postgres"),
                password=SecretString(pg_config.get("password", "")),
                database=pg_config.get("database", "postgres"),
                schema_name=pg_config.get("schema_name", "public"),
            )

        if destination_type == DESTINATION_TYPE_SNOWFLAKE:
            sf_config = config.get("snowflake", {})
            password = sf_config.get("password")
            return SnowflakeCache(
                account=sf_config.get("account", ""),
                username=sf_config.get("username", ""),
                password=SecretString(password) if password else None,
                warehouse=sf_config.get("warehouse", ""),
                database=sf_config.get("database", ""),
                schema_name=sf_config.get("schema_name", "PUBLIC"),
                role=sf_config.get("role", ""),
            )

        if destination_type == DESTINATION_TYPE_BIGQUERY:
            bq_config = config.get("bigquery", {})
            return BigQueryCache(
                project_name=bq_config.get("project_name", ""),
                dataset_name=bq_config.get("dataset_name", ""),
                credentials_path=bq_config.get("credentials_path"),
            )

        if destination_type == DESTINATION_TYPE_MOTHERDUCK:
            md_config = config.get("motherduck", {})
            return MotherDuckCache(
                database=md_config.get("database", "my_db"),
                schema_name=md_config.get("schema_name", "main"),
                api_key=SecretString(md_config.get("api_key", "")),
            )

        raise ValueError(f"Unsupported destination type: {destination_type}")

    def check(
        self,
        logger: logging.Logger,  # noqa: ARG002
        config: Mapping[str, Any],
    ) -> AirbyteConnectionStatus:
        """Test the connection to the destination."""
        try:
            cache = self._get_cache(config)
            engine = cache.get_sql_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED, message=f"Connection failed: {e!r}"
            )

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """Write data to the destination using PyAirbyte cache.

        This method processes messages in a streaming fashion, buffering records
        and flushing on state messages to ensure fault tolerance.
        """
        cache = self._get_cache(config)
        streams = {s.stream.name for s in configured_catalog.streams}
        schema_name = cache.schema_name

        logger.info(f"Starting write to PyAirbyte Universal with {len(streams)} streams")

        # Get SQL engine and ensure schema exists
        engine = cache.get_sql_engine()
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()  # pyrefly: ignore[missing-attribute]

        # Create tables for each stream
        for configured_stream in configured_catalog.streams:
            name = configured_stream.stream.name
            table_name = f"_airbyte_raw_{name}"

            with engine.connect() as conn:
                if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                    logger.info(f"Dropping table for overwrite: {table_name}")
                    conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name}"))
                    conn.commit()  # pyrefly: ignore[missing-attribute]

                # Create the raw table if needed
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    _airbyte_ab_id VARCHAR(36) PRIMARY KEY,
                    _airbyte_emitted_at TIMESTAMP,
                    _airbyte_data JSON
                )
                """
                conn.execute(text(create_sql))
                conn.commit()  # pyrefly: ignore[missing-attribute]

        # Buffer for records
        buffer: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))

        for message in input_messages:
            if message.type == Type.STATE:
                # Flush the buffer before yielding state
                for stream_name in list(buffer.keys()):
                    self._flush_buffer(
                        engine=engine,
                        buffer=buffer,
                        schema_name=schema_name,
                        stream_name=stream_name,
                    )
                buffer = defaultdict(lambda: defaultdict(list))
                yield message

            elif message.type == Type.RECORD:
                record = message.record
                if record is None:
                    continue
                stream_name = record.stream
                if stream_name not in streams:
                    logger.debug(f"Stream {stream_name} not in configured streams, skipping")
                    continue

                # Add to buffer
                buffer[stream_name]["_airbyte_ab_id"].append(str(uuid.uuid4()))
                buffer[stream_name]["_airbyte_emitted_at"].append(
                    datetime.datetime.now(datetime.timezone.utc).isoformat()
                )
                buffer[stream_name]["_airbyte_data"].append(json.dumps(record.data))

            else:
                logger.debug(f"Message type {message.type} not handled, skipping")

        # Flush any remaining records
        for stream_name in list(buffer.keys()):
            self._flush_buffer(
                engine=engine,
                buffer=buffer,
                schema_name=schema_name,
                stream_name=stream_name,
            )

        # Close the cache
        cache.close()

    def _flush_buffer(
        self,
        *,
        engine: Engine,
        buffer: dict[str, dict[str, list[Any]]],
        schema_name: str,
        stream_name: str,
    ) -> None:
        """Flush buffered records to the database."""
        if not buffer[stream_name]["_airbyte_ab_id"]:
            return

        table_name = f"_airbyte_raw_{stream_name}"
        entries = buffer[stream_name]

        logger.info(f"Flushing {len(entries['_airbyte_ab_id'])} records to {table_name}")

        with engine.connect() as conn:
            for i in range(len(entries["_airbyte_ab_id"])):
                insert_sql = text(f"""
                    INSERT INTO {schema_name}.{table_name}
                    (_airbyte_ab_id, _airbyte_emitted_at, _airbyte_data)
                    VALUES (:ab_id, :emitted_at, :data)
                """)
                conn.execute(
                    insert_sql,
                    {
                        "ab_id": entries["_airbyte_ab_id"][i],
                        "emitted_at": entries["_airbyte_emitted_at"][i],
                        "data": entries["_airbyte_data"][i],
                    },
                )
            conn.commit()  # pyrefly: ignore[missing-attribute]
