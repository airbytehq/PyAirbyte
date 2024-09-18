# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""A BigQuery implementation of the SQL Processor."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import TYPE_CHECKING, final

import google.oauth2
import sqlalchemy
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
from overrides import overrides
from pydantic import Field
from sqlalchemy.engine import make_url

from airbyte import exceptions as exc
from airbyte._writers.jsonl import JsonlWriter
from airbyte.constants import DEFAULT_CACHE_SCHEMA_NAME
from airbyte.secrets.base import SecretString
from airbyte.shared import SqlProcessorBase
from airbyte.shared.sql_processor import SqlConfig
from airbyte.types import SQLTypeConverter


if TYPE_CHECKING:
    from sqlalchemy.engine.url import URL


if TYPE_CHECKING:
    from sqlalchemy.engine.reflection import Inspector


class BigQueryConfig(SqlConfig):
    """Configuration for BigQuery."""

    database_name: str = Field(alias="project_name")
    """The name of the project to use. In BigQuery, this is equivalent to the database name."""

    schema_name: str = Field(alias="dataset_name", default=DEFAULT_CACHE_SCHEMA_NAME)
    """The name of the dataset to use. In BigQuery, this is equivalent to the schema name."""

    credentials_path: str | None = None
    """The path to the credentials file to use.
    If not passed, falls back to the default inferred from the environment."""

    @property
    def project_name(self) -> str:
        """Return the project name (alias of self.database_name)."""
        return self.database_name

    @property
    def dataset_name(self) -> str:
        """Return the dataset name (alias of self.schema_name)."""
        return self.schema_name

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use.

        We suppress warnings about unrecognized JSON type. More info on that here:
        - https://github.com/airbytehq/PyAirbyte/issues/254
        """
        warnings.filterwarnings(
            "ignore",
            message="Did not recognize type 'JSON' of column",
            category=sqlalchemy.exc.SAWarning,
        )

        url: URL = make_url(f"bigquery://{self.project_name!s}")
        if self.credentials_path:
            url = url.update_query_dict({"credentials_path": self.credentials_path})

        return SecretString(url)

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database. For BigQuery, this is the project name."""
        return self.project_name

    def get_vendor_client(self) -> bigquery.Client:
        """Return a BigQuery python client."""
        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
        else:
            credentials, _ = google.auth.default()

        return bigquery.Client(credentials=credentials)


class BigQueryTypeConverter(SQLTypeConverter):
    """A class to convert types for BigQuery."""

    @classmethod
    def get_string_type(cls) -> sqlalchemy.types.TypeEngine:
        """Return the string type for BigQuery."""
        return "String"

    @overrides
    def to_sql_type(
        self,
        json_schema_property_def: dict[str, str | dict | list],
    ) -> sqlalchemy.types.TypeEngine:
        """Convert a value to a SQL type.

        We first call the parent class method to get the type. Then if the type is VARCHAR or
        BIGINT, we replace it with respective BigQuery types.
        """
        sql_type = super().to_sql_type(json_schema_property_def)
        # to-do: replace hardcoded return types with some sort of snowflake Variant equivalent
        if isinstance(sql_type, sqlalchemy.types.VARCHAR):
            return self.get_string_type()
        if isinstance(sql_type, sqlalchemy.types.BIGINT):
            return "INT64"

        return sql_type.__class__.__name__


class BigQuerySqlProcessor(SqlProcessorBase):
    """A BigQuery implementation of the SQL Processor."""

    file_writer_class = JsonlWriter
    type_converter_class = BigQueryTypeConverter
    supports_merge_insert = True

    sql_config: BigQueryConfig

    _schema_exists: bool = False

    @final
    @overrides
    def _fully_qualified(
        self,
        table_name: str,
    ) -> str:
        """Return the fully qualified name of the given table."""
        return f"`{self.sql_config.schema_name}`.`{table_name!s}`"

    @final
    @overrides
    def _quote_identifier(self, identifier: str) -> str:
        """Return the identifier name.

        BigQuery uses backticks to quote identifiers. Because BigQuery is case-sensitive for quoted
        identifiers, we convert the identifier to lowercase before quoting it.
        """
        return f"`{identifier.lower()}`"

    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Write a file(s) to a new table.

        This is a generic implementation, which can be overridden by subclasses
        to improve performance.
        """
        temp_table_name = self._create_table_for_loading(stream_name, batch_id)

        # Specify the table ID (in the format `project_id.dataset_id.table_id`)
        table_id = (
            f"{self.sql_config.project_name}.{self.sql_config.dataset_name}.{temp_table_name}"
        )

        # Initialize a BigQuery client
        client = self.sql_config.get_vendor_client()

        for file_path in files:
            with Path.open(file_path, "rb") as source_file:
                load_job = client.load_table_from_file(  # Make an API request
                    file_obj=source_file,
                    destination=table_id,
                    job_config=bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        schema=[
                            bigquery.SchemaField(name, field_type=str(type_))
                            for name, type_ in self._get_sql_column_definitions(
                                stream_name=stream_name
                            ).items()
                        ],
                    ),
                )
            _ = load_job.result()  # Wait for the job to complete

        return temp_table_name

    def _ensure_schema_exists(
        self,
    ) -> None:
        """Ensure the target schema exists.

        We override the default implementation because BigQuery is very slow at scanning schemas.

        This implementation simply calls "CREATE SCHEMA IF NOT EXISTS" and ignores any errors.
        """
        if self._schema_exists:
            return

        sql = f"CREATE SCHEMA IF NOT EXISTS {self.sql_config.schema_name}"
        try:
            self._execute_sql(sql)
        except Exception as ex:
            # Ignore schema exists errors.
            if "already exists" not in str(ex):
                raise

        self._schema_exists = True

    def _table_exists(
        self,
        table_name: str,
    ) -> bool:
        """Return true if the given table exists.

        We override the default implementation because BigQuery is very slow at scanning tables.
        """
        client = self.sql_config.get_vendor_client()
        table_id = f"{self.sql_config.project_name}.{self.sql_config.dataset_name}.{table_name}"
        try:
            client.get_table(table_id)
        except NotFound:
            return False

        except ValueError as ex:
            raise exc.PyAirbyteInputError(
                message="Invalid project name or dataset name.",
                context={
                    "table_id": table_id,
                    "table_name": table_name,
                    "project_name": self.sql_config.project_name,
                    "dataset_name": self.sql_config.dataset_name,
                },
            ) from ex

        return True

    @final
    @overrides
    def _get_tables_list(
        self,
    ) -> list[str]:
        """Get the list of available tables in the schema.

        For bigquery, {schema_name}.{table_name} is returned, so we need to
        strip the schema name in front of the table name, if it exists.

        Warning: This method is slow for BigQuery, as it needs to scan all tables in the dataset.
        It has been observed to take 30+ seconds in some cases.
        """
        with self.get_sql_connection() as conn:
            inspector: Inspector = sqlalchemy.inspect(conn)
            tables = inspector.get_table_names(schema=self.sql_config.schema_name)
            schema_prefix = f"{self.sql_config.schema_name}."
            return [
                table.replace(schema_prefix, "", 1) if table.startswith(schema_prefix) else table
                for table in tables
            ]

    def _swap_temp_table_with_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Swap the temp table with the main one, dropping the old version of the 'final' table.

        The BigQuery RENAME implementation requires that the table schema (dataset) is named in the
        first part of the ALTER statement, but not in the second part.

        For example, BigQuery expects this format:

        ALTER TABLE my_schema.my_old_table_name RENAME TO my_new_table_name;
        """
        if final_table_name is None:
            raise exc.PyAirbyteInternalError(message="Arg 'final_table_name' cannot be None.")
        if temp_table_name is None:
            raise exc.PyAirbyteInternalError(message="Arg 'temp_table_name' cannot be None.")

        _ = stream_name
        deletion_name = f"{final_table_name}_deleteme"
        commands = "\n".join(
            [
                f"ALTER TABLE {self._fully_qualified(final_table_name)} "
                f"RENAME TO {deletion_name};",
                f"ALTER TABLE {self._fully_qualified(temp_table_name)} "
                f"RENAME TO {final_table_name};",
                f"DROP TABLE {self._fully_qualified(deletion_name)};",
            ]
        )
        self._execute_sql(commands)
