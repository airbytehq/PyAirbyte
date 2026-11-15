# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""A Databricks (Unity Catalog) implementation of the SQL processor.

Records are bulk loaded through a Unity Catalog volume:

1. Each batch of gzipped JSONL files is uploaded to
   `/Volumes/<catalog>/<schema>/<staging_volume>/<batch_id>/` with the connector's `PUT` command.
2. A single `COPY INTO` statement loads the files into the batch's temp table. Files are read as
   text and each column is extracted with `get_json_object()`, so nested objects and arrays land
   as their original JSON text instead of Spark struct strings.
3. The staged files are removed (`REMOVE`), and the temp table is appended, merged or swapped into
   the final table like the other SQL caches.

Merges use Delta `MERGE INTO`. Because Delta rejects a merge where more than one source row matches
the same target row, the source batch is first deduplicated to the latest record per primary key.
"""

from __future__ import annotations

import importlib.util
import threading
import time
from typing import TYPE_CHECKING, Any

import requests
import sqlalchemy
from overrides import overrides
from pydantic import Field
from sqlalchemy.engine import URL
from sqlalchemy.types import UserDefinedType

from airbyte import exceptions as exc
from airbyte._writers.jsonl import JsonlWriter
from airbyte.constants import AB_EXTRACTED_AT_COLUMN, DEFAULT_CACHE_SCHEMA_NAME
from airbyte.secrets.base import SecretString
from airbyte.shared import SqlProcessorBase
from airbyte.shared.sql_processor import SqlConfig
from airbyte.types import SQLTypeConverter


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


DEFAULT_STAGING_VOLUME_NAME = "airbyte_staging"
"""The default name of the Unity Catalog volume used to stage files for `COPY INTO`."""

_OAUTH_TOKEN_REFRESH_MARGIN_SECONDS = 60
_OAUTH_REQUEST_TIMEOUT_SECONDS = 30


def _ensure_databricks_dialect_installed() -> None:
    """Raise a helpful error if the optional Databricks dependencies are missing."""
    if importlib.util.find_spec("databricks.sqlalchemy") is None:
        raise exc.AirbyteLibInputError(
            message=(
                "The Databricks cache requires the optional `databricks` extra. "
                "Install it with `pip install 'airbyte[databricks]'`."
            ),
        )


class DatabricksOAuthM2MCredentialsProvider:
    """OAuth machine-to-machine (service principal) credentials for the Databricks connector.

    Implements the `credentials_provider` protocol of `databricks-sql-connector`: calling the
    provider returns a header factory, and each call to the header factory returns the
    `Authorization` header. Tokens come from the workspace's `/oidc/v1/token` endpoint using the
    client-credentials grant and are refreshed shortly before they expire.

    See: https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html
    """

    def __init__(
        self,
        *,
        server_hostname: str,
        client_id: str,
        client_secret: SecretString,
    ) -> None:
        """Initialize the provider."""
        self._token_url = f"https://{server_hostname}/oidc/v1/token"
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token: str | None = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def auth_type(self) -> str:
        """Return the auth type name used by the connector."""
        return "oauth-m2m"

    def _refresh_token(self) -> None:
        response = requests.post(
            self._token_url,
            auth=(self._client_id, str(self._client_secret)),
            data={"grant_type": "client_credentials", "scope": "all-apis"},
            timeout=_OAUTH_REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:  # noqa: PLR2004
            raise exc.AirbyteLibInputError(
                message="Failed to get a Databricks OAuth token for the service principal.",
                context={
                    "token_url": self._token_url,
                    "client_id": self._client_id,
                    "status_code": response.status_code,
                },
            )
        payload = response.json()
        self._access_token = payload["access_token"]
        self._expires_at = time.monotonic() + float(payload.get("expires_in", 3600))

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing it if it is missing or about to expire."""
        with self._lock:
            if (
                self._access_token is None
                or time.monotonic() >= self._expires_at - _OAUTH_TOKEN_REFRESH_MARGIN_SECONDS
            ):
                self._refresh_token()
            return str(self._access_token)

    def __call__(self, *args: Any, **kwargs: Any) -> Callable[[], dict[str, str]]:  # noqa: ANN401, ARG002
        """Return a header factory, as expected by `databricks-sql-connector`."""

        def header_factory() -> dict[str, str]:
            return {"Authorization": f"Bearer {self.get_access_token()}"}

        return header_factory


class DatabricksConfig(SqlConfig):
    """Configuration for a Databricks SQL warehouse (or cluster) on Unity Catalog."""

    server_hostname: str
    """The workspace hostname, for example `dbc-a1b2345c-d6e7.cloud.databricks.com`."""

    http_path: str
    """The HTTP path of the SQL warehouse or cluster, for example `/sql/1.0/warehouses/abc123`."""

    catalog: str
    """The Unity Catalog catalog to write to. In Databricks, this is the database name."""

    schema_name: str = Field(default=DEFAULT_CACHE_SCHEMA_NAME)
    """The schema to write to. It is created if it does not exist."""

    access_token: SecretString | None = None
    """A personal access token. Provide either this or `client_id` and `client_secret`."""

    client_id: str | None = None
    """The application (client) ID of a service principal, for OAuth M2M authentication."""

    client_secret: SecretString | None = None
    """The OAuth secret of the service principal, for OAuth M2M authentication."""

    staging_volume: str = DEFAULT_STAGING_VOLUME_NAME
    """The Unity Catalog volume (in `catalog.schema_name`) used to stage files for `COPY INTO`.

    The volume is created if it does not exist. To use an existing volume instead, grant the
    principal `READ VOLUME` and `WRITE VOLUME` on it.
    """

    _credentials_provider: DatabricksOAuthM2MCredentialsProvider | None = None

    def _validate_authentication_config(self) -> None:
        """Validate that exactly one authentication method is configured."""
        has_token = self.access_token is not None
        has_client_id = self.client_id is not None
        has_client_secret = self.client_secret is not None

        if has_client_id != has_client_secret:
            raise exc.AirbyteLibInputError(
                message="OAuth M2M authentication requires both `client_id` and `client_secret`.",
            )
        if has_token and has_client_id:
            raise exc.AirbyteLibInputError(
                message=(
                    "Multiple authentication methods provided. Please provide either "
                    "'access_token' or 'client_id' and 'client_secret', not both."
                ),
            )
        if not has_token and not has_client_id:
            raise exc.AirbyteLibInputError(
                message=(
                    "No authentication method provided. Please provide either "
                    "'access_token' or 'client_id' and 'client_secret'."
                ),
            )

    def _get_credentials_provider(self) -> DatabricksOAuthM2MCredentialsProvider | None:
        """Return the (cached) OAuth M2M credentials provider, if OAuth M2M is configured."""
        if self.client_id is None or self.client_secret is None:
            return None
        if self._credentials_provider is None:
            self._credentials_provider = DatabricksOAuthM2MCredentialsProvider(
                server_hostname=self.server_hostname,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        return self._credentials_provider

    def _get_connection_kwargs(self) -> dict[str, Any]:
        """Return the keyword arguments to pass to `databricks.sql.connect()`."""
        self._validate_authentication_config()
        kwargs: dict[str, Any] = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "catalog": self.catalog,
            "schema": self.schema_name,
        }
        credentials_provider = self._get_credentials_provider()
        if credentials_provider is not None:
            kwargs["credentials_provider"] = credentials_provider
        else:
            kwargs["access_token"] = str(self.access_token)
        return kwargs

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use."""
        _ensure_databricks_dialect_installed()
        self._validate_authentication_config()
        url = URL.create(
            drivername="databricks",
            username="token" if self.access_token is not None else None,
            password=str(self.access_token) if self.access_token is not None else None,
            host=self.server_hostname,
            query={
                "http_path": self.http_path,
                "catalog": self.catalog,
                "schema": self.schema_name,
            },
        )
        return SecretString(url.render_as_string(hide_password=False))

    @overrides
    def get_sql_alchemy_connect_args(self) -> dict[str, Any]:
        """Return the SQLAlchemy connect_args.

        With OAuth M2M, the token is not part of the URL; the connector gets it from a
        credentials provider instead.
        """
        self._validate_authentication_config()
        credentials_provider = self._get_credentials_provider()
        if credentials_provider is None:
            return {}
        return {"credentials_provider": credentials_provider}

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database. For Databricks, this is the catalog name."""
        return self.catalog

    def get_vendor_client(
        self,
        *,
        staging_allowed_local_path: str | list[str] | None = None,
    ) -> Any:  # noqa: ANN401  # The connector's connection type is not part of our public API.
        """Return a `databricks.sql` connection.

        Pass `staging_allowed_local_path` to allow `PUT` uploads from those local directories.
        """
        _ensure_databricks_dialect_installed()
        from databricks import sql as databricks_sql  # noqa: PLC0415  # Optional dependency.

        kwargs = self._get_connection_kwargs()
        if staging_allowed_local_path is not None:
            kwargs["staging_allowed_local_path"] = staging_allowed_local_path
        return databricks_sql.connect(**kwargs)


class _DatabricksType(UserDefinedType):
    """A column type that renders as a literal Databricks SQL type name in DDL."""

    cache_ok = True

    def __init__(self, type_name: str) -> None:
        self.type_name = type_name

    def get_col_spec(self, **kw: Any) -> str:  # noqa: ANN401, ARG002
        return self.type_name

    def __repr__(self) -> str:
        return f"_DatabricksType({self.type_name!r})"


DATABRICKS_STRING = _DatabricksType("STRING")
DATABRICKS_TIMESTAMP = _DatabricksType("TIMESTAMP")
DATABRICKS_TIMESTAMP_NTZ = _DatabricksType("TIMESTAMP_NTZ")


class DatabricksTypeConverter(SQLTypeConverter):
    """A class to convert JSON schema types to Databricks SQL types.

    Databricks has no `VARCHAR` without a length, no `TIME` and no `JSON` type, so strings, times
    and nested objects/arrays are stored as `STRING`. Nested values keep their JSON text.
    """

    @classmethod
    def get_string_type(cls) -> sqlalchemy.types.TypeEngine:
        """Return the string type for Databricks."""
        return DATABRICKS_STRING

    @classmethod
    def get_json_type(cls) -> sqlalchemy.types.TypeEngine:
        """Return the type used for nested JSON data (stored as JSON text)."""
        return DATABRICKS_STRING

    @overrides
    def to_sql_type(
        self,
        json_schema_property_def: dict[str, str | dict | list],
    ) -> sqlalchemy.types.TypeEngine:
        """Convert a JSON schema property to a Databricks SQL type."""
        sql_type = super().to_sql_type(json_schema_property_def)
        if isinstance(sql_type, sqlalchemy.types.TIMESTAMP):
            return DATABRICKS_TIMESTAMP if sql_type.timezone else DATABRICKS_TIMESTAMP_NTZ
        if isinstance(
            sql_type,
            (
                sqlalchemy.types.String,
                sqlalchemy.types.JSON,
                sqlalchemy.types.ARRAY,
                sqlalchemy.types.Time,
            ),
        ):
            return DATABRICKS_STRING
        return sql_type


class DatabricksSqlProcessor(SqlProcessorBase):
    """A Databricks implementation of the SQL processor, backed by Delta tables."""

    file_writer_class = JsonlWriter
    type_converter_class: type[DatabricksTypeConverter] = (  # pyrefly: ignore[bad-override]
        DatabricksTypeConverter
    )
    supports_merge_insert = True
    sql_config: DatabricksConfig

    _staging_volume_exists: bool = False

    @overrides
    def _quote_identifier(self, identifier: str) -> str:
        """Quote an identifier with backticks.

        Unity Catalog stores names in lower case, so identifiers are lower-cased as well.
        """
        escaped = identifier.lower().replace("`", "``")
        return f"`{escaped}`"

    def _fully_qualified_schema(self) -> str:
        return (
            f"{self._quote_identifier(self.sql_config.catalog)}."
            f"{self._quote_identifier(self.sql_config.schema_name)}"
        )

    @overrides
    def _fully_qualified(
        self,
        table_name: str,
    ) -> str:
        """Return the three-part `catalog`.`schema`.`table` name."""
        return f"{self._fully_qualified_schema()}.{self._quote_identifier(table_name)}"

    @overrides
    def _ensure_schema_exists(self) -> None:
        """Create the schema in the configured catalog if it does not exist."""
        schema_name = self.normalizer.normalize(self.sql_config.schema_name)
        known_schemas = self.normalizer.normalize_list(self._known_schemas_list)
        if known_schemas and schema_name in known_schemas:
            return

        self._execute_sql(f"CREATE SCHEMA IF NOT EXISTS {self._fully_qualified_schema()}")
        self._known_schemas_list = [*self._known_schemas_list, schema_name]

    @property
    def _staging_volume_path(self) -> str:
        """Return the `/Volumes/...` path of the staging volume."""
        return (
            f"/Volumes/{self.sql_config.catalog}/{self.sql_config.schema_name}"
            f"/{self.sql_config.staging_volume}"
        )

    def _ensure_staging_volume_exists(self) -> None:
        """Create the staging volume if it does not exist (checked once per processor)."""
        if self._staging_volume_exists:
            return

        self._ensure_schema_exists()
        self._execute_sql(
            f"CREATE VOLUME IF NOT EXISTS {self._fully_qualified_schema()}."
            f"{self._quote_identifier(self.sql_config.staging_volume)}"
        )
        self._staging_volume_exists = True

    @staticmethod
    def _sql_string_literal(value: str) -> str:
        """Return `value` as a single-quoted Databricks SQL string literal."""
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    def _get_copy_into_select_expressions(self, stream_name: str) -> list[str]:
        """Return one `SELECT` expression per column, reading from the raw JSON line (`value`)."""
        json_type_name = DATABRICKS_STRING.type_name
        expressions: list[str] = []
        for column_name, sql_type in self._get_sql_column_definitions(stream_name).items():
            json_path = self._sql_string_literal(f"$['{column_name}']")
            extracted = f"get_json_object(value, {json_path})"
            type_name = str(sql_type)
            if type_name != json_type_name:
                extracted = f"CAST({extracted} AS {type_name})"
            expressions.append(f"{extracted} AS {self._quote_identifier(column_name)}")
        return expressions

    @overrides
    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Upload the batch files to the staging volume and `COPY INTO` a new temp table."""
        temp_table_name = self._create_table_for_loading(
            stream_name=stream_name,
            batch_id=batch_id,
        )
        if not files:
            return temp_table_name

        self._ensure_staging_volume_exists()
        remote_dir = f"{self._staging_volume_path}/{temp_table_name}"
        remote_paths = [f"{remote_dir}/{file_path.name}" for file_path in files]
        local_dirs = sorted({str(file_path.parent.absolute()) for file_path in files})

        with (
            self.sql_config.get_vendor_client(staging_allowed_local_path=local_dirs) as conn,
            conn.cursor() as cursor,
        ):
            uploaded: list[str] = []
            try:
                try:
                    for file_path, remote_path in zip(files, remote_paths, strict=True):
                        cursor.execute(
                            f"PUT {self._sql_string_literal(str(file_path.absolute()))} "
                            f"INTO {self._sql_string_literal(remote_path)} OVERWRITE"
                        )
                        uploaded.append(remote_path)
                except Exception as ex:
                    raise exc.AirbyteLibInternalError(
                        message="Failed to upload batch files to the Databricks staging volume.",
                        context={
                            "files": [str(f) for f in files],
                            "staging_volume_path": remote_dir,
                        },
                    ) from ex

                select_list = ",\n    ".join(self._get_copy_into_select_expressions(stream_name))
                files_list = ", ".join(self._sql_string_literal(f.name) for f in files)
                cursor.execute(
                    f"COPY INTO {self._fully_qualified(temp_table_name)}\n"
                    f"FROM (\n  SELECT\n    {select_list}\n"
                    f"  FROM {self._sql_string_literal(remote_dir)}\n)\n"
                    f"FILEFORMAT = TEXT\n"
                    f"FILES = ({files_list})"
                )
            finally:
                # Remove whatever was staged, also when an upload or the load failed.
                for remote_path in uploaded:
                    cursor.execute(f"REMOVE {self._sql_string_literal(remote_path)}")

        return temp_table_name

    @overrides
    def _add_column_to_table(
        self,
        table: sqlalchemy.Table,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Add a column to the given table."""
        self._execute_sql(
            f"ALTER TABLE {self._fully_qualified(table.name)} "
            f"ADD COLUMN {self._quote_identifier(column_name)} {column_type}"
        )

    @overrides
    def _swap_temp_table_with_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Swap the temp table into place of the final table.

        The Databricks connector runs one statement per call, so the rename sequence is executed
        statement by statement, using fully qualified names on both sides of each rename.
        """
        if final_table_name is None:
            raise exc.AirbyteLibInternalError(message="Arg 'final_table_name' cannot be None.")
        if temp_table_name is None:
            raise exc.AirbyteLibInternalError(message="Arg 'temp_table_name' cannot be None.")

        _ = stream_name
        deletion_name = f"{final_table_name}_deleteme"
        self._execute_sql(
            f"ALTER TABLE {self._fully_qualified(final_table_name)} "
            f"RENAME TO {self._fully_qualified(deletion_name)}"
        )
        self._execute_sql(
            f"ALTER TABLE {self._fully_qualified(temp_table_name)} "
            f"RENAME TO {self._fully_qualified(final_table_name)}"
        )
        self._execute_sql(f"DROP TABLE {self._fully_qualified(deletion_name)}")

    @overrides
    def _merge_temp_table_to_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Merge the temp table into the final table with Delta `MERGE INTO`.

        Delta fails a merge when several source rows match one target row, which happens when an
        incremental batch contains more than one version of a record. The source is therefore
        reduced to the most recently extracted row per primary key before merging.
        """
        nl = "\n"
        columns = [self._quote_identifier(c) for c in self._get_sql_column_definitions(stream_name)]
        pk_columns = [
            self._quote_identifier(c) for c in self.catalog_provider.get_primary_keys(stream_name)
        ]
        if not pk_columns:
            raise exc.AirbyteLibInternalError(
                message="Cannot merge a stream without primary keys.",
                context={"stream_name": stream_name},
            )
        non_pk_columns = [c for c in columns if c not in pk_columns]
        join_clause = f"{nl}  AND ".join(f"final.{pk} = tmp.{pk}" for pk in pk_columns)
        set_clause = f",{nl}    ".join(f"final.{col} = tmp.{col}" for col in non_pk_columns)
        partition_by = ", ".join(pk_columns)
        extracted_at = self._quote_identifier(AB_EXTRACTED_AT_COLUMN)
        update_clause = (
            f"WHEN MATCHED THEN UPDATE SET{nl}    {set_clause}{nl}" if set_clause else ""
        )
        row_number = self._quote_identifier("_airbyte_row_number")
        self._execute_sql(
            f"MERGE INTO {self._fully_qualified(final_table_name)} AS final\n"
            f"USING (\n"
            f"  SELECT {', '.join(columns)} FROM (\n"
            f"    SELECT *, ROW_NUMBER() OVER (\n"
            f"      PARTITION BY {partition_by} ORDER BY {extracted_at} DESC\n"
            f"    ) AS {row_number}\n"
            f"    FROM {self._fully_qualified(temp_table_name)}\n"
            f"  )\n"
            f"  WHERE {row_number} = 1\n"
            f") AS tmp\n"
            f"ON {join_clause}\n"
            f"{update_clause}"
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})\n"
            f"VALUES ({', '.join(f'tmp.{col}' for col in columns)})"
        )
