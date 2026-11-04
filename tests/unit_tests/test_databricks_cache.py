# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Databricks cache and SQL processor (no Databricks workspace needed)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import sqlalchemy
from airbyte_api.models import (
    DestinationDatabricks,
    OAuth2Recommended,
    PersonalAccessToken,
)
from airbyte_protocol.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)

from airbyte import exceptions as exc
from airbyte._processors.sql import databricks as dbx
from airbyte._processors.sql.databricks import (
    DatabricksConfig,
    DatabricksOAuthM2MCredentialsProvider,
    DatabricksSqlProcessor,
    DatabricksTypeConverter,
)
from airbyte.caches import DatabricksCache
from airbyte.caches._utils._cache_to_dest import cache_to_destination_configuration
from airbyte.caches._utils._dest_to_cache import destination_to_cache
from airbyte.secrets.base import SecretString
from airbyte.shared.catalog_providers import CatalogProvider


HOST = "dbc-a1b2345c-d6e7.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/abc123"


@pytest.fixture(autouse=True)
def _offline_cache(mocker):
    """Let `DatabricksCache(...)` be constructed without connecting to a workspace."""
    mocker.patch.object(
        DatabricksSqlProcessor, "_ensure_schema_exists", return_value=None
    )
    mocker.patch("airbyte.caches.base.SqlCatalogBackend")
    mocker.patch("airbyte.caches.base.SqlStateBackend")


def _pat_config() -> DatabricksConfig:
    return DatabricksConfig(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        catalog="main",
        schema_name="airbyte_raw",
        access_token=SecretString("dapi-secret"),
    )


def _pat_cache(**kwargs) -> DatabricksCache:
    return DatabricksCache(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        catalog="main",
        schema_name="airbyte_raw",
        access_token=SecretString("dapi-secret"),
        **kwargs,
    )


def _oauth_cache() -> DatabricksCache:
    return DatabricksCache(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        catalog="main",
        schema_name="airbyte_raw",
        client_id="sp-client-id",
        client_secret=SecretString("sp-secret"),
    )


def _catalog(primary_key: list[list[str]] | None = None) -> ConfiguredAirbyteCatalog:
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "Name": {"type": "string"},
            "score": {"type": "number"},
            "birthday": {"type": "string", "format": "date"},
            "updated_at": {"type": "string", "format": "date-time"},
            "address": {"type": "object"},
        },
    }
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="users",
                    json_schema=schema,
                    supported_sync_modes=[SyncMode.incremental],
                ),
                sync_mode=SyncMode.incremental,
                destination_sync_mode=DestinationSyncMode.append_dedup,
                primary_key=primary_key if primary_key is not None else [["id"]],
            )
        ]
    )


def _processor(
    mocker,
    sql_config: DatabricksConfig | None = None,
    primary_key: list[list[str]] | None = None,
) -> tuple[DatabricksSqlProcessor, list[str]]:
    executed: list[str] = []
    mocker.patch.object(
        DatabricksSqlProcessor,
        "_execute_sql",
        side_effect=lambda sql: executed.append(str(sql)),
    )
    processor = DatabricksSqlProcessor(
        catalog_provider=CatalogProvider(_catalog(primary_key)),
        temp_dir=Path(),
        temp_file_cleanup=True,
        sql_config=sql_config or _pat_config(),
    )
    return processor, executed


class TestAuthentication:
    def test_requires_an_auth_method(self):
        config = DatabricksConfig(
            server_hostname=HOST, http_path=HTTP_PATH, catalog="main"
        )
        with pytest.raises(exc.AirbyteLibInputError, match="No authentication method"):
            config.get_sql_alchemy_url()

    def test_rejects_token_and_oauth_together(self):
        config = DatabricksConfig(
            server_hostname=HOST,
            http_path=HTTP_PATH,
            catalog="main",
            access_token=SecretString("dapi"),
            client_id="id",
            client_secret=SecretString("secret"),
        )
        with pytest.raises(exc.AirbyteLibInputError, match="Multiple authentication"):
            config.get_sql_alchemy_url()

    @pytest.mark.parametrize(
        "kwargs",
        [{"client_id": "id"}, {"client_secret": SecretString("secret")}],
    )
    def test_rejects_partial_oauth(self, kwargs):
        config = DatabricksConfig(
            server_hostname=HOST, http_path=HTTP_PATH, catalog="main", **kwargs
        )
        with pytest.raises(exc.AirbyteLibInputError, match="requires both"):
            config.get_sql_alchemy_url()

    def test_pat_url_and_connect_args(self):
        cache = _pat_cache()
        url = sqlalchemy.engine.make_url(str(cache.get_sql_alchemy_url()))
        assert url.drivername == "databricks"
        assert url.username == "token"
        assert url.password == "dapi-secret"
        assert url.host == HOST
        assert url.query == {
            "http_path": HTTP_PATH,
            "catalog": "main",
            "schema": "airbyte_raw",
        }
        assert cache.get_sql_alchemy_connect_args() == {}
        assert cache.get_database_name() == "main"

    def test_oauth_uses_credentials_provider_not_url(self):
        cache = _oauth_cache()
        url = sqlalchemy.engine.make_url(str(cache.get_sql_alchemy_url()))
        assert url.username is None
        assert url.password is None
        connect_args = cache.get_sql_alchemy_connect_args()
        provider = connect_args["credentials_provider"]
        assert isinstance(provider, DatabricksOAuthM2MCredentialsProvider)
        # The provider is reused so that tokens are cached across connections.
        assert cache.get_sql_alchemy_connect_args()["credentials_provider"] is provider
        assert "access_token" not in cache._get_connection_kwargs()

    def test_missing_extra_gives_install_hint(self, mocker):
        mocker.patch.object(dbx.importlib.util, "find_spec", return_value=None)
        with pytest.raises(exc.AirbyteLibInputError, match=r"airbyte\[databricks\]"):
            _pat_cache().get_sql_alchemy_url()


class TestOAuthM2MCredentialsProvider:
    def _provider(self) -> DatabricksOAuthM2MCredentialsProvider:
        return DatabricksOAuthM2MCredentialsProvider(
            server_hostname=HOST,
            client_id="sp-client-id",
            client_secret=SecretString("sp-secret"),
        )

    def test_fetches_caches_and_refreshes_token(self, mocker):
        now = [1000.0]
        mocker.patch.object(dbx.time, "monotonic", side_effect=lambda: now[0])
        responses = iter(["token-1", "token-2"])

        def fake_post(url, auth, data, timeout):
            assert url == f"https://{HOST}/oidc/v1/token"
            assert auth == ("sp-client-id", "sp-secret")
            assert data == {"grant_type": "client_credentials", "scope": "all-apis"}
            return MagicMock(
                status_code=200,
                json=lambda: {"access_token": next(responses), "expires_in": 3600},
            )

        post = mocker.patch.object(dbx.requests, "post", side_effect=fake_post)
        header_factory = self._provider()()

        assert header_factory() == {"Authorization": "Bearer token-1"}
        now[0] += 3000  # still more than the refresh margin before expiry
        assert header_factory() == {"Authorization": "Bearer token-1"}
        assert post.call_count == 1

        now[0] += 560  # within 60 seconds of expiry
        assert header_factory() == {"Authorization": "Bearer token-2"}
        assert post.call_count == 2

    def test_token_error_does_not_leak_secret(self, mocker):
        mocker.patch.object(
            dbx.requests, "post", return_value=MagicMock(status_code=401, text="nope")
        )
        with pytest.raises(exc.AirbyteLibInputError) as ex_info:
            self._provider().get_access_token()
        assert "sp-secret" not in str(ex_info.value)
        assert ex_info.value.context["status_code"] == 401


class TestTypeConverter:
    @pytest.mark.parametrize(
        ("json_schema", "expected"),
        [
            ({"type": "string"}, "STRING"),
            ({"type": "integer"}, "BIGINT"),
            ({"type": "boolean"}, "BOOLEAN"),
            ({"type": "string", "format": "date"}, "DATE"),
            ({"type": "string", "format": "date-time"}, "TIMESTAMP"),
            (
                {
                    "type": "string",
                    "format": "date-time",
                    "airbyte_type": "timestamp_without_timezone",
                },
                "TIMESTAMP_NTZ",
            ),
            ({"type": "string", "format": "time"}, "STRING"),
            ({"type": "object"}, "STRING"),
            ({"type": "array", "items": {"type": "string"}}, "STRING"),
        ],
    )
    def test_to_sql_type(self, json_schema, expected):
        assert str(DatabricksTypeConverter().to_sql_type(json_schema)) == expected

    def test_number_is_decimal(self):
        sql_type = DatabricksTypeConverter().to_sql_type({"type": "number"})
        assert isinstance(sql_type, sqlalchemy.types.DECIMAL)


class TestSqlGeneration:
    def test_identifiers_are_lowercased_and_backtick_quoted(self, mocker):
        processor, _ = _processor(mocker)
        assert processor._quote_identifier("Name") == "`name`"
        assert processor._quote_identifier("we`ird") == "`we``ird`"
        assert processor._fully_qualified("Users") == "`main`.`airbyte_raw`.`users`"

    def test_string_literal_escaping(self):
        literal = DatabricksSqlProcessor._sql_string_literal("O'Brien \\ x")
        assert literal == "'O\\'Brien \\\\ x'"

    def test_copy_into_select_extracts_and_casts_each_column(self, mocker):
        processor, _ = _processor(mocker)
        exprs = processor._get_copy_into_select_expressions("users")
        assert "CAST(get_json_object(value, '$[\\'id\\']') AS BIGINT) AS `id`" in exprs
        # Strings and nested JSON are read as text, without a cast.
        assert "get_json_object(value, '$[\\'name\\']') AS `name`" in exprs
        assert "get_json_object(value, '$[\\'address\\']') AS `address`" in exprs
        assert any(
            e.endswith("AS `_airbyte_extracted_at`") and "TIMESTAMP)" in e
            for e in exprs
        )

    def test_write_files_puts_copies_and_removes(self, mocker, tmp_path):
        processor, executed = _processor(mocker)
        files = [tmp_path / "users_1.jsonl.gz", tmp_path / "users_2.jsonl.gz"]
        for f in files:
            f.write_bytes(b"")
        cursor = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        get_client = mocker.patch.object(
            DatabricksConfig, "get_vendor_client", return_value=conn
        )

        temp_table = processor._write_files_to_new_table(files, "users", "batch1")

        assert (
            executed[0]
            .strip()
            .startswith(f"CREATE TABLE `main`.`airbyte_raw`.`{temp_table}`")
        )
        assert executed[1] == (
            "CREATE VOLUME IF NOT EXISTS `main`.`airbyte_raw`.`airbyte_staging`"
        )
        assert get_client.call_args.kwargs == {
            "staging_allowed_local_path": [str(tmp_path.absolute())]
        }
        statements = [c.args[0] for c in cursor.execute.call_args_list]
        remote_dir = f"/Volumes/main/airbyte_raw/airbyte_staging/{temp_table}"
        assert statements[0] == (
            f"PUT '{files[0].absolute()}' INTO '{remote_dir}/users_1.jsonl.gz' OVERWRITE"
        )
        assert statements[2].startswith(
            f"COPY INTO `main`.`airbyte_raw`.`{temp_table}`"
        )
        assert f"FROM '{remote_dir}'" in statements[2]
        assert "FILEFORMAT = TEXT" in statements[2]
        assert "FILES = ('users_1.jsonl.gz', 'users_2.jsonl.gz')" in statements[2]
        assert statements[3:] == [
            f"REMOVE '{remote_dir}/users_1.jsonl.gz'",
            f"REMOVE '{remote_dir}/users_2.jsonl.gz'",
        ]

    def test_staged_files_removed_even_if_copy_fails(self, mocker, tmp_path):
        processor, _ = _processor(mocker)
        file = tmp_path / "users_1.jsonl.gz"
        file.write_bytes(b"")
        cursor = MagicMock()

        def execute(sql):
            if sql.startswith("COPY INTO"):
                raise RuntimeError("copy failed")

        cursor.execute.side_effect = execute
        conn = MagicMock()
        conn.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        mocker.patch.object(DatabricksConfig, "get_vendor_client", return_value=conn)

        with pytest.raises(RuntimeError, match="copy failed"):
            processor._write_files_to_new_table([file], "users", "batch1")
        assert cursor.execute.call_args_list[-1].args[0].startswith("REMOVE ")

    def test_partial_upload_failure_removes_uploaded_files(self, mocker, tmp_path):
        processor, _ = _processor(mocker)
        files = [tmp_path / "users_1.jsonl.gz", tmp_path / "users_2.jsonl.gz"]
        for f in files:
            f.write_bytes(b"")
        cursor = MagicMock()

        def execute(sql):
            if sql.startswith("PUT") and "users_2" in sql:
                raise RuntimeError("upload failed")

        cursor.execute.side_effect = execute
        conn = MagicMock()
        conn.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        mocker.patch.object(DatabricksConfig, "get_vendor_client", return_value=conn)

        with pytest.raises(exc.AirbyteLibInternalError, match="Failed to upload"):
            processor._write_files_to_new_table(files, "users", "batch1")
        statements = [c.args[0] for c in cursor.execute.call_args_list]
        assert not any(s.startswith("COPY INTO") for s in statements)
        assert [s for s in statements if s.startswith("REMOVE")] == [
            "REMOVE '/Volumes/main/airbyte_raw/airbyte_staging/users_batch1/users_1.jsonl.gz'"
        ]

    def test_merge_deduplicates_source_rows(self, mocker):
        processor, executed = _processor(mocker)
        processor._merge_temp_table_to_final_table("users", "users_tmp", "users")
        sql = executed[-1]
        assert sql.startswith("MERGE INTO `main`.`airbyte_raw`.`users` AS final")
        assert "PARTITION BY `id` ORDER BY `_airbyte_extracted_at` DESC" in sql
        assert "WHERE `_airbyte_row_number` = 1" in sql
        assert "ON final.`id` = tmp.`id`" in sql
        assert "final.`name` = tmp.`name`" in sql
        assert "final.`id` = tmp.`id`," not in sql  # primary keys are not updated
        assert "WHEN NOT MATCHED THEN INSERT (`id`, `name`" in sql
        # The helper row number column must not be inserted into the final table.
        insert_clause = sql.split("WHEN NOT MATCHED")[1]
        assert "_airbyte_row_number" not in insert_clause

    def test_merge_composite_primary_key(self, mocker):
        processor, executed = _processor(mocker, primary_key=[["id"], ["Name"]])
        processor._merge_temp_table_to_final_table("users", "users_tmp", "users")
        assert "PARTITION BY `id`, `name`" in executed[-1]
        assert (
            "ON final.`id` = tmp.`id`\n  AND final.`name` = tmp.`name`" in executed[-1]
        )

    def test_merge_without_primary_key_raises(self, mocker):
        processor, _ = _processor(mocker, primary_key=[])
        with pytest.raises(exc.AirbyteLibInternalError, match="without primary keys"):
            processor._merge_temp_table_to_final_table("users", "users_tmp", "users")

    def test_swap_renames_with_fully_qualified_names(self, mocker):
        processor, executed = _processor(mocker)
        processor._swap_temp_table_with_final_table("users", "users_tmp", "users")
        fq = "`main`.`airbyte_raw`"
        assert executed == [
            f"ALTER TABLE {fq}.`users` RENAME TO {fq}.`users_deleteme`",
            f"ALTER TABLE {fq}.`users_tmp` RENAME TO {fq}.`users`",
            f"DROP TABLE {fq}.`users_deleteme`",
        ]

    def test_ensure_schema_exists(self, mocker):
        processor, executed = _processor(mocker)
        mocker.stopall()
        mocker.patch.object(
            DatabricksSqlProcessor,
            "_execute_sql",
            side_effect=lambda sql: executed.append(str(sql)),
        )
        processor._ensure_schema_exists()
        processor._ensure_schema_exists()
        assert executed == ["CREATE SCHEMA IF NOT EXISTS `main`.`airbyte_raw`"]


class TestDestinationPairing:
    def test_pat_cache_to_destination(self):
        dest = cache_to_destination_configuration(_pat_cache())
        assert isinstance(dest, DestinationDatabricks)
        assert dest.hostname == HOST
        assert dest.http_path == HTTP_PATH
        assert dest.database == "main"
        assert dest.schema == "airbyte_raw"
        assert dest.authentication == PersonalAccessToken(
            personal_access_token="dapi-secret"
        )
        assert dest.accept_terms is False

    def test_oauth_cache_to_destination(self):
        dest = _oauth_cache().paired_destination_config
        assert dest.authentication == OAuth2Recommended(
            client_id="sp-client-id", secret="sp-secret"
        )

    def test_round_trip(self):
        for cache in (_pat_cache(), _oauth_cache()):
            restored = destination_to_cache(cache_to_destination_configuration(cache))
            assert isinstance(restored, DatabricksCache)
            assert restored.server_hostname == cache.server_hostname
            assert restored.http_path == cache.http_path
            assert restored.catalog == cache.catalog
            assert restored.schema_name == cache.schema_name
            assert restored.access_token == cache.access_token
            assert restored.client_id == cache.client_id
            assert restored.client_secret == cache.client_secret

    def test_obfuscated_secret_is_resolved_from_secret_manager(self, mocker):
        mocker.patch(
            "airbyte.caches._utils._dest_to_cache.get_secret",
            return_value=SecretString("dapi-from-secrets"),
        )
        restored = destination_to_cache({
            "destinationType": "databricks",
            "hostname": HOST,
            "http_path": HTTP_PATH,
            "database": "main",
            "schema": "airbyte_raw",
            "authentication": {
                "auth_type": "BASIC",
                "personal_access_token": "**********",
            },
        })
        assert isinstance(restored, DatabricksCache)
        assert str(restored.access_token) == "dapi-from-secrets"
