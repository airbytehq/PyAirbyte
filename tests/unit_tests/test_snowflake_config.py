# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Unit tests for SnowflakeConfig methods."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from airbyte._processors.sql.snowflake import SnowflakeConfig
from airbyte.secrets.base import SecretString


@pytest.fixture
def basic_config():
    """Basic SnowflakeConfig without authentication."""
    return SnowflakeConfig(
        account="test_account",
        username="test_user",
        warehouse="test_warehouse",
        database="test_database",
        role="test_role",
    )


@pytest.fixture
def password_config():
    """SnowflakeConfig with password authentication."""
    return SnowflakeConfig(
        account="test_account",
        username="test_user",
        password=SecretString("test_password"),
        warehouse="test_warehouse",
        database="test_database",
        role="test_role",
    )


@pytest.fixture
def private_key_config():
    """SnowflakeConfig with private key authentication."""
    private_key_pem = _generate_test_private_key()
    return SnowflakeConfig(
        account="test_account",
        username="test_user",
        private_key=SecretString(private_key_pem.decode("utf-8")),
        warehouse="test_warehouse",
        database="test_database",
        role="test_role",
    )


@pytest.fixture
def private_key_with_passphrase_config():
    """SnowflakeConfig with private key and passphrase authentication."""
    passphrase = "test_passphrase"
    private_key_pem = _generate_test_private_key_with_passphrase(passphrase)
    return SnowflakeConfig(
        account="test_account",
        username="test_user",
        private_key=SecretString(private_key_pem.decode("utf-8")),
        private_key_passphrase=SecretString(passphrase),
        warehouse="test_warehouse",
        database="test_database",
        role="test_role",
    )


class TestGetPrivateKeyContent:
    """Tests for _get_private_key_content method."""

    def test_get_private_key_content_from_private_key(self, private_key_config):
        """Test getting private key content when private_key is provided."""
        expected_content = str(private_key_config.private_key).encode("utf-8")

        content = private_key_config._get_private_key_content()

        assert isinstance(content, bytes)
        assert content == expected_content

    def test_get_private_key_content_from_file_path(self):
        """Test getting private key content when private_key_path is provided."""
        private_key_pem = _generate_test_private_key()

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
            temp_file.write(private_key_pem)
            temp_file_path = temp_file.name

        try:
            config = SnowflakeConfig(
                account="test_account",
                username="test_user",
                private_key_path=temp_file_path,
                warehouse="test_warehouse",
                database="test_database",
                role="test_role",
            )
            content = config._get_private_key_content()
            assert isinstance(content, bytes)
            assert content == private_key_pem
        finally:
            Path(temp_file_path).unlink()

    def test_get_private_key_content_no_key_provided(self, basic_config):
        """Test error when no private key is provided."""
        with pytest.raises(ValueError, match="No private key provided"):
            basic_config._get_private_key_content()

    def test_get_private_key_content_file_not_found(self):
        """Test error when private key file doesn't exist."""
        config = SnowflakeConfig(
            account="test_account",
            username="test_user",
            private_key_path="/nonexistent/path",
            warehouse="test_warehouse",
            database="test_database",
            role="test_role",
        )
        with pytest.raises(FileNotFoundError):
            config._get_private_key_content()


class TestGetPrivateKeyBytes:
    """Tests for _get_private_key_bytes method."""

    def test_get_private_key_bytes_without_passphrase(self, private_key_config):
        """Test getting private key bytes without passphrase."""
        expected_key_bytes = _get_test_private_key_bytes(private_key_config.private_key)

        key_bytes = private_key_config._get_private_key_bytes()

        assert isinstance(key_bytes, bytes)
        assert key_bytes == expected_key_bytes

    def test_get_private_key_bytes_with_passphrase(
        self, private_key_with_passphrase_config
    ):
        """Test getting private key bytes with passphrase."""
        expected_key_bytes = _get_test_private_key_bytes(
            private_key_with_passphrase_config.private_key,
            private_key_with_passphrase_config.private_key_passphrase,
        )

        key_bytes = private_key_with_passphrase_config._get_private_key_bytes()

        assert isinstance(key_bytes, bytes)
        assert key_bytes == expected_key_bytes

    def test_get_private_key_bytes_invalid_key_format(self):
        """Test error with invalid private key format."""
        config = SnowflakeConfig(
            account="test_account",
            username="test_user",
            private_key=SecretString("not a private key"),
            warehouse="test_warehouse",
            database="test_database",
            role="test_role",
        )
        with pytest.raises(ValueError):
            config._get_private_key_bytes()

    def test_get_private_key_bytes_wrong_passphrase(self):
        """Test error with wrong passphrase."""
        passphrase = "test_passphrase"
        private_key_pem = _generate_test_private_key_with_passphrase(passphrase)

        config = SnowflakeConfig(
            account="test_account",
            username="test_user",
            private_key=SecretString(private_key_pem.decode("utf-8")),
            private_key_passphrase=SecretString("wrong_passphrase"),
            warehouse="test_warehouse",
            database="test_database",
            role="test_role",
        )
        with pytest.raises(ValueError):
            config._get_private_key_bytes()


class TestGetSqlAlchemyConnectArgs:
    """Tests for get_sql_alchemy_connect_args method."""

    def test_get_sql_alchemy_connect_args_no_private_key(self, password_config):
        """Test connect args when no private key is provided."""
        connect_args = password_config.get_sql_alchemy_connect_args()
        assert not connect_args

    def test_get_sql_alchemy_connect_args_with_private_key(self, private_key_config):
        """Test connect args when private key is provided."""
        expected_private_key_bytes = _get_test_private_key_bytes(
            private_key_config.private_key
        )

        connect_args = private_key_config.get_sql_alchemy_connect_args()

        assert "private_key" in connect_args
        assert isinstance(connect_args["private_key"], bytes)
        assert connect_args["private_key"] == expected_private_key_bytes

    def test_get_sql_alchemy_connect_args_with_private_key_path(self):
        """Test connect args when private key path is provided."""
        private_key_pem = _generate_test_private_key()
        expected_private_key_bytes = _get_test_private_key_bytes(
            SecretString(private_key_pem.decode("utf-8"))
        )

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
            temp_file.write(private_key_pem)
            temp_file_path = temp_file.name

        try:
            config = SnowflakeConfig(
                account="test_account",
                username="test_user",
                private_key_path=temp_file_path,
                warehouse="test_warehouse",
                database="test_database",
                role="test_role",
            )

            connect_args = config.get_sql_alchemy_connect_args()

            assert "private_key" in connect_args
            assert isinstance(connect_args["private_key"], bytes)
            assert connect_args["private_key"] == expected_private_key_bytes
        finally:
            Path(temp_file_path).unlink()


class TestGetSqlAlchemyUrl:
    """Tests for get_sql_alchemy_url method."""

    def test_get_sql_alchemy_url_with_password(self, password_config):
        """Test SQL Alchemy URL generation with password."""
        url = password_config.get_sql_alchemy_url()
        url_str = str(url)
        assert (
            url_str
            == "snowflake://test_user:test_password@test_account/test_database/airbyte_raw?role=test_role&warehouse=test_warehouse"
        )

    def test_get_sql_alchemy_url_without_password(self, private_key_config):
        """Test SQL Alchemy URL generation without password (key auth)."""
        url = private_key_config.get_sql_alchemy_url()
        url_str = str(url)
        assert (
            url_str
            == "snowflake://test_user:@test_account/test_database/airbyte_raw?role=test_role&warehouse=test_warehouse"
        )


class TestGetVendorClient:
    """Tests for get_vendor_client method."""

    @patch("airbyte._processors.sql.snowflake.connector.connect")
    def test_get_vendor_client_with_password(self, mock_connect, password_config):
        """Test vendor client creation with password authentication."""
        mock_client = Mock()
        mock_connect.return_value = mock_client

        client = password_config.get_vendor_client()

        mock_connect.assert_called_once_with(
            user="test_user",
            account="test_account",
            warehouse="test_warehouse",
            database="test_database",
            schema="airbyte_raw",
            role="test_role",
            password=password_config.password,
        )
        assert client == mock_client

    @patch("airbyte._processors.sql.snowflake.connector.connect")
    def test_get_vendor_client_with_private_key_path(self, mock_connect):
        """Test vendor client creation with private key path authentication."""
        mock_client = Mock()
        mock_connect.return_value = mock_client

        private_key_pem = _generate_test_private_key()
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
            temp_file.write(private_key_pem)
            temp_file_path = temp_file.name

        try:
            config = SnowflakeConfig(
                account="test_account",
                username="test_user",
                private_key_path=temp_file_path,
                private_key_passphrase=SecretString("test_passphrase"),
                warehouse="test_warehouse",
                database="test_database",
                role="test_role",
            )

            client = config.get_vendor_client()

            mock_connect.assert_called_once_with(
                user="test_user",
                account="test_account",
                warehouse="test_warehouse",
                database="test_database",
                schema="airbyte_raw",
                role="test_role",
                private_key_file=temp_file_path,
                private_key_file_pwd=config.private_key_passphrase,
                authenticator="SNOWFLAKE_JWT",
            )
            assert client == mock_client
        finally:
            Path(temp_file_path).unlink()

    @patch("airbyte._processors.sql.snowflake.connector.connect")
    def test_get_vendor_client_with_private_key_string(
        self, mock_connect, private_key_config
    ):
        """Test vendor client creation with private key string authentication."""
        mock_client = Mock()
        mock_connect.return_value = mock_client
        expected_private_key_bytes = _get_test_private_key_bytes(
            private_key_config.private_key
        )

        client = private_key_config.get_vendor_client()

        mock_connect.assert_called_once_with(
            user="test_user",
            account="test_account",
            warehouse="test_warehouse",
            database="test_database",
            schema="airbyte_raw",
            role="test_role",
            private_key=expected_private_key_bytes,
            authenticator="SNOWFLAKE_JWT",
        )
        assert client == mock_client


def _generate_test_private_key() -> bytes:
    """Generate a test private key for testing purposes."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _generate_test_private_key_with_passphrase(passphrase: str) -> bytes:
    """Generate a test private key with passphrase for testing purposes."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(passphrase.encode()),
    )


def _get_test_private_key_bytes(
    private_key: SecretString, passphrase: SecretString | None = None
):
    if passphrase:
        passphrase = str(passphrase).encode("utf-8")

    private_key_content = str(private_key).encode("utf-8")
    loaded_private_key = serialization.load_pem_private_key(
        private_key_content,
        password=passphrase,
        backend=default_backend(),
    )
    expected_key_bytes = loaded_private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return expected_key_bytes
