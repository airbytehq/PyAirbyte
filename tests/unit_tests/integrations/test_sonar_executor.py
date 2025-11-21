# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for SonarExecutor class."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airbyte import exceptions as exc
from airbyte._executors.sonar import SonarExecutor


@pytest.fixture
def mock_yaml_path(tmp_path: Path) -> Path:
    """Create a mock YAML file."""
    yaml_file = tmp_path / "test-connector.yaml"
    yaml_file.write_text("""
connector:
  name: test-api
  version: 1.0.0
resources:
  - name: customers
    verbs: [list, get]
""")
    return yaml_file


@pytest.fixture
def executor(mock_yaml_path: Path) -> SonarExecutor:
    """Create a SonarExecutor instance."""
    return SonarExecutor(
        name="test-api",
        yaml_path=mock_yaml_path,
        secrets={"api_key": "test_key"},
    )


class TestSonarExecutor:
    """Tests for SonarExecutor class."""

    def test_init(self, executor: SonarExecutor, mock_yaml_path: Path) -> None:
        """Test SonarExecutor initialization."""
        assert executor.name == "test-api"
        assert executor.yaml_path == mock_yaml_path
        assert executor.secrets == {"api_key": "test_key"}
        assert executor.enable_logging is False
        assert executor.log_file is None
        assert executor._executor is None

    def test_init_with_nonexistent_yaml(self) -> None:
        """Test SonarExecutor initialization with nonexistent YAML."""
        with pytest.raises(exc.PyAirbyteInputError) as excinfo:
            SonarExecutor(
                name="test-api",
                yaml_path="/nonexistent/path.yaml",
                secrets={},
            )

        assert "Connector YAML file not found" in str(excinfo.value)

    def test_init_with_logging(self, mock_yaml_path: Path) -> None:
        """Test SonarExecutor initialization with logging."""
        executor = SonarExecutor(
            name="test-api",
            yaml_path=mock_yaml_path,
            secrets={},
            enable_logging=True,
            log_file="/tmp/test.log",
        )

        assert executor.enable_logging is True
        assert executor.log_file == "/tmp/test.log"

    @patch("connector_sdk.ConnectorExecutor")
    def test_get_executor(
        self,
        mock_connector_executor: MagicMock,
        executor: SonarExecutor,
    ) -> None:
        """Test lazy executor creation."""
        mock_executor_instance = MagicMock()
        mock_connector_executor.return_value = mock_executor_instance

        result = executor._get_executor()

        assert result == mock_executor_instance
        assert executor._executor == mock_executor_instance
        mock_connector_executor.assert_called_once()

    def test_get_executor_import_error(self, executor: SonarExecutor) -> None:
        """Test executor creation with missing connector-sdk."""
        with patch("connector_sdk.ConnectorExecutor") as mock:
            mock.side_effect = ImportError("No module named 'connector_sdk'")

            with patch.dict("sys.modules", {"connector_sdk": None}):
                with pytest.raises(exc.PyAirbyteInputError) as excinfo:
                    executor._get_executor()

                assert "connector-sdk is required" in str(excinfo.value)

    def test_cli_property(self, executor: SonarExecutor) -> None:
        """Test _cli property returns empty list."""
        assert executor._cli == []

    def test_execute_not_supported(self, executor: SonarExecutor) -> None:
        """Test execute raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as excinfo:
            list(executor.execute(["spec"]))

        assert "do not support subprocess execution" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_aexecute(self, executor: SonarExecutor) -> None:
        """Test async execute."""
        mock_response = {"id": "123", "name": "Test"}
        mock_executor_instance = MagicMock()
        mock_executor_instance.execute = AsyncMock(return_value=mock_response)

        with patch.object(
            executor, "_get_executor", return_value=mock_executor_instance
        ):
            result = await executor.aexecute("customers", "get", {"id": "123"})

            assert result == mock_response
            mock_executor_instance.execute.assert_called_once_with(
                "customers",
                "get",
                {"id": "123"},
            )

    @pytest.mark.asyncio
    async def test_aexecute_batch(self, executor: SonarExecutor) -> None:
        """Test async batch execute."""
        mock_responses = [{"id": "1"}, {"id": "2"}]
        mock_executor_instance = MagicMock()
        mock_executor_instance.execute_batch = AsyncMock(return_value=mock_responses)

        with patch.object(
            executor, "_get_executor", return_value=mock_executor_instance
        ):
            operations = [
                ("customers", "get", {"id": "1"}),
                ("customers", "get", {"id": "2"}),
            ]
            results = await executor.aexecute_batch(operations)

            assert results == mock_responses
            mock_executor_instance.execute_batch.assert_called_once_with(operations)

    def test_ensure_installation(self, executor: SonarExecutor) -> None:
        """Test ensure_installation is a no-op."""
        executor.ensure_installation()  # Should not raise

    def test_install(self, executor: SonarExecutor) -> None:
        """Test install is a no-op."""
        executor.install()  # Should not raise

    def test_uninstall(self, executor: SonarExecutor) -> None:
        """Test uninstall is a no-op."""
        executor.uninstall()  # Should not raise

    @patch("airbyte._executors.sonar.load_connector_config")
    def test_get_installed_version(
        self,
        mock_load: MagicMock,
        executor: SonarExecutor,
    ) -> None:
        """Test get_installed_version."""
        mock_config = MagicMock()
        mock_config.connector.version = "1.0.0"
        mock_load.return_value = mock_config

        version = executor.get_installed_version()

        assert version == "1.0.0"

    @patch("airbyte._executors.sonar.load_connector_config")
    def test_get_installed_version_no_connector(
        self,
        mock_load: MagicMock,
        executor: SonarExecutor,
    ) -> None:
        """Test get_installed_version with no connector metadata."""
        mock_config = MagicMock()
        mock_config.connector = None
        mock_load.return_value = mock_config

        version = executor.get_installed_version()

        assert version is None

    @patch("airbyte._executors.sonar.load_connector_config")
    def test_get_installed_version_error(
        self,
        mock_load: MagicMock,
        executor: SonarExecutor,
    ) -> None:
        """Test get_installed_version with error."""
        mock_load.side_effect = Exception("Load failed")

        version = executor.get_installed_version()

        assert version is None

    @patch("airbyte._executors.sonar.load_connector_config")
    def test_get_installed_version_raise_on_error(
        self,
        mock_load: MagicMock,
        executor: SonarExecutor,
    ) -> None:
        """Test get_installed_version with raise_on_error."""
        mock_load.side_effect = Exception("Load failed")

        with pytest.raises(Exception) as excinfo:
            executor.get_installed_version(raise_on_error=True)

        assert "Load failed" in str(excinfo.value)
