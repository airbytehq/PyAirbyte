# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for Integration class."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airbyte import exceptions as exc
from airbyte._executors.sonar import SonarExecutor
from airbyte.integrations.base import Integration
from airbyte.integrations.util import get_integration


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
    verbs: [list, get, create]
  - name: products
    verbs: [list, get]
""")
    return yaml_file


@pytest.fixture
def mock_executor(mock_yaml_path: Path) -> SonarExecutor:
    """Create a mock SonarExecutor."""
    return SonarExecutor(
        name="test-api",
        yaml_path=mock_yaml_path,
        secrets={"api_key": "test_key"},
    )


@pytest.fixture
def integration(mock_executor: SonarExecutor, mock_yaml_path: Path) -> Integration:
    """Create an Integration instance."""
    return Integration(
        executor=mock_executor,
        name="test-api",
        yaml_path=mock_yaml_path,
    )


class TestIntegration:
    """Tests for Integration class."""

    def test_init(self, integration: Integration, mock_yaml_path: Path) -> None:
        """Test Integration initialization."""
        assert integration.name == "test-api"
        assert integration.yaml_path == mock_yaml_path
        assert integration.connector_type == "integration"

    def test_init_with_nonexistent_yaml(self, mock_executor: SonarExecutor) -> None:
        """Test Integration initialization with nonexistent YAML."""
        integration = Integration(
            executor=mock_executor,
            name="test-api",
            yaml_path="/nonexistent/path.yaml",
        )
        assert integration.yaml_path == Path("/nonexistent/path.yaml")

    @patch("connector_sdk.config_loader.load_connector_config")
    def test_validate_yaml_success(
        self,
        mock_load: MagicMock,
        integration: Integration,
    ) -> None:
        """Test YAML validation success."""
        mock_load.return_value = MagicMock()
        integration._validate_yaml()
        mock_load.assert_called_once()

    @patch("connector_sdk.config_loader.load_connector_config")
    def test_validate_yaml_import_error(
        self,
        mock_load: MagicMock,
        integration: Integration,
    ) -> None:
        """Test YAML validation with missing connector-sdk."""
        mock_load.side_effect = ImportError("No module named 'connector_sdk'")

        with pytest.raises(exc.PyAirbyteInputError) as excinfo:
            integration._validate_yaml()

        assert "connector-sdk is required" in str(excinfo.value)

    @patch("connector_sdk.config_loader.load_connector_config")
    def test_validate_yaml_invalid(
        self,
        mock_load: MagicMock,
        integration: Integration,
    ) -> None:
        """Test YAML validation with invalid YAML."""
        mock_load.side_effect = ValueError("Invalid YAML")

        with pytest.raises(exc.PyAirbyteInputError) as excinfo:
            integration._validate_yaml()

        assert "Invalid connector YAML" in str(excinfo.value)

    @patch("connector_sdk.config_loader.load_connector_config")
    def test_list_resources(
        self,
        mock_load: MagicMock,
        integration: Integration,
    ) -> None:
        """Test listing resources."""
        mock_config = MagicMock()
        mock_resource1 = MagicMock()
        mock_resource1.name = "customers"
        mock_resource2 = MagicMock()
        mock_resource2.name = "products"
        mock_config.resources = [mock_resource1, mock_resource2]
        mock_load.return_value = mock_config

        resources = integration.list_resources()

        assert resources == ["customers", "products"]
        assert integration._resources == ["customers", "products"]

    @patch("connector_sdk.config_loader.load_connector_config")
    def test_list_verbs(
        self,
        mock_load: MagicMock,
        integration: Integration,
    ) -> None:
        """Test listing verbs for a resource."""
        mock_config = MagicMock()
        mock_resource = MagicMock()
        mock_resource.name = "customers"
        mock_verb1 = MagicMock()
        mock_verb1.value = "list"
        mock_verb2 = MagicMock()
        mock_verb2.value = "get"
        mock_resource.verbs = [mock_verb1, mock_verb2]
        mock_config.resources = [mock_resource]
        mock_load.return_value = mock_config

        verbs = integration.list_verbs("customers")

        assert verbs == ["list", "get"]

    @patch("connector_sdk.config_loader.load_connector_config")
    def test_list_verbs_resource_not_found(
        self,
        mock_load: MagicMock,
        integration: Integration,
    ) -> None:
        """Test listing verbs for nonexistent resource."""
        mock_config = MagicMock()
        mock_resource = MagicMock()
        mock_resource.name = "customers"
        mock_config.resources = [mock_resource]
        mock_load.return_value = mock_config

        with pytest.raises(exc.PyAirbyteInputError) as excinfo:
            integration.list_verbs("nonexistent")

        assert "Resource 'nonexistent' not found" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_aexecute(self, integration: Integration) -> None:
        """Test async execute."""
        mock_response = {"id": "123", "name": "Test"}

        with patch.object(
            integration.executor,
            "aexecute",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_aexecute:
            result = await integration.aexecute(
                "customers",
                "get",
                params={"id": "123"},
            )

            assert result == mock_response
            mock_aexecute.assert_called_once_with(
                "customers",
                "get",
                {"id": "123"},
            )

    def test_execute(self, integration: Integration) -> None:
        """Test sync execute."""
        mock_response = {"id": "123", "name": "Test"}

        with patch.object(
            integration,
            "aexecute",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_aexecute:
            result = integration.execute(
                "customers",
                "get",
                params={"id": "123"},
            )

            assert result == mock_response
            mock_aexecute.assert_called_once_with(
                "customers",
                "get",
                {"id": "123"},
            )

    @pytest.mark.asyncio
    async def test_aexecute_batch(self, integration: Integration) -> None:
        """Test async batch execute."""
        mock_responses = [
            {"id": "1", "name": "Customer 1"},
            {"id": "2", "name": "Customer 2"},
        ]

        with patch.object(
            integration.executor,
            "aexecute_batch",
            new_callable=AsyncMock,
            return_value=mock_responses,
        ) as mock_batch:
            operations = [
                ("customers", "get", {"id": "1"}),
                ("customers", "get", {"id": "2"}),
            ]
            results = await integration.aexecute_batch(operations)

            assert results == mock_responses
            mock_batch.assert_called_once_with(operations)

    def test_execute_batch(self, integration: Integration) -> None:
        """Test sync batch execute."""
        mock_responses = [
            {"id": "1", "name": "Customer 1"},
            {"id": "2", "name": "Customer 2"},
        ]

        with patch.object(
            integration,
            "aexecute_batch",
            new_callable=AsyncMock,
            return_value=mock_responses,
        ) as mock_batch:
            operations = [
                ("customers", "get", {"id": "1"}),
                ("customers", "get", {"id": "2"}),
            ]
            results = integration.execute_batch(operations)

            assert results == mock_responses
            mock_batch.assert_called_once_with(operations)

    def test_check_not_implemented(self, integration: Integration) -> None:
        """Test check raises NotImplementedError when no authorize verb."""
        with patch.object(integration, "list_resources", return_value=["customers"]):
            with patch.object(integration, "list_verbs", return_value=["list", "get"]):
                with pytest.raises(NotImplementedError) as excinfo:
                    integration.check()

                assert "does not define a health check mechanism" in str(excinfo.value)

    def test_check_with_authorize_verb(self, integration: Integration) -> None:
        """Test check succeeds when authorize verb exists."""
        with patch.object(integration, "list_resources", return_value=["auth"]):
            with patch.object(integration, "list_verbs", return_value=["authorize"]):
                with patch.object(
                    integration,
                    "execute",
                    return_value={"status": "ok"},
                ) as mock_execute:
                    integration.check()
                    mock_execute.assert_called_once_with("auth", "authorize", params={})


class TestGetIntegration:
    """Tests for get_integration factory function."""

    def test_get_integration(self, mock_yaml_path: Path) -> None:
        """Test get_integration factory."""
        integration = get_integration(
            name="test-api",
            yaml_path=mock_yaml_path,
            secrets={"api_key": "test_key"},
        )

        assert isinstance(integration, Integration)
        assert integration.name == "test-api"
        assert integration.yaml_path == mock_yaml_path
        assert isinstance(integration.executor, SonarExecutor)

    def test_get_integration_with_logging(self, mock_yaml_path: Path) -> None:
        """Test get_integration with logging enabled."""
        integration = get_integration(
            name="test-api",
            yaml_path=mock_yaml_path,
            secrets={"api_key": "test_key"},
            enable_logging=True,
            log_file="/tmp/test.log",
        )

        assert isinstance(integration, Integration)
        assert integration.executor.enable_logging is True
        assert integration.executor.log_file == "/tmp/test.log"

    def test_get_integration_with_validation(self, mock_yaml_path: Path) -> None:
        """Test get_integration with validation."""
        with patch("connector_sdk.config_loader.load_connector_config"):
            integration = get_integration(
                name="test-api",
                yaml_path=mock_yaml_path,
                secrets={"api_key": "test_key"},
                validate=True,
            )

            assert isinstance(integration, Integration)
