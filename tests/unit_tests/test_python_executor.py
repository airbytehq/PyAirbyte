from unittest.mock import Mock, patch
import requests

from airbyte._executors.python import VenvExecutor, _get_pypi_python_requirements_cached
from airbyte._util.semver import check_python_version_compatibility


class TestGetPypiPythonRequirementsCached:
    """Test the _get_pypi_python_requirements_cached function."""

    def test_offline_mode_returns_none(self):
        """Test that offline mode returns None without making network calls."""
        with patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", True):
            _get_pypi_python_requirements_cached.cache_clear()
            result = _get_pypi_python_requirements_cached("airbyte-source-hubspot")
            assert result is None

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_successful_api_response(self, mock_get):
        """Test successful PyPI API response returns requires_python."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"info": {"requires_python": "<3.12,>=3.10"}}
        mock_get.return_value = mock_response

        _get_pypi_python_requirements_cached.cache_clear()
        result = _get_pypi_python_requirements_cached("airbyte-source-hubspot")

        assert result == "<3.12,>=3.10"
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert (
            call_args[1]["url"] == "https://pypi.org/pypi/airbyte-source-hubspot/json"
        )
        assert call_args[1]["timeout"] == 10

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_package_not_found_returns_none(self, mock_get):
        """Test that 404 response returns None."""
        mock_response = Mock()
        mock_response.ok = False
        mock_get.return_value = mock_response

        _get_pypi_python_requirements_cached.cache_clear()
        result = _get_pypi_python_requirements_cached("nonexistent-package")

        assert result is None

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_json_parsing_error_returns_none(self, mock_get):
        """Test that JSON parsing errors return None."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        _get_pypi_python_requirements_cached.cache_clear()
        result = _get_pypi_python_requirements_cached("test-package")

        assert result is None

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_missing_requires_python_field_returns_none(self, mock_get):
        """Test that missing requires_python field returns None."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"info": {"name": "test-package"}}
        mock_get.return_value = mock_response

        _get_pypi_python_requirements_cached.cache_clear()
        result = _get_pypi_python_requirements_cached("test-package")

        assert result is None

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_network_timeout_returns_none(self, mock_get):
        """Test that network timeouts return None."""
        mock_get.side_effect = requests.exceptions.Timeout()

        _get_pypi_python_requirements_cached.cache_clear()
        result = _get_pypi_python_requirements_cached("test-package")

        assert result is None

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_caching_behavior(self, mock_get):
        """Test that lru_cache works correctly."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"info": {"requires_python": ">=3.8"}}
        mock_get.return_value = mock_response

        _get_pypi_python_requirements_cached.cache_clear()

        result1 = _get_pypi_python_requirements_cached("test-package")
        result2 = _get_pypi_python_requirements_cached("test-package")

        assert result1 == ">=3.8"
        assert result2 == ">=3.8"
        mock_get.assert_called_once()

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_user_agent_header(self, mock_get):
        """Test that proper User-Agent header is sent."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"info": {"requires_python": ">=3.8"}}
        mock_get.return_value = mock_response

        with patch("airbyte._executors.python.get_version", return_value="1.0.0"):
            _get_pypi_python_requirements_cached.cache_clear()
            _get_pypi_python_requirements_cached("test-package")

        call_args = mock_get.call_args
        assert "PyAirbyte/1.0.0" in call_args[1]["headers"]["User-Agent"]

    @patch("airbyte._executors.python.AIRBYTE_OFFLINE_MODE", False)
    @patch("requests.get")
    def test_user_agent_header_no_version(self, mock_get):
        """Test User-Agent header when version is None."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"info": {"requires_python": ">=3.8"}}
        mock_get.return_value = mock_response

        with patch("airbyte._executors.python.get_version", return_value=None):
            _get_pypi_python_requirements_cached.cache_clear()
            _get_pypi_python_requirements_cached("test-package")

        call_args = mock_get.call_args
        assert call_args[1]["headers"]["User-Agent"] == "PyAirbyte"


class TestVenvExecutorVersionCompatibility:
    """Test version compatibility checking in VenvExecutor."""

    def test_check_python_version_compatibility_no_requirements(self):
        """Test that None requirements return None."""
        result = check_python_version_compatibility("test-package", None)
        assert result is None

    @patch("airbyte._util.semver.warn_once")
    def test_check_python_version_compatibility_incompatible(self, mock_warn):
        """Test warning for incompatible Python version."""
        mock_version_info = Mock()
        mock_version_info.major = 3
        mock_version_info.minor = 13
        mock_version_info.micro = 0

        with patch("sys.version_info", mock_version_info):
            result = check_python_version_compatibility(
                "test-package", "<3.12,>=3.10"
            )

        assert result is False
        mock_warn.assert_called_once()
        warning_message = mock_warn.call_args[0][0]
        assert "Python version compatibility warning" in warning_message
        assert "test-package" in warning_message
        assert "3.13.0" in warning_message
        assert "<3.12,>=3.10" in warning_message

    def test_check_python_version_compatibility_compatible(self):
        """Test compatible Python version returns True."""
        mock_version_info = Mock()
        mock_version_info.major = 3
        mock_version_info.minor = 11
        mock_version_info.micro = 5

        with patch("sys.version_info", mock_version_info):
            result = check_python_version_compatibility(
                "test-package", "<3.12,>=3.10"
            )

        assert result is True
