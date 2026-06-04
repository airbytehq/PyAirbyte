from __future__ import annotations

import logging
import warnings

import pytest
from unittest.mock import patch

from airbyte.logs import (
    LoggingBehavior,
    _parse_logging_behavior,
    get_global_file_logger,
    get_global_stats_logger,
    new_passthrough_file_logger,
)

connector_name = "test_connector"


def clear_logger_caches():
    get_global_file_logger.cache_clear()
    get_global_stats_logger.cache_clear()
    logging.getLogger("airbyte").handlers.clear()
    logging.getLogger("airbyte.stats").handlers.clear()
    logging.getLogger(f"airbyte.{connector_name}").handlers.clear()


class TestParseLoggingBehavior:
    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("FILE_ONLY", LoggingBehavior.FILE_ONLY),
            ("file_only", LoggingBehavior.FILE_ONLY),
            ("File_Only", LoggingBehavior.FILE_ONLY),
            ("CONSOLE_ONLY", LoggingBehavior.CONSOLE_ONLY),
            ("console_only", LoggingBehavior.CONSOLE_ONLY),
            ("Console_Only", LoggingBehavior.CONSOLE_ONLY),
            ("FILE_AND_CONSOLE", LoggingBehavior.FILE_AND_CONSOLE),
            ("file_and_console", LoggingBehavior.FILE_AND_CONSOLE),
            ("File_And_Console", LoggingBehavior.FILE_AND_CONSOLE),
            ("", LoggingBehavior.FILE_ONLY),
            ("FILE", LoggingBehavior.FILE_ONLY),
            ("INVALID", LoggingBehavior.FILE_ONLY),
            ("CONSOLE", LoggingBehavior.FILE_ONLY),
            ("FILE_ONLY_EXTRA", LoggingBehavior.FILE_ONLY),
        ],
    )
    def test_logging_behavior_parsing(self, input_value, expected):
        result = _parse_logging_behavior(input_value)
        assert result == expected


class TestGetGlobalFileLogger:
    """Test the get_global_file_logger function."""

    def setup_method(self):
        clear_logger_caches()

    def test_logger_creation_with_defaults(self):
        with (
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = get_global_file_logger()

            assert logger is not None
            assert logger.name == "airbyte"
            assert len(logger.handlers) == 1
            assert isinstance(logger.handlers[0], logging.FileHandler)
            assert (
                logger.handlers[0].formatter._fmt
                == "%(asctime)s - %(levelname)s - %(message)s"
            )

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,formatter_fmt,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, "%(message)s", [logging.FileHandler]),
            (
                LoggingBehavior.FILE_ONLY,
                False,
                "%(asctime)s - %(levelname)s - %(message)s",
                [logging.FileHandler],
            ),
            (
                LoggingBehavior.CONSOLE_ONLY,
                True,
                "%(message)s",
                [logging.StreamHandler],
            ),
            (
                LoggingBehavior.CONSOLE_ONLY,
                False,
                "%(asctime)s - %(levelname)s - %(message)s",
                [logging.StreamHandler],
            ),
            (
                LoggingBehavior.FILE_AND_CONSOLE,
                True,
                "%(message)s",
                [logging.FileHandler, logging.StreamHandler],
            ),
            (
                LoggingBehavior.FILE_AND_CONSOLE,
                False,
                "%(asctime)s - %(levelname)s - %(message)s",
                [logging.FileHandler, logging.StreamHandler],
            ),
        ],
    )
    def test_logger_creation_with_structured_logging(
        self,
        airbyte_logging_behavior,
        airbyte_structured_logging,
        formatter_fmt,
        handlers,
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = get_global_file_logger()
            assert logger is not None
            assert logger.name == "airbyte"
            assert len(logger.handlers) == len(handlers)
            for idx, handler_type in enumerate(handlers):
                assert isinstance(logger.handlers[idx], handler_type)
                assert logger.handlers[idx].formatter._fmt == formatter_fmt

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, []),
            (LoggingBehavior.FILE_ONLY, False, []),
            (LoggingBehavior.CONSOLE_ONLY, True, [logging.StreamHandler]),
            (LoggingBehavior.CONSOLE_ONLY, False, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, True, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, False, [logging.StreamHandler]),
        ],
    )
    def test_directory_creation_failure(
        self, airbyte_logging_behavior, airbyte_structured_logging, handlers
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("pathlib.Path.mkdir", side_effect=OSError("Permission denied")),
            patch("pathlib.Path.exists", return_value=False),
            warnings.catch_warnings(),
        ):
            warnings.simplefilter("ignore", UserWarning)
            logger = get_global_file_logger()
            if len(handlers) == 0:
                assert logger is None
            else:
                assert logger is not None
                assert len(logger.handlers) == len(handlers)
                for idx, handler_type in enumerate(handlers):
                    assert isinstance(logger.handlers[idx], handler_type)

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, []),
            (LoggingBehavior.FILE_ONLY, False, []),
            (LoggingBehavior.CONSOLE_ONLY, True, [logging.StreamHandler]),
            (LoggingBehavior.CONSOLE_ONLY, False, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, True, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, False, [logging.StreamHandler]),
        ],
    )
    def test_logger_creation_with_logging_root_none(
        self, airbyte_logging_behavior, airbyte_structured_logging, handlers
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("airbyte.logs.AIRBYTE_LOGGING_ROOT", None),
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = get_global_file_logger()
            if len(handlers) == 0:
                assert logger is None
            else:
                assert logger is not None
                assert len(logger.handlers) == len(handlers)
                for idx, handler_type in enumerate(handlers):
                    assert isinstance(logger.handlers[idx], handler_type)


class TestGetGlobalStatsLogger:
    """Test the get_global_stats_logger function."""

    expected_fmt = "%(message)s"

    def setup_method(self):
        clear_logger_caches()

    def test_logger_creation_with_defaults(self):
        with (
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = get_global_stats_logger()

            assert logger is not None
            assert logger.name == "airbyte.stats"
            assert len(logger.handlers) == 1
            assert isinstance(logger.handlers[0], logging.FileHandler)
            assert logger.handlers[0].formatter._fmt == self.expected_fmt

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, [logging.FileHandler]),
            (LoggingBehavior.FILE_ONLY, False, [logging.FileHandler]),
            (LoggingBehavior.CONSOLE_ONLY, True, [logging.StreamHandler]),
            (LoggingBehavior.CONSOLE_ONLY, False, [logging.StreamHandler]),
            (
                LoggingBehavior.FILE_AND_CONSOLE,
                True,
                [logging.FileHandler, logging.StreamHandler],
            ),
            (
                LoggingBehavior.FILE_AND_CONSOLE,
                False,
                [logging.FileHandler, logging.StreamHandler],
            ),
        ],
    )
    def test_logger_creation_with_structured_logging(
        self, airbyte_logging_behavior, airbyte_structured_logging, handlers
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = get_global_stats_logger()
            assert logger is not None
            assert logger.name == "airbyte.stats"
            assert len(logger.handlers) == len(handlers)
            for idx, handler_type in enumerate(handlers):
                assert isinstance(logger.handlers[idx], handler_type)
                assert logger.handlers[idx].formatter._fmt == self.expected_fmt

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, []),
            (LoggingBehavior.FILE_ONLY, False, []),
            (LoggingBehavior.CONSOLE_ONLY, True, [logging.StreamHandler]),
            (LoggingBehavior.CONSOLE_ONLY, False, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, True, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, False, [logging.StreamHandler]),
        ],
    )
    def test_directory_creation_failure(
        self, airbyte_logging_behavior, airbyte_structured_logging, handlers
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("pathlib.Path.mkdir", side_effect=OSError("Permission denied")),
            patch("pathlib.Path.exists", return_value=False),
            warnings.catch_warnings(),
        ):
            warnings.simplefilter("ignore", UserWarning)
            logger = get_global_stats_logger()
            assert logger is not None
            assert len(logger.handlers) == len(handlers)
            for idx, handler_type in enumerate(handlers):
                assert isinstance(logger.handlers[idx], handler_type)

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, []),
            (LoggingBehavior.FILE_ONLY, False, []),
            (LoggingBehavior.CONSOLE_ONLY, True, [logging.StreamHandler]),
            (LoggingBehavior.CONSOLE_ONLY, False, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, True, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, False, [logging.StreamHandler]),
        ],
    )
    def test_logger_creation_with_logging_root_none(
        self, airbyte_logging_behavior, airbyte_structured_logging, handlers
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("airbyte.logs.AIRBYTE_LOGGING_ROOT", None),
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = get_global_stats_logger()
            assert logger is not None
            assert len(logger.handlers) == len(handlers)
            for idx, handler_type in enumerate(handlers):
                assert isinstance(logger.handlers[idx], handler_type)


class TestNewPassthroughFileLogger:
    """Test the new_passthrough_file_logger function."""

    def setup_method(self):
        clear_logger_caches()

    def test_logger_creation_with_defaults(self):
        with (
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = new_passthrough_file_logger(connector_name)

            assert logger is not None
            assert logger.name == f"airbyte.{connector_name}"
            assert len(logger.handlers) == 1
            assert isinstance(logger.handlers[0], logging.FileHandler)
            assert (
                logger.handlers[0].formatter._fmt
                == "%(asctime)s - %(levelname)s - %(message)s"
            )

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,formatter_fmt,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, "%(message)s", [logging.FileHandler]),
            (
                LoggingBehavior.FILE_ONLY,
                False,
                "%(asctime)s - %(levelname)s - %(message)s",
                [logging.FileHandler],
            ),
            (
                LoggingBehavior.CONSOLE_ONLY,
                True,
                "%(message)s",
                [logging.StreamHandler],
            ),
            (
                LoggingBehavior.CONSOLE_ONLY,
                False,
                "%(asctime)s - %(levelname)s - %(message)s",
                [logging.StreamHandler],
            ),
            (
                LoggingBehavior.FILE_AND_CONSOLE,
                True,
                "%(message)s",
                [logging.FileHandler, logging.StreamHandler],
            ),
            (
                LoggingBehavior.FILE_AND_CONSOLE,
                False,
                "%(asctime)s - %(levelname)s - %(message)s",
                [logging.FileHandler, logging.StreamHandler],
            ),
        ],
    )
    def test_logger_creation_with_structured_logging(
        self,
        airbyte_logging_behavior,
        airbyte_structured_logging,
        formatter_fmt,
        handlers,
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = new_passthrough_file_logger(connector_name)
            assert logger is not None
            assert logger.name == f"airbyte.{connector_name}"
            assert len(logger.handlers) == len(handlers)
            for idx, handler_type in enumerate(handlers):
                assert isinstance(logger.handlers[idx], handler_type)
                assert logger.handlers[idx].formatter._fmt == formatter_fmt

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, None),
            (LoggingBehavior.FILE_ONLY, False, None),
            (LoggingBehavior.CONSOLE_ONLY, True, [logging.StreamHandler]),
            (LoggingBehavior.CONSOLE_ONLY, False, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, True, None),
            (LoggingBehavior.FILE_AND_CONSOLE, False, None),
        ],
    )
    def test_directory_creation_failure(
        self, airbyte_logging_behavior, airbyte_structured_logging, handlers
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("pathlib.Path.mkdir", side_effect=OSError("Permission denied")),
            patch("pathlib.Path.exists", return_value=False),
        ):
            if handlers is None:
                with pytest.raises(OSError):
                    new_passthrough_file_logger(connector_name)
            else:
                logger = new_passthrough_file_logger(connector_name)
                assert logger is not None
                assert len(logger.handlers) == len(handlers)
                for idx, handler_type in enumerate(handlers):
                    assert isinstance(logger.handlers[idx], handler_type)

    @pytest.mark.parametrize(
        "airbyte_logging_behavior,airbyte_structured_logging,handlers",
        [
            (LoggingBehavior.FILE_ONLY, True, []),
            (LoggingBehavior.FILE_ONLY, False, []),
            (LoggingBehavior.CONSOLE_ONLY, True, [logging.StreamHandler]),
            (LoggingBehavior.CONSOLE_ONLY, False, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, True, [logging.StreamHandler]),
            (LoggingBehavior.FILE_AND_CONSOLE, False, [logging.StreamHandler]),
        ],
    )
    def test_logger_creation_with_logging_root_none(
        self, airbyte_logging_behavior, airbyte_structured_logging, handlers
    ):
        with (
            patch("airbyte.logs.AIRBYTE_LOGGING_BEHAVIOR", airbyte_logging_behavior),
            patch(
                "airbyte.logs.AIRBYTE_STRUCTURED_LOGGING", airbyte_structured_logging
            ),
            patch("airbyte.logs.AIRBYTE_LOGGING_ROOT", None),
            patch("pathlib.Path.mkdir"),
            patch("pathlib.Path.exists", return_value=True),
            patch("builtins.open", create=True),
        ):
            logger = new_passthrough_file_logger(connector_name)
            assert logger is not None
            assert len(logger.handlers) == len(handlers)
            for idx, handler_type in enumerate(handlers):
                assert isinstance(logger.handlers[idx], handler_type)
