# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path

import pytest

from airbyte.settings import Settings, _str_to_bool


_SETTING_ENV_VARS = (
    "AIRBYTE_PROJECT_DIR",
    "AIRBYTE_INSTALL_DIR",
    "AIRBYTE_CACHE_ROOT",
    "AIRBYTE_TEMP_DIR",
    "AIRBYTE_TEMP_FILE_CLEANUP",
    "AIRBYTE_OFFLINE_MODE",
    "AIRBYTE_PRINT_FULL_ERROR_LOGS",
    "AIRBYTE_NO_UV",
    "CI",
)


@pytest.fixture(autouse=True)
def clear_settings_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _SETTING_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("", False, id="empty"),
        pytest.param("0", False, id="zero"),
        pytest.param("false", False, id="false"),
        pytest.param("FALSE", False, id="uppercase-false"),
        pytest.param("f", False, id="f"),
        pytest.param("no", False, id="no"),
        pytest.param("n", False, id="n"),
        pytest.param("off", False, id="off"),
        pytest.param("yes", True, id="yes"),
        pytest.param("random", True, id="random"),
    ],
)
def test_str_to_bool_preserves_legacy_truthiness(value: str, expected: bool) -> None:
    assert _str_to_bool(value) is expected


def test_missing_config_files_use_defaults(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    settings = Settings()

    assert settings.project_dir is None
    assert settings.cache_root is None
    assert settings.temp_file_cleanup is True
    assert settings.offline_mode is False
    assert settings.print_full_error_logs is False


def test_config_file_values_are_loaded_and_environment_wins(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "airbyte.yaml").write_text(
        "offline_mode: false\ncache_root: /from-file\nprint_full_error_logs: false\n"
    )
    monkeypatch.setenv("AIRBYTE_OFFLINE_MODE", "true")
    monkeypatch.setenv("AIRBYTE_CACHE_ROOT", "/from-env")

    settings = Settings()

    assert settings.offline_mode is True
    assert settings.cache_root == Path("/from-env")
    assert settings.print_full_error_logs is False


def test_toml_config_file_values_are_loaded(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "airbyte.toml").write_text(
        'offline_mode = true\ncache_root = "/from-toml"\n'
    )

    settings = Settings()

    assert settings.offline_mode is True
    assert settings.cache_root == Path("/from-toml")


def test_ci_controls_print_full_error_logs_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CI", "yes")

    assert Settings().print_full_error_logs is True


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("1", False, id="one"),
        pytest.param("true", False, id="true"),
        pytest.param("yes", False, id="yes"),
        pytest.param("false", True, id="false"),
        pytest.param("", True, id="empty"),
    ],
)
def test_no_uv_preserves_inverted_environment_behavior(
    value: str,
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_NO_UV", value)

    assert Settings().no_uv is expected


def test_empty_temp_dir_is_none_and_non_empty_value_is_a_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_TEMP_DIR", "")
    assert Settings().temp_dir is None

    monkeypatch.setenv("AIRBYTE_TEMP_DIR", "/tmp/airbyte")
    assert Settings().temp_dir == Path("/tmp/airbyte")
