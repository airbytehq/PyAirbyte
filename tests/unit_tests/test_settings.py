# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from airbyte import constants
from airbyte.settings import AirbyteCloudSettings, AirbyteSettings


_CLOUD_SETTINGS_ENV_VARS = (
    constants.AIRBYTE_CLIENT_ID_ENV_VAR,
    constants.AIRBYTE_CLIENT_SECRET_ENV_VAR,
    constants.AIRBYTE_BEARER_TOKEN_ENV_VAR,
    constants.AIRBYTE_WORKSPACE_ID_ENV_VAR,
    constants.AIRBYTE_ORGANIZATION_ID_ENV_VAR,
    constants.CLOUD_CLIENT_ID_ENV_VAR,
    constants.CLOUD_CLIENT_SECRET_ENV_VAR,
    constants.CLOUD_BEARER_TOKEN_ENV_VAR,
    constants.CLOUD_WORKSPACE_ID_ENV_VAR,
    constants.CLOUD_ORGANIZATION_ID_ENV_VAR,
    constants.CLOUD_API_ROOT_ENV_VAR,
    constants.CLOUD_CONFIG_API_ROOT_ENV_VAR,
)


@pytest.fixture(autouse=True)
def clear_cloud_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in _CLOUD_SETTINGS_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        pytest.param(None, False, id="unset"),
        pytest.param("1", True, id="one"),
        pytest.param("true", True, id="lowercase-true"),
        pytest.param("TRUE", True, id="uppercase-true"),
        pytest.param("YeS", True, id="mixed-case-yes"),
        pytest.param("on", True, id="on"),
        pytest.param("0", False, id="zero"),
        pytest.param("false", False, id="false"),
        pytest.param("no", False, id="no"),
        pytest.param("off", False, id="off"),
        pytest.param("", False, id="empty"),
    ],
)
def test_no_uv_environment_mapping(
    monkeypatch: pytest.MonkeyPatch,
    env_value: str | None,
    expected: bool,
) -> None:
    if env_value is None:
        monkeypatch.delenv("AIRBYTE_NO_UV", raising=False)
    else:
        monkeypatch.setenv("AIRBYTE_NO_UV", env_value)

    assert AirbyteSettings().no_uv is expected


def test_no_uv_rejects_unrecognized_boolean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRBYTE_NO_UV", "other")

    with pytest.raises(ValidationError):
        AirbyteSettings()


def test_directory_defaults_and_project_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    for variable in (
        "AIRBYTE_PROJECT_DIR",
        "AIRBYTE_INSTALL_DIR",
        "AIRBYTE_CACHE_ROOT",
    ):
        monkeypatch.delenv(variable, raising=False)

    settings = AirbyteSettings()
    assert settings.project_dir == tmp_path
    assert settings.install_dir == tmp_path
    assert settings.cache_root == tmp_path / ".cache"

    project_dir = tmp_path / "custom-project"
    monkeypatch.setenv("AIRBYTE_PROJECT_DIR", str(project_dir))
    settings = AirbyteSettings()
    assert settings.project_dir == project_dir
    assert settings.install_dir == project_dir
    assert settings.cache_root == project_dir / ".cache"


def test_directory_overrides_and_home_expansion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("AIRBYTE_PROJECT_DIR", "~/project")
    monkeypatch.setenv("AIRBYTE_INSTALL_DIR", "~/install")
    monkeypatch.setenv("AIRBYTE_CACHE_ROOT", "~/cache")

    settings = AirbyteSettings()
    assert settings.project_dir == home / "project"
    assert settings.install_dir == home / "install"
    assert settings.cache_root == home / "cache"


def test_empty_directory_values_are_ignored(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    for variable in (
        "AIRBYTE_PROJECT_DIR",
        "AIRBYTE_INSTALL_DIR",
        "AIRBYTE_CACHE_ROOT",
    ):
        monkeypatch.setenv(variable, "")

    settings = AirbyteSettings()
    assert settings.project_dir == tmp_path
    assert settings.install_dir == tmp_path
    assert settings.cache_root == tmp_path / ".cache"


@pytest.mark.parametrize(
    ("ci_value", "override", "expected"),
    [
        pytest.param(None, None, False, id="ci-unset"),
        pytest.param("true", None, True, id="ci-true"),
        pytest.param("true", "false", False, id="explicit-false"),
    ],
)
def test_print_full_error_logs_default(
    monkeypatch: pytest.MonkeyPatch,
    ci_value: str | None,
    override: str | None,
    expected: bool,
) -> None:
    if ci_value is None:
        monkeypatch.delenv("CI", raising=False)
    else:
        monkeypatch.setenv("CI", ci_value)
    if override is None:
        monkeypatch.delenv("AIRBYTE_PRINT_FULL_ERROR_LOGS", raising=False)
    else:
        monkeypatch.setenv("AIRBYTE_PRINT_FULL_ERROR_LOGS", override)

    assert AirbyteSettings().print_full_error_logs is expected


def test_importing_airbyte_does_not_create_settings_directories(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    install_dir = tmp_path / "install"
    environment = os.environ.copy()
    environment["AIRBYTE_PROJECT_DIR"] = str(project_dir)
    environment["AIRBYTE_INSTALL_DIR"] = str(install_dir)

    subprocess.run(
        [sys.executable, "-c", "import airbyte"],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
        cwd=Path(__file__).parents[2],
    )

    assert not project_dir.exists()
    assert not install_dir.exists()


@pytest.mark.parametrize(
    ("field", "generic_env_var", "cloud_env_var"),
    [
        pytest.param(
            "client_id",
            constants.AIRBYTE_CLIENT_ID_ENV_VAR,
            constants.CLOUD_CLIENT_ID_ENV_VAR,
            id="client-id",
        ),
        pytest.param(
            "client_secret",
            constants.AIRBYTE_CLIENT_SECRET_ENV_VAR,
            constants.CLOUD_CLIENT_SECRET_ENV_VAR,
            id="client-secret",
        ),
        pytest.param(
            "bearer_token",
            constants.AIRBYTE_BEARER_TOKEN_ENV_VAR,
            constants.CLOUD_BEARER_TOKEN_ENV_VAR,
            id="bearer-token",
        ),
        pytest.param(
            "workspace_id",
            constants.AIRBYTE_WORKSPACE_ID_ENV_VAR,
            constants.CLOUD_WORKSPACE_ID_ENV_VAR,
            id="workspace-id",
        ),
        pytest.param(
            "organization_id",
            constants.AIRBYTE_ORGANIZATION_ID_ENV_VAR,
            constants.CLOUD_ORGANIZATION_ID_ENV_VAR,
            id="organization-id",
        ),
    ],
)
def test_cloud_settings_credential_environment_aliases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
    generic_env_var: str,
    cloud_env_var: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(cloud_env_var, "cloud-value")
    assert str(getattr(AirbyteCloudSettings(), field)) == "cloud-value"

    monkeypatch.setenv(generic_env_var, "generic-value")
    assert str(getattr(AirbyteCloudSettings(), field)) == "generic-value"


def test_cloud_settings_api_url_aliases_and_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    settings = AirbyteCloudSettings()
    assert settings.api_url == constants.CLOUD_API_ROOT
    assert settings.config_api_url is None

    monkeypatch.setenv(constants.CLOUD_API_ROOT_ENV_VAR, "https://api.example")
    monkeypatch.setenv(
        constants.CLOUD_CONFIG_API_ROOT_ENV_VAR,
        "https://config.example",
    )
    settings = AirbyteCloudSettings()
    assert settings.api_url == "https://api.example"
    assert settings.config_api_url == "https://config.example"


def test_cloud_settings_dotenv_values_and_environment_precedence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        f"{constants.CLOUD_CLIENT_ID_ENV_VAR}=dotenv-client-id\n"
        f"{constants.CLOUD_API_ROOT_ENV_VAR}=https://dotenv.example\n",
        encoding="utf-8",
    )

    settings = AirbyteCloudSettings()
    assert str(settings.client_id) == "dotenv-client-id"
    assert settings.api_url == "https://dotenv.example"

    monkeypatch.setenv(constants.AIRBYTE_CLIENT_ID_ENV_VAR, "process-client-id")
    monkeypatch.setenv(constants.CLOUD_API_ROOT_ENV_VAR, "https://process.example")
    settings = AirbyteCloudSettings()
    assert str(settings.client_id) == "process-client-id"
    assert settings.api_url == "https://process.example"


def test_cloud_settings_ignore_empty_values_and_use_default_api_url(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    for env_var in _CLOUD_SETTINGS_ENV_VARS:
        monkeypatch.setenv(env_var, "")

    settings = AirbyteCloudSettings()

    assert settings.client_id is None
    assert settings.client_secret is None
    assert settings.bearer_token is None
    assert settings.workspace_id is None
    assert settings.organization_id is None
    assert settings.api_url == constants.CLOUD_API_ROOT
    assert settings.config_api_url is None
