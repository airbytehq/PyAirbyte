# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Secrets management for PyAirbyte."""

from __future__ import annotations

from airbyte.secrets.base import SecretHandle, SecretSourceEnum, SecretString
from airbyte.secrets.config import disable_secret_source, register_secret_manager
from airbyte.secrets.env_vars import DotenvSecretManager, EnvVarSecretManager
from airbyte.secrets.google_colab import ColabSecretManager
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from airbyte.secrets.prompt import SecretsPrompt
from airbyte.secrets.util import get_secret


__all__ = [
    # Secret Access
    "get_secret",
    # Secret Classes
    "SecretHandle",
    "SecretString",
    "SecretSourceEnum",
    # Secret Managers
    "ColabSecretManager",
    "CustomSecretManager",
    "DotenvSecretManager",
    "EnvVarSecretManager",
    "GoogleGSMSecretManager",
    "SecretManager",
    "SecretsPrompt",
    # Registration Functions`
    "register_secret_manager",
    "disable_secret_source",
]
