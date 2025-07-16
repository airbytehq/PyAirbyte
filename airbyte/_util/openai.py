# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""OpenAI API utilities, including support for OpenAI-compatible APIs."""

import subprocess
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from openai import Client

from airbyte.secrets import get_secret
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable


_has_github_model_token: dict[str, bool] = {}


def _try_get_openai_client_from_api_key(
    **kwargs: Any,
) -> Client | None:
    """Get the OpenAI API key, or None if not set.

    Anything sent as keyword argument will be passed to the OpenAI client constructor.
    asdf
    """
    if api_key := get_secret("OPENAI_API_KEY", or_none=True):
        return Client(
            api_key=api_key,
            base_url=get_secret("OPENAI_BASE_URL", or_none=True) or None,
            **kwargs,
        )

    return None


def _validate_github_models_token(token: SecretString) -> bool:
    """Validate that a GitHub token has models:read permission."""
    try:
        # Create a temporary client to test the token
        test_client = Client(
            base_url="https://models.github.ai/inference",
            api_key=token,
        )
        # Try to list available models - this requires models:read permission
        test_client.models.list()
    except Exception:
        # Any error (auth, permission, network) means the token isn't valid for models
        return False
    else:
        # If we reach here, the token is valid and has the required permission
        return True


def _try_get_github_models_token() -> SecretString | None:
    """Try to get a GitHub token from various sources."""
    methods: dict[str, Callable] = {
        "GITHUB_MODELS_PAT": lambda: get_secret("GITHUB_MODELS_PAT", or_none=True),
        "GITHUB_TOKEN": lambda: get_secret("GITHUB_TOKEN", or_none=True),
        "gh_cli": lambda: subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5
        ).stdout.strip(),
    }
    # First try the specific GitHub Models PAT
    for method_name, token_fn in methods.items():
        match _has_github_model_token.get(method_name, None):
            case True:
                # If we already know this method works, return the token
                # This could trigger an exception if the token has recently
                # become invalid. But that is a true exception and should
                # be raised to the caller.
                if raw_token := token_fn():
                    return SecretString(raw_token)
                # If token_fn() fails, let the exception propagate

            case False:
                # Skip this method, we know it doesn't work
                continue

            case None:
                # Not yet validated, proceed to validate
                token: SecretString | None = None
                with suppress(Exception):
                    if raw_token := token_fn():
                        token = SecretString(raw_token)
                if token and _validate_github_models_token(token):
                    _has_github_model_token[method_name] = True
                    return token

                # Otherwise, mark this method as not valid:
                _has_github_model_token[method_name] = False

    # If we reach here, no valid GitHub token was found
    return None


def try_get_openai_client(**kwargs: Any) -> Client | None:
    """Get the OpenAI client, or None if not available.

    Any keyword arguments are passed to the OpenAI client constructor.
    """
    if client := _try_get_openai_client_from_api_key(**kwargs):
        return client

    if token := _try_get_github_models_token():
        return Client(
            base_url="https://models.github.ai/inference",  # << OpenAI-compatible endpoint
            api_key=token,
            **kwargs,
        )

    return None
