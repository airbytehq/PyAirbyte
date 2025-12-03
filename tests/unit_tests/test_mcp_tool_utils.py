# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool utility functions."""

from __future__ import annotations

import importlib
import warnings
from unittest.mock import patch

import pytest

import airbyte.constants as constants
import airbyte.mcp._tool_utils as tool_utils
from airbyte.mcp._annotations import READ_ONLY_HINT

# (enabled, disabled, domain, readonly_mode, is_readonly, domain_enabled, should_register)
_DOMAIN_CASES = [
    (None, None, "cloud", False, False, True, True),
    (None, None, "registry", False, False, True, True),
    (None, None, "local", False, False, True, True),
    (["cloud"], None, "cloud", False, False, True, True),
    (["cloud"], None, "registry", False, False, False, False),
    (None, ["registry"], "registry", False, False, False, False),
    (None, ["registry"], "cloud", False, False, True, True),
    (["registry", "cloud"], ["registry"], "cloud", False, False, True, True),
    (["registry", "cloud"], ["registry"], "registry", False, False, False, False),
    (["cloud"], ["registry"], "local", False, False, False, False),
    (["CLOUD"], None, "cloud", False, False, True, True),
    (["cloud"], None, "CLOUD", False, False, True, True),
    (None, None, "cloud", True, False, True, False),
    (None, None, "cloud", True, True, True, True),
    (None, None, "registry", True, False, True, True),
    (["cloud"], None, "cloud", True, True, True, True),
    (["registry"], None, "cloud", True, True, False, False),
]


@pytest.mark.parametrize(
    "enabled,disabled,domain,readonly_mode,is_readonly,domain_enabled,should_register",
    _DOMAIN_CASES,
)
def test_domain_logic(
    enabled: list[str] | None,
    disabled: list[str] | None,
    domain: str,
    readonly_mode: bool,
    is_readonly: bool,
    domain_enabled: bool,
    should_register: bool,
) -> None:
    norm_enabled = [d.lower() for d in enabled] if enabled else None
    norm_disabled = [d.lower() for d in disabled] if disabled else None
    with (
        patch("airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS", norm_enabled),
        patch("airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS_DISABLED", norm_disabled),
        patch("airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_READONLY_MODE", readonly_mode),
    ):
        tool_utils._resolve_mcp_domain_filters.cache_clear()
        assert tool_utils.is_domain_enabled(domain) == domain_enabled
        assert (
            tool_utils.should_register_tool({
                "domain": domain,
                READ_ONLY_HINT: is_readonly,
            })
            == should_register
        )


# (env_var, attr, env_value, expected)
_ENV_PARSE_CASES = [
    ("AIRBYTE_MCP_DOMAINS", "AIRBYTE_MCP_DOMAINS", "", None),
    ("AIRBYTE_MCP_DOMAINS", "AIRBYTE_MCP_DOMAINS", "cloud", ["cloud"]),
    (
        "AIRBYTE_MCP_DOMAINS",
        "AIRBYTE_MCP_DOMAINS",
        "registry,cloud",
        ["registry", "cloud"],
    ),
    (
        "AIRBYTE_MCP_DOMAINS",
        "AIRBYTE_MCP_DOMAINS",
        "registry, cloud",
        ["registry", "cloud"],
    ),
    (
        "AIRBYTE_MCP_DOMAINS",
        "AIRBYTE_MCP_DOMAINS",
        "REGISTRY,CLOUD",
        ["registry", "cloud"],
    ),
    (
        "AIRBYTE_MCP_DOMAINS",
        "AIRBYTE_MCP_DOMAINS",
        "registry,,cloud",
        ["registry", "cloud"],
    ),
    ("AIRBYTE_MCP_DOMAINS_DISABLED", "AIRBYTE_MCP_DOMAINS_DISABLED", "", None),
    (
        "AIRBYTE_MCP_DOMAINS_DISABLED",
        "AIRBYTE_MCP_DOMAINS_DISABLED",
        "registry",
        ["registry"],
    ),
    (
        "AIRBYTE_MCP_DOMAINS_DISABLED",
        "AIRBYTE_MCP_DOMAINS_DISABLED",
        "registry,local",
        ["registry", "local"],
    ),
]


@pytest.mark.parametrize("env_var,attr,env_value,expected", _ENV_PARSE_CASES)
def test_env_parsing(
    env_var: str, attr: str, env_value: str, expected: list[str] | None
) -> None:
    with patch.dict("os.environ", {env_var: env_value}, clear=False):
        importlib.reload(constants)
        assert getattr(constants, attr) == expected
    importlib.reload(constants)


# (env_var, env_value, warning_fragment)
_WARNING_CASES = [
    (
        "AIRBYTE_MCP_DOMAINS",
        "cloud,invalid",
        "AIRBYTE_MCP_DOMAINS contains unknown domain(s)",
    ),
    (
        "AIRBYTE_MCP_DOMAINS_DISABLED",
        "registry,fake",
        "AIRBYTE_MCP_DOMAINS_DISABLED contains unknown domain(s)",
    ),
]


@pytest.mark.parametrize("env_var,env_value,fragment", _WARNING_CASES)
def test_unknown_domain_warning(env_var: str, env_value: str, fragment: str) -> None:
    with (
        patch.dict("os.environ", {env_var: env_value}, clear=False),
        warnings.catch_warnings(record=True) as caught,
    ):
        warnings.simplefilter("always")
        importlib.reload(constants)
        importlib.reload(tool_utils)
        tool_utils._resolve_mcp_domain_filters.cache_clear()
        tool_utils._resolve_mcp_domain_filters()
        messages = [str(w.message) for w in caught]
        assert any(fragment in m for m in messages)
        assert any("Known MCP domains are:" in m for m in messages)
    importlib.reload(constants)
    importlib.reload(tool_utils)
