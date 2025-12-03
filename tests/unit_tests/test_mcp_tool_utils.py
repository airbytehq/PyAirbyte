# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool utility functions."""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    "enabled_domains,disabled_domains,domain,expected",
    [
        pytest.param(
            set(),
            set(),
            "cloud",
            True,
            id="no_env_vars_set_all_enabled",
        ),
        pytest.param(
            set(),
            set(),
            "registry",
            True,
            id="no_env_vars_set_registry_enabled",
        ),
        pytest.param(
            set(),
            set(),
            "local",
            True,
            id="no_env_vars_set_local_enabled",
        ),
        pytest.param(
            {"registry", "cloud"},
            set(),
            "cloud",
            True,
            id="scenario_a_cloud_enabled",
        ),
        pytest.param(
            {"registry", "cloud"},
            set(),
            "registry",
            True,
            id="scenario_a_registry_enabled",
        ),
        pytest.param(
            {"registry", "cloud"},
            set(),
            "local",
            False,
            id="scenario_a_local_disabled",
        ),
        pytest.param(
            set(),
            {"registry"},
            "cloud",
            True,
            id="scenario_b_cloud_enabled",
        ),
        pytest.param(
            set(),
            {"registry"},
            "local",
            True,
            id="scenario_b_local_enabled",
        ),
        pytest.param(
            set(),
            {"registry"},
            "registry",
            False,
            id="scenario_b_registry_disabled",
        ),
        pytest.param(
            {"registry", "cloud"},
            {"registry"},
            "cloud",
            True,
            id="scenario_c_cloud_enabled",
        ),
        pytest.param(
            {"registry", "cloud"},
            {"registry"},
            "registry",
            False,
            id="scenario_c_registry_disabled_by_intersection",
        ),
        pytest.param(
            {"registry", "cloud"},
            {"registry"},
            "local",
            False,
            id="scenario_c_local_not_in_enabled_list",
        ),
        pytest.param(
            {"cloud"},
            {"registry"},
            "cloud",
            True,
            id="scenario_d_cloud_enabled",
        ),
        pytest.param(
            {"cloud"},
            {"registry"},
            "registry",
            False,
            id="scenario_d_registry_not_in_enabled_list",
        ),
        pytest.param(
            {"cloud"},
            {"registry"},
            "local",
            False,
            id="scenario_d_local_not_in_enabled_list",
        ),
        pytest.param(
            {"CLOUD"},
            set(),
            "cloud",
            True,
            id="case_insensitive_enabled_uppercase",
        ),
        pytest.param(
            {"cloud"},
            set(),
            "CLOUD",
            True,
            id="case_insensitive_domain_uppercase",
        ),
    ],
)
def test_is_domain_enabled(
    enabled_domains: set[str],
    disabled_domains: set[str],
    domain: str,
    expected: bool,
) -> None:
    """Test is_domain_enabled function with various domain configurations."""
    with (
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS",
            {d.lower() for d in enabled_domains},
        ),
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS_DISABLED",
            {d.lower() for d in disabled_domains},
        ),
    ):
        from airbyte.mcp._tool_utils import is_domain_enabled

        result = is_domain_enabled(domain)
        assert result == expected


@pytest.mark.parametrize(
    "enabled_domains,disabled_domains,tool_domain,readonly_mode,is_readonly,expected",
    [
        pytest.param(
            set(),
            set(),
            "cloud",
            False,
            False,
            True,
            id="no_filtering_cloud_tool_registered",
        ),
        pytest.param(
            set(),
            set(),
            "registry",
            False,
            False,
            True,
            id="no_filtering_registry_tool_registered",
        ),
        pytest.param(
            {"cloud"},
            set(),
            "cloud",
            False,
            False,
            True,
            id="cloud_enabled_cloud_tool_registered",
        ),
        pytest.param(
            {"cloud"},
            set(),
            "registry",
            False,
            False,
            False,
            id="cloud_enabled_registry_tool_not_registered",
        ),
        pytest.param(
            set(),
            {"registry"},
            "registry",
            False,
            False,
            False,
            id="registry_disabled_registry_tool_not_registered",
        ),
        pytest.param(
            set(),
            {"registry"},
            "cloud",
            False,
            False,
            True,
            id="registry_disabled_cloud_tool_registered",
        ),
        pytest.param(
            set(),
            set(),
            "cloud",
            True,
            False,
            False,
            id="readonly_mode_non_readonly_cloud_tool_not_registered",
        ),
        pytest.param(
            set(),
            set(),
            "cloud",
            True,
            True,
            True,
            id="readonly_mode_readonly_cloud_tool_registered",
        ),
        pytest.param(
            set(),
            set(),
            "registry",
            True,
            False,
            True,
            id="readonly_mode_non_cloud_tool_registered",
        ),
        pytest.param(
            {"cloud"},
            set(),
            "cloud",
            True,
            True,
            True,
            id="domain_filter_and_readonly_mode_combined",
        ),
        pytest.param(
            {"registry"},
            set(),
            "cloud",
            True,
            True,
            False,
            id="domain_filter_blocks_even_readonly_cloud_tool",
        ),
    ],
)
def test_should_register_tool(
    enabled_domains: set[str],
    disabled_domains: set[str],
    tool_domain: str,
    readonly_mode: bool,
    is_readonly: bool,
    expected: bool,
) -> None:
    """Test should_register_tool function with various configurations."""
    from airbyte.mcp._annotations import READ_ONLY_HINT

    annotations = {
        "domain": tool_domain,
        READ_ONLY_HINT: is_readonly,
    }

    with (
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS",
            {d.lower() for d in enabled_domains},
        ),
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS_DISABLED",
            {d.lower() for d in disabled_domains},
        ),
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_READONLY_MODE",
            readonly_mode,
        ),
    ):
        from airbyte.mcp._tool_utils import should_register_tool

        result = should_register_tool(annotations)
        assert result == expected


@pytest.mark.parametrize(
    "env_value,expected_set",
    [
        pytest.param(
            "",
            set(),
            id="empty_string_returns_empty_set",
        ),
        pytest.param(
            "cloud",
            {"cloud"},
            id="single_value",
        ),
        pytest.param(
            "registry,cloud",
            {"registry", "cloud"},
            id="multiple_values",
        ),
        pytest.param(
            "registry, cloud, local",
            {"registry", "cloud", "local"},
            id="values_with_spaces",
        ),
        pytest.param(
            "REGISTRY,CLOUD",
            {"registry", "cloud"},
            id="uppercase_normalized_to_lowercase",
        ),
        pytest.param(
            "registry,,cloud",
            {"registry", "cloud"},
            id="empty_values_filtered",
        ),
    ],
)
def test_domain_env_var_parsing(env_value: str, expected_set: set[str]) -> None:
    """Test that AIRBYTE_MCP_DOMAINS env var is parsed correctly."""
    import importlib

    import airbyte.mcp._tool_utils as tool_utils

    with patch.dict("os.environ", {"AIRBYTE_MCP_DOMAINS": env_value}, clear=False):
        importlib.reload(tool_utils)
        assert tool_utils.AIRBYTE_MCP_DOMAINS == expected_set

    importlib.reload(tool_utils)


@pytest.mark.parametrize(
    "env_value,expected_set",
    [
        pytest.param(
            "",
            set(),
            id="empty_string_returns_empty_set",
        ),
        pytest.param(
            "registry",
            {"registry"},
            id="single_value",
        ),
        pytest.param(
            "registry,local",
            {"registry", "local"},
            id="multiple_values",
        ),
    ],
)
def test_domain_disabled_env_var_parsing(
    env_value: str, expected_set: set[str]
) -> None:
    """Test that AIRBYTE_MCP_DOMAINS_DISABLED env var is parsed correctly."""
    import importlib

    import airbyte.mcp._tool_utils as tool_utils

    with patch.dict(
        "os.environ", {"AIRBYTE_MCP_DOMAINS_DISABLED": env_value}, clear=False
    ):
        importlib.reload(tool_utils)
        assert tool_utils.AIRBYTE_MCP_DOMAINS_DISABLED == expected_set

    importlib.reload(tool_utils)
