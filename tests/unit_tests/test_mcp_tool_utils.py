# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool utility functions."""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    "enabled_domains,disabled_domains,domain,expected",
    [
        pytest.param(
            None,
            None,
            "cloud",
            True,
            id="no_env_vars_set_all_enabled",
        ),
        pytest.param(
            None,
            None,
            "registry",
            True,
            id="no_env_vars_set_registry_enabled",
        ),
        pytest.param(
            None,
            None,
            "local",
            True,
            id="no_env_vars_set_local_enabled",
        ),
        pytest.param(
            ["registry", "cloud"],
            None,
            "cloud",
            True,
            id="scenario_a_cloud_enabled",
        ),
        pytest.param(
            ["registry", "cloud"],
            None,
            "registry",
            True,
            id="scenario_a_registry_enabled",
        ),
        pytest.param(
            ["registry", "cloud"],
            None,
            "local",
            False,
            id="scenario_a_local_disabled",
        ),
        pytest.param(
            None,
            ["registry"],
            "cloud",
            True,
            id="scenario_b_cloud_enabled",
        ),
        pytest.param(
            None,
            ["registry"],
            "local",
            True,
            id="scenario_b_local_enabled",
        ),
        pytest.param(
            None,
            ["registry"],
            "registry",
            False,
            id="scenario_b_registry_disabled",
        ),
        pytest.param(
            ["registry", "cloud"],
            ["registry"],
            "cloud",
            True,
            id="scenario_c_cloud_enabled",
        ),
        pytest.param(
            ["registry", "cloud"],
            ["registry"],
            "registry",
            False,
            id="scenario_c_registry_disabled_by_intersection",
        ),
        pytest.param(
            ["registry", "cloud"],
            ["registry"],
            "local",
            False,
            id="scenario_c_local_not_in_enabled_list",
        ),
        pytest.param(
            ["cloud"],
            ["registry"],
            "cloud",
            True,
            id="scenario_d_cloud_enabled",
        ),
        pytest.param(
            ["cloud"],
            ["registry"],
            "registry",
            False,
            id="scenario_d_registry_not_in_enabled_list",
        ),
        pytest.param(
            ["cloud"],
            ["registry"],
            "local",
            False,
            id="scenario_d_local_not_in_enabled_list",
        ),
        pytest.param(
            ["CLOUD"],
            None,
            "cloud",
            True,
            id="case_insensitive_enabled_uppercase",
        ),
        pytest.param(
            ["cloud"],
            None,
            "CLOUD",
            True,
            id="case_insensitive_domain_uppercase",
        ),
    ],
)
def test_is_domain_enabled(
    enabled_domains: list[str] | None,
    disabled_domains: list[str] | None,
    domain: str,
    expected: bool,
) -> None:
    """Test is_domain_enabled function with various domain configurations."""
    import airbyte.mcp._tool_utils as tool_utils

    # Normalize to lowercase like the real code does
    normalized_enabled = (
        [d.lower() for d in enabled_domains] if enabled_domains else None
    )
    normalized_disabled = (
        [d.lower() for d in disabled_domains] if disabled_domains else None
    )

    with (
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS",
            normalized_enabled,
        ),
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS_DISABLED",
            normalized_disabled,
        ),
    ):
        # Clear the lru_cache to ensure fresh computation
        tool_utils._resolve_mcp_domain_filters.cache_clear()
        result = tool_utils.is_domain_enabled(domain)
        assert result == expected


@pytest.mark.parametrize(
    "enabled_domains,disabled_domains,tool_domain,readonly_mode,is_readonly,expected",
    [
        pytest.param(
            None,
            None,
            "cloud",
            False,
            False,
            True,
            id="no_filtering_cloud_tool_registered",
        ),
        pytest.param(
            None,
            None,
            "registry",
            False,
            False,
            True,
            id="no_filtering_registry_tool_registered",
        ),
        pytest.param(
            ["cloud"],
            None,
            "cloud",
            False,
            False,
            True,
            id="cloud_enabled_cloud_tool_registered",
        ),
        pytest.param(
            ["cloud"],
            None,
            "registry",
            False,
            False,
            False,
            id="cloud_enabled_registry_tool_not_registered",
        ),
        pytest.param(
            None,
            ["registry"],
            "registry",
            False,
            False,
            False,
            id="registry_disabled_registry_tool_not_registered",
        ),
        pytest.param(
            None,
            ["registry"],
            "cloud",
            False,
            False,
            True,
            id="registry_disabled_cloud_tool_registered",
        ),
        pytest.param(
            None,
            None,
            "cloud",
            True,
            False,
            False,
            id="readonly_mode_non_readonly_cloud_tool_not_registered",
        ),
        pytest.param(
            None,
            None,
            "cloud",
            True,
            True,
            True,
            id="readonly_mode_readonly_cloud_tool_registered",
        ),
        pytest.param(
            None,
            None,
            "registry",
            True,
            False,
            True,
            id="readonly_mode_non_cloud_tool_registered",
        ),
        pytest.param(
            ["cloud"],
            None,
            "cloud",
            True,
            True,
            True,
            id="domain_filter_and_readonly_mode_combined",
        ),
        pytest.param(
            ["registry"],
            None,
            "cloud",
            True,
            True,
            False,
            id="domain_filter_blocks_even_readonly_cloud_tool",
        ),
    ],
)
def test_should_register_tool(
    enabled_domains: list[str] | None,
    disabled_domains: list[str] | None,
    tool_domain: str,
    readonly_mode: bool,
    is_readonly: bool,
    expected: bool,
) -> None:
    """Test should_register_tool function with various configurations."""
    import airbyte.mcp._tool_utils as tool_utils

    from airbyte.mcp._annotations import READ_ONLY_HINT

    annotations = {
        "domain": tool_domain,
        READ_ONLY_HINT: is_readonly,
    }

    with (
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS",
            enabled_domains,
        ),
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_MCP_DOMAINS_DISABLED",
            disabled_domains,
        ),
        patch(
            "airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_READONLY_MODE",
            readonly_mode,
        ),
    ):
        # Clear the lru_cache to ensure fresh computation
        tool_utils._resolve_mcp_domain_filters.cache_clear()
        result = tool_utils.should_register_tool(annotations)
        assert result == expected


@pytest.mark.parametrize(
    "env_value,expected_list",
    [
        pytest.param(
            "",
            None,
            id="empty_string_returns_none",
        ),
        pytest.param(
            "cloud",
            ["cloud"],
            id="single_value",
        ),
        pytest.param(
            "registry,cloud",
            ["registry", "cloud"],
            id="multiple_values",
        ),
        pytest.param(
            "registry, cloud, local",
            ["registry", "cloud", "local"],
            id="values_with_spaces",
        ),
        pytest.param(
            "REGISTRY,CLOUD",
            ["registry", "cloud"],
            id="uppercase_normalized_to_lowercase",
        ),
        pytest.param(
            "registry,,cloud",
            ["registry", "cloud"],
            id="empty_values_filtered",
        ),
    ],
)
def test_domain_env_var_parsing(
    env_value: str, expected_list: list[str] | None
) -> None:
    """Test that AIRBYTE_MCP_DOMAINS env var is parsed correctly."""
    import importlib

    import airbyte.constants as constants

    with patch.dict("os.environ", {"AIRBYTE_MCP_DOMAINS": env_value}, clear=False):
        importlib.reload(constants)
        assert constants.AIRBYTE_MCP_DOMAINS == expected_list

    importlib.reload(constants)


@pytest.mark.parametrize(
    "env_value,expected_list",
    [
        pytest.param(
            "",
            None,
            id="empty_string_returns_none",
        ),
        pytest.param(
            "registry",
            ["registry"],
            id="single_value",
        ),
        pytest.param(
            "registry,local",
            ["registry", "local"],
            id="multiple_values",
        ),
    ],
)
def test_domain_disabled_env_var_parsing(
    env_value: str, expected_list: list[str] | None
) -> None:
    """Test that AIRBYTE_MCP_DOMAINS_DISABLED env var is parsed correctly."""
    import importlib

    import airbyte.constants as constants

    with patch.dict(
        "os.environ", {"AIRBYTE_MCP_DOMAINS_DISABLED": env_value}, clear=False
    ):
        importlib.reload(constants)
        assert constants.AIRBYTE_MCP_DOMAINS_DISABLED == expected_list

    importlib.reload(constants)


@pytest.mark.parametrize(
    "env_var,env_value,expected_warning_contains",
    [
        pytest.param(
            "AIRBYTE_MCP_DOMAINS",
            "cloud,invalid_domain",
            "AIRBYTE_MCP_DOMAINS contains unknown domain(s): ['invalid_domain']",
            id="unknown_enabled_domain",
        ),
        pytest.param(
            "AIRBYTE_MCP_DOMAINS_DISABLED",
            "registry,fake_domain",
            "AIRBYTE_MCP_DOMAINS_DISABLED contains unknown domain(s): ['fake_domain']",
            id="unknown_disabled_domain",
        ),
    ],
)
def test_unknown_domain_warning(
    env_var: str, env_value: str, expected_warning_contains: str
) -> None:
    """Test that a warning is emitted when unknown domains are specified."""
    import importlib
    import warnings

    import airbyte.constants as constants
    import airbyte.mcp._tool_utils as tool_utils

    with (
        patch.dict("os.environ", {env_var: env_value}, clear=False),
        warnings.catch_warnings(record=True) as caught_warnings,
    ):
        warnings.simplefilter("always")
        # Reload constants to pick up the new env var value
        importlib.reload(constants)
        # Reload tool_utils to get fresh imports
        importlib.reload(tool_utils)
        # Clear the cache and call the function to trigger the warning
        tool_utils._resolve_mcp_domain_filters.cache_clear()
        tool_utils._resolve_mcp_domain_filters()

        warning_messages = [str(w.message) for w in caught_warnings]
        assert any(expected_warning_contains in msg for msg in warning_messages), (
            f"Expected warning containing '{expected_warning_contains}' "
            f"not found in {warning_messages}"
        )

        assert any("Known MCP domains are:" in msg for msg in warning_messages), (
            f"Expected 'Known MCP domains are:' in warning messages: {warning_messages}"
        )

    # Reload to restore original state
    importlib.reload(constants)
    importlib.reload(tool_utils)
