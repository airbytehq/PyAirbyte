"""Unit tests for local MCP tool descriptions."""

from __future__ import annotations

import inspect

import pytest

from airbyte.mcp import local

_CONFIG_PARAMS = ("config", "config_secret_name", "manifest_path")
_CONFIG_HELP_TEXT = local._CONFIG_HELP.strip()

_TOOLS_WITH_CONFIG_HELP = (
    "validate_local_connector_config",
    "list_local_source_streams",
    "get_local_source_stream_json_schema",
    "read_local_source_stream_records",
    "get_local_stream_previews",
    "sync_local_source_to_cache",
)
_TOOLS_WITHOUT_CONFIG_HELP = (
    "list_local_dotenv_secrets",
    "list_local_cached_streams",
    "describe_local_default_cache",
    "run_local_sql_query",
)


def _local_functions() -> list:
    return [
        func
        for func in vars(local).values()
        if inspect.isfunction(func)
        and func.__module__ == local.__name__
        and func.__doc__
    ]


_FUNCS_WITH_CONFIG_HELP = [
    func for func in _local_functions() if _CONFIG_HELP_TEXT in func.__doc__
]


@pytest.mark.parametrize(
    "func",
    _FUNCS_WITH_CONFIG_HELP,
    ids=[func.__name__ for func in _FUNCS_WITH_CONFIG_HELP],
)
def test_config_help_only_on_tools_with_config_params(func) -> None:
    """Tools documenting config params must actually accept them."""
    params = inspect.signature(func).parameters
    missing = [name for name in _CONFIG_PARAMS if name not in params]
    assert not missing, (
        f"{func.__name__} includes _CONFIG_HELP but lacks params: {missing}"
    )


@pytest.mark.parametrize("tool_name", _TOOLS_WITH_CONFIG_HELP)
def test_config_tools_keep_config_help(tool_name: str) -> None:
    """Tools that take config params still include the config help text."""
    assert _CONFIG_HELP_TEXT in getattr(local, tool_name).__doc__


@pytest.mark.parametrize("tool_name", _TOOLS_WITHOUT_CONFIG_HELP)
def test_non_config_tools_omit_config_help(tool_name: str) -> None:
    """Tools without config params do not include the config help text."""
    assert _CONFIG_HELP_TEXT not in getattr(local, tool_name).__doc__
