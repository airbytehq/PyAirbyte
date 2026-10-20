# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Load MCP argument-tracing rules from the adjacent YAML file."""

from pathlib import Path

import yaml


ARG_TRACE_RULES: dict[str, str] = yaml.safe_load(
    Path(__file__).with_name("arg_trace_rules.yaml").read_text(encoding="utf-8")
)
