# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Compatibility re-export of `airbyte._direct_connectors.connector_docs`."""

from __future__ import annotations

from airbyte._direct_connectors.connector_docs import (
    BIGQUERY_DESTINATION_DEFINITION_ID,
    DESTINATION_SKILL_PREFIX,
    LOCAL_DESTINATION_SECTION_IDS,
    SECTION_CONNECTIONS,
    SECTION_SQL_PASSTHROUGH,
    SECTION_STREAMS,
    SNOWFLAKE_DESTINATION_DEFINITION_ID,
    SOURCE_SKILL_PREFIX,
    SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS,
    SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    SQL_PASSTHROUGH_DESTINATION_NAMES,
    build_destination_connector_details,
    build_destination_skill_docs,
    connector_id_from_skill_id,
    destination_skill_id,
    merge_destination_skill_docs,
)


__all__ = [
    "BIGQUERY_DESTINATION_DEFINITION_ID",
    "DESTINATION_SKILL_PREFIX",
    "LOCAL_DESTINATION_SECTION_IDS",
    "SECTION_CONNECTIONS",
    "SECTION_SQL_PASSTHROUGH",
    "SECTION_STREAMS",
    "SNOWFLAKE_DESTINATION_DEFINITION_ID",
    "SOURCE_SKILL_PREFIX",
    "SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS",
    "SQL_PASSTHROUGH_DESTINATION_DIALECTS",
    "SQL_PASSTHROUGH_DESTINATION_NAMES",
    "build_destination_connector_details",
    "build_destination_skill_docs",
    "connector_id_from_skill_id",
    "destination_skill_id",
    "merge_destination_skill_docs",
]
