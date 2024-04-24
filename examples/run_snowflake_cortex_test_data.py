# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry install
    poetry run python examples/run_snowflake_cortex_with_github.py
"""

from __future__ import annotations

from typing import Any

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    StreamDescriptor,
    SyncMode,
    Type,
)

import airbyte as ab
from airbyte._processors.sql.snowflakecortex import SnowflakeCortexSqlProcessor

# from airbyte._util.google_secrets import get_gcp_secret_json
from airbyte.caches import SnowflakeCache
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from airbyte.strategies import WriteStrategy


AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
secret_mgr = GoogleGSMSecretManager(
    project=AIRBYTE_INTERNAL_GCP_PROJECT,
    credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
)

secret = secret_mgr.get_secret(
    secret_name="PYAIRBYTE_SNOWFLAKE_PARTNER_ACCOUNT_CREDS",
)
assert secret is not None, "Secret not found."
secret_config = secret.parse_json()

cache = SnowflakeCache(
    account=secret_config["account"],
    username=secret_config["username"],
    password=secret_config["password"],
    database=secret_config["database"],
    warehouse=secret_config["warehouse"],
    role=secret_config["role"],
)

# create sample catalog
stream_schema = {
    "type": "object",
    "properties": {
        "str_col": {"type": "string"},
        "int_col": {"type": "integer"},
        "page_content": {"type": "string"},
        "metadata": {"type": "object"},
        "embedding": {"type": "vector_array"},
    },
}
overwrite_stream = ConfiguredAirbyteStream(
    stream=AirbyteStream(
        name="myteststream",
        json_schema=stream_schema,
        supported_sync_modes=[SyncMode.incremental, SyncMode.full_refresh],
    ),
    primary_key=[["int_col"]],
    sync_mode=SyncMode.incremental,
    destination_sync_mode=DestinationSyncMode.overwrite,
)
catalog = ConfiguredAirbyteCatalog(streams=[overwrite_stream])


# create test messages
message1 = AirbyteMessage(
    type=Type.RECORD,
    record=AirbyteRecordMessage(
        stream="myteststream",
        data={
            "str_col": "Dogs are number 1",
            "int_col": 4,
            "page_content": "str_col: Dogs are number 1",
            "metadata": {"int_col": 4, "_ab_stream": "mystream"},
            "embedding": [
                -0.00438284986621647,
                -0.0037110261657951915,
                -0.02161210642043671,
                -0.00438284986621647,
                -0.00438284986621647,
            ],
        },
        emitted_at=0,
    ),
)
message2 = AirbyteMessage(
    type=Type.RECORD,
    record=AirbyteRecordMessage(
        stream="myteststream",
        data={
            "str_col": "Dogs are number 2",
            "int_col": 5,
            "page_content": "str_col: Dogs are number 2",
            "metadata": {"int_col": 5, "_ab_stream": "mystream"},
            "embedding": [
                -0.00438284986621647,
                -0.0037110261657951915,
                -0.02161210642043671,
                -0.00438284986621647,
                -0.00438284986621647,
            ],
        },
        emitted_at=0,
    ),
)
message3 = AirbyteMessage(
    type=Type.RECORD,
    record=AirbyteRecordMessage(
        stream="myteststream",
        data={
            "str_col": "Dogs are number 3",
            "int_col": 10,
            "page_content": "str_col: Dogs are number 3",
            "metadata": {"int_col": 10, "_ab_stream": "mystream"},
            "embedding": [
                -0.00438284986621647,
                -0.0037110261657951915,
                -0.02161210642043671,
                -0.00438284986621647,
                -0.00438284986621647,
            ],
        },
        emitted_at=0,
    ),
)


# helper methods to create state message
def _state(data: dict[str, Any]) -> AirbyteMessage:
    stream = AirbyteStreamState(
        stream_descriptor=StreamDescriptor(name="myteststream", namespace=None)
    )

    return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(stream=stream, data=data))


state_message = _state({"state": "1"})
messages = [message1, message2, message3, state_message]

# create a SQL processor using Snowflake cache
processor = SnowflakeCortexSqlProcessor(
    cache=cache,
    catalog=catalog,
    vector_length=5,
    source_name="github",
    stream_names=["myteststream"],
)
processor.process_airbyte_messages(messages, WriteStrategy.REPLACE)
