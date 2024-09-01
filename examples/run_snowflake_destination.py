# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry install
    poetry run python examples/run_snowflake_destination.py
"""

from __future__ import annotations

import airbyte as ab
from airbyte.caches import SnowflakeCache
from airbyte.secrets.google_gsm import GoogleGSMSecretManager

SCALE = 100


AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
secret_mgr = GoogleGSMSecretManager(
    project=AIRBYTE_INTERNAL_GCP_PROJECT,
    credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
)

secret = secret_mgr.get_secret(
    secret_name="AIRBYTE_LIB_SNOWFLAKE_CREDS",
)
assert secret is not None, "Secret not found."
secret_config = secret.parse_json()

snowflake_destination_secret = secret_mgr.fetch_connector_secret(
    connector_name="destination-snowflake",
).parse_json()
cortex_destination_secret = secret_mgr.fetch_connector_secret(
    connector_name="destination-snowflake-cortex",
).parse_json()


source = ab.get_source(
    "source-faker",
    config={
        "count": SCALE,
    },
    install_if_missing=True,
    streams=["products", "users"],
)
cache = SnowflakeCache(
    account=secret_config["account"],
    username=secret_config["username"],
    password=secret_config["password"],
    database=secret_config["database"],
    warehouse=secret_config["warehouse"],
    role=secret_config["role"],
)
snowflake_destination = ab.get_destination(
    "destination-snowflake",
    config={
        **snowflake_destination_secret,
        "default_schema": "pyairbyte_tests",
    },
)
cortex_destination_secret["processing"]["text_fields"] = [
    "make",
    "model",
    "name",
    "gender",
]
cortex_destination_secret["indexing"]["default_schema"] = "pyairbyte_tests"
cortex_destination = ab.get_destination(
    "destination-snowflake-cortex",
    config=cortex_destination_secret,
)
cortex_destination.print_config_spec()
# snowflake_destination.print_config_spec()
# cortex_destination.print_config_spec()
# snowflake_destination.check()
# cortex_destination.check()
# source.check()

# # This works:
# snowflake_write_result = snowflake_destination.write(
#     source,
#     cache=False,  # Toggle comment to test with/without caching
# )

# This fails with 'BrokenPipeError', but no other error logged:
cortex_write_result = cortex_destination.write(
    source,
    cache=False,  # Toggle comment to test with/without caching
)

# result = source.read(cache)

# for name in ["products"]:
#     print(f"Stream {name}: {len(list(result[name]))} records")
