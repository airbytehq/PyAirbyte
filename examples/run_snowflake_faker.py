# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry install
    poetry run python examples/run_snowflake_faker.py
"""

from __future__ import annotations

import airbyte as ab
from airbyte.caches import SnowflakeCache
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


SCALE = 10_000


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


cache = SnowflakeCache(
    account=secret_config["account"],
    username=secret_config["username"],
    password=secret_config["password"],
    database=secret_config["database"],
    warehouse=secret_config["warehouse"],
    role=secret_config["role"],
)

source = ab.get_source(
    "source-faker",
    config={
        "count": SCALE,
    },
    install_if_missing=True,
    streams="*",
)
source.check()

result = source.read(cache)

for name in ["products"]:
    print(f"Stream {name}: {len(list(result[name]))} records")
