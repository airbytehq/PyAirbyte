# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import airbyte as ab
from airbyte._util.google_secrets import get_gcp_secret_json
from airbyte.caches import SnowflakeCache


source = ab.get_source(
    "source-faker",
    config={"count": 10000, "seed": 0, "parallelism": 1, "always_updated": False},
    install_if_missing=True,
)

secret = get_gcp_secret_json(
    project_name="dataline-integration-testing",
    secret_name="AIRBYTE_LIB_SNOWFLAKE_CREDS",
)

cache = SnowflakeCache(
    account=secret["account"],
    username=secret["username"],
    password=secret["password"],
    database=secret["database"],
    warehouse=secret["warehouse"],
    role=secret["role"],
)

source.check()

source.select_streams(["products"])
result = source.read(cache)

for name in ["products"]:
    print(f"Stream {name}: {len(list(result[name]))} records")
