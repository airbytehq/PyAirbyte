# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""An example script to run a fast lake copy operation using PyAirbyte.

Usage:
    poetry run python examples/run_fast_lake_copy.py

Required secrets:
  - SNOWFLAKE_PASSWORD: Password for Snowflake connection.
  - AWS_ACCESS_KEY_ID: AWS access key ID for S3 connection.
  - AWS_SECRET_ACCESS_KEY: AWS secret access key for S3 connection.
"""
from numpy import source

import airbyte as ab
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.lakes import S3LakeStorage
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


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

source = ab.get_source(
    "source-faker",
    config={
        "count": 1000,
        "seed": 0,
        "parallelism": 1,
        "always_updated": False,
    },
    install_if_missing=True,
    streams=["products"],
)

snowflake_cache_a = SnowflakeCache(
    account=secret_config["account"],
    username=secret_config["username"],
    password=secret_config["password"],
    database=secret_config["database"],
    warehouse=secret_config["warehouse"],
    role=secret_config["role"],
    schema_name="test_fast_copy_source",
)
snowflake_cache_b = SnowflakeCache(
    account=secret_config["account"],
    username=secret_config["username"],
    password=secret_config["password"],
    database=secret_config["database"],
    warehouse=secret_config["warehouse"],
    role=secret_config["role"],
    schema_name="test_fast_copy_dest",
)

s3_lake = S3LakeStorage(
    bucket_name="mybucket",
    region="us-west-2",
    access_key_id=ab.get_secret("AWS_ACCESS_KEY_ID"),
    secret_access_key=ab.get_secret("AWS_SECRET_ACCESS_KEY"),
)

# Begin processing
source.read(cache=snowflake_cache_a)

snowflake_cache_a.unload_stream_to_lake(
    stream_name="products",
    lake_store=s3_lake,
)

snowflake_cache_b.load_stream_from_lake(
    stream_name="products",
    lake_store=s3_lake,
    zero_copy=True,  # Set to True for zero-copy loading if supported.
)
