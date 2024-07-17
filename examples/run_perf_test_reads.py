# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Simple script to get performance profile of read throughput.

This script accepts a single argument `-e=SCALE` as a power of 10.

-e=2 is equivalent to 500 records.
-e=3 is equivalent to 5_000 records.
-e=4 is equivalent to 50_000 records.
-e=5 is equivalent to 500_000 records.
-e=6 is equivalent to 5_000_000 records.

Use smaller values of `e` (2-3) to understand read and overhead costs.
Use larger values of `e` (4-5) to understand write throughput at scale.

For performance profiling, use `viztracer` to generate a flamegraph:
```
poetry run viztracer --open -- ./examples/run_perf_test_reads.py -e=3
poetry run viztracer --open -- ./examples/run_perf_test_reads.py -e=5
```

To run without profiling, prefix script name with `poetry run python`:
```
# Run with 5_000 records
poetry run python ./examples/run_perf_test_reads.py -e=3
# Run with 500_000 records
poetry run python ./examples/run_perf_test_reads.py -e=5

# Load 5_000 records to Snowflake
poetry run python ./examples/run_perf_test_reads.py -e=3 --cache=snowflake

# Load 5_000 records to BigQuery
poetry run python ./examples/run_perf_test_reads.py -e=3 --cache=bigquery
```

"""

from __future__ import annotations

import argparse
import tempfile

import airbyte as ab
from airbyte.caches import BigQueryCache, CacheBase, SnowflakeCache
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"


def get_gsm_secret_json(secret_name: str) -> dict:
    secret_mgr = GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    )
    secret = secret_mgr.get_secret(
        secret_name=secret_name,
    )
    assert secret is not None, "Secret not found."
    return secret.parse_json()


def main(
    e: int = 4,
    cache_type: str = "duckdb",
) -> None:
    e = e or 4
    cache_type = cache_type or "duckdb"

    cache: CacheBase
    if cache_type == "duckdb":
        cache = ab.new_local_cache()

    elif cache_type == "snowflake":
        secret_config = get_gsm_secret_json(
            secret_name="AIRBYTE_LIB_SNOWFLAKE_CREDS",
        )
        cache = SnowflakeCache(
            account=secret_config["account"],
            username=secret_config["username"],
            password=secret_config["password"],
            database=secret_config["database"],
            warehouse=secret_config["warehouse"],
            role=secret_config["role"],
        )

    elif cache_type == "bigquery":
        temp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        secret_config = get_gsm_secret_json(
            secret_name="SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS",
        )
        try:
            # Write credentials to the temp file
            temp.write(secret_config["credentials_json"])
            temp.flush()
        finally:
            temp.close()

        cache = BigQueryCache(
            project_name=secret_config["project_id"],
            dataset_name=secret_config.get("dataset_id", "pyairbyte_integtest"),
            credentials_path=temp.name,
        )

    source = ab.get_source(
        "source-faker",
        config={"count": 5 * (10**e)},
        install_if_missing=False,
        streams=["purchases"],
    )
    source.check()

    source.read(cache)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run performance test reads.")
    parser.add_argument(
        "-e",
        type=int,
        help=(
            "The scale, as a power of 10. "
            "Recommended values: 2-3 (500 or 5_000) for read and overhead costs, "
            " 4-6 (50K or 5MM) for write throughput."
        ),
    )
    parser.add_argument(
        "--cache",
        type=str,
        help="The cache type to use.",
        choices=["duckdb", "snowflake", "bigquery"],
        default="duckdb",
    )
    args = parser.parse_args()

    main(e=args.e, cache_type=args.cache)
