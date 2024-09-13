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

You can also use this script to test destination load performance:

```bash
# Load 5_000 records to BigQuery
poetry run python ./examples/run_perf_test_reads.py -e=5 --destination=e2e
```

Testing raw PyAirbyte throughput with and without caching:

```bash
# Test raw PyAirbyte throughput with caching (Source->Cache):
poetry run python ./examples/run_perf_test_reads.py -e=5
# Test raw PyAirbyte throughput without caching (Source->Destination):
poetry run python ./examples/run_perf_test_reads.py -e=5 --destination=e2e --no-cache
```

Testing Python CDK throughput:

```bash
# Test max throughput:
poetry run python ./examples/run_perf_test_reads.py -n=2400000 --source=hardcoded --destination=e2e
# Analyze tracing data:
poetry run viztracer --open -- ./examples/run_perf_test_reads.py -e=3 --source=hardcoded --destination=e2e
```


Note:
- The Faker stream ('purchases') is assumed to be 220 bytes, meaning 4_500 records is
  approximately 1 MB. Based on this: 25K records/second is approximately 5.5 MB/s.
- The E2E stream is assumed to be 180 bytes, meaning 5_500 records is
  approximately 1 MB. Based on this: 40K records/second is approximately 7.2 MB/s
  and 60K records/second is approximately 10.5 MB/s.
"""

from __future__ import annotations

import argparse
import tempfile
from typing import TYPE_CHECKING

import airbyte as ab
from airbyte.caches import BigQueryCache, CacheBase, SnowflakeCache
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from typing_extensions import Literal

if TYPE_CHECKING:
    from airbyte.sources.base import Source


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


def get_cache(
    cache_type: Literal["duckdb", "snowflake", "bigquery", False],
) -> CacheBase | Literal[False]:
    if cache_type is False:
        return False

    if cache_type == "duckdb":
        return ab.new_local_cache()

    if cache_type == "snowflake":
        secret_config = get_gsm_secret_json(
            secret_name="AIRBYTE_LIB_SNOWFLAKE_CREDS",
        )
        return SnowflakeCache(
            account=secret_config["account"],
            username=secret_config["username"],
            password=secret_config["password"],
            database=secret_config["database"],
            warehouse=secret_config["warehouse"],
            role=secret_config["role"],
        )

    if cache_type == "bigquery":
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

        return BigQueryCache(
            project_name=secret_config["project_id"],
            dataset_name=secret_config.get("dataset_id", "pyairbyte_integtest"),
            credentials_path=temp.name,
        )

    raise ValueError(f"Unknown cache type: {cache_type}")  # noqa: TRY003


def get_source(
    source_alias: str,
    num_records: int,
) -> Source:
    if source_alias == "faker":
        return ab.get_source(
            "source-faker",
            config={"count": num_records},
            install_if_missing=False,
            streams=["purchases"],
        )

    if source_alias == "e2e":
        return ab.get_source(
            "source-e2e",
            docker_image="airbyte/source-e2e-test:latest",
            streams="*",
            config={
                "type": "BENCHMARK",
                "schema": "FIVE_STRING_COLUMNS",
                "terminationCondition": {
                    "type": "MAX_RECORDS",
                    "max": num_records,
                },
            },
        )

    if source_alias == "hardcoded":
        return ab.get_source(
            "source-hardcoded-records",
            streams=["dummy_fields"],
            config={
                "count": num_records,
            },
        )

    if source_alias in ["postgres", "pg"]:
        pg_secret_config = get_gsm_secret_json(
            secret_name="SECRET_SOURCE_POSTGRES_PERFORMANCE_TEST_CREDS",
        )
        pg_secret_config["schemas"] = ["postgres"]
        pg_secret_config["replication_method"] = {"method": "Standard"}
        source = ab.get_source(
            "source-postgres",
            config=pg_secret_config,
            # streams=["dummy_fields"],
        )
        # TODO: Deleteme after debugging:
        source.print_config_spec()
        return source

    raise ValueError(f"Unknown source alias: {source_alias}")  # noqa: TRY003


def get_destination(destination_type: str) -> ab.Destination:
    if destination_type == "e2e":
        return ab.get_destination(
            name="destination-e2e-test",
            config={
                "test_destination": {
                    "test_destination_type": "LOGGING",
                    "logging_config": {
                        "logging_type": "FirstN",
                        "max_entry_count": 100,
                    },
                }
            },
            docker_image="airbyte/destination-e2e-test:latest",
        )

    raise ValueError(f"Unknown destination type: {destination_type}")  # noqa: TRY003


def main(
    e: int | None = None,
    n: int | None = None,
    cache_type: Literal["duckdb", "bigquery", "snowflake", False] = "duckdb",
    source_alias: str = "e2e",
    destination_type: str | None = None,
) -> None:
    num_records: int = n or 5 * (10 ** (e or 3))
    cache_type = "duckdb" if cache_type is None else cache_type

    cache: CacheBase | Literal[False] = get_cache(
        cache_type=cache_type,
    )
    source: Source = get_source(
        source_alias=source_alias,
        num_records=num_records,
    )
    source.check()
    if destination_type:
        destination = get_destination(destination_type=destination_type)
    if cache is not False:
        read_result = source.read(cache)
        if destination_type:
            destination.write(read_result)
    else:
        destination.write(source, cache=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run performance test reads.")
    parser.add_argument(
        "-e",
        type=int,
        help=(
            "The scale, as a power of 10."
            "Recommended values: 2-3 (500 or 5_000) for read and overhead costs, "
            " 4-6 (50K or 5MM) for write throughput. "
            "This is mutually exclusive with -n."
        ),
    )
    parser.add_argument(
        "-n",
        type=int,
        help=(
            "The number of records to generate in the source. "
            "This is mutually exclusive with -e."
        ),
    )
    parser.add_argument(
        "--cache",
        type=str,
        help="The cache type to use.",
        choices=["duckdb", "snowflake", "bigquery"],
        default="duckdb",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable caching.",
    )
    parser.add_argument(
        "--source",
        type=str,
        help=(
            "The cache type to use. The `e2e` source is recommended when Docker is available, "
            "while the `faker` source runs natively in Python. The 'hardcoded' source is "
            "similar to the 'e2e' source, but written in Python."
        ),
        choices=["faker", "e2e", "hardcoded", "pg", "postgres"],
        default="hardcoded",
    )
    parser.add_argument(
        "--destination",
        type=str,
        help=("The destination to use (optional)."),
        choices=["e2e"],
        default=None,
    )
    args = parser.parse_args()

    main(
        e=args.e,
        n=args.n,
        cache_type=args.cache if not args.no_cache else False,
        source_alias=args.source,
        destination_type=args.destination,
    )
