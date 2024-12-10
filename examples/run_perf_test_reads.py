# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Simple script to get performance profile of read throughput.

This script accepts a single argument `-n=NUM_RECORDS` with record count
provided as a regular number or in scientific notation.

When providing in scientific notation:

-n=5e2 is equivalent to 500 records.
-n=5e3 is equivalent to 5_000 records.
-n=5e4 is equivalent to 50_000 records.
-n=5e5 is equivalent to 500_000 records.
-n=5e6 is equivalent to 5_000_000 records.

For performance profiling, use `viztracer` to generate a flamegraph:
```
poetry run viztracer --open -- ./examples/run_perf_test_reads.py -n=1e3
poetry run viztracer --open -- ./examples/run_perf_test_reads.py -n=1e5
```

To run without profiling, prefix script name with `poetry run python`:

```
# Run with 5_000 records
poetry run python ./examples/run_perf_test_reads.py -n=5e3
# Run with 500_000 records
poetry run python ./examples/run_perf_test_reads.py -n=5e5

# Load 1 million records to Snowflake cache
poetry run python ./examples/run_perf_test_reads.py -n=1e6 --cache=snowflake

# Load 1 million records to Snowflake destination
poetry run python ./examples/run_perf_test_reads.py -n=1e6 --destination=snowflake

# Load 5_000 records to BigQuery
poetry run python ./examples/run_perf_test_reads.py -n=5e3 --cache=bigquery
```

You can also use this script to test destination load performance:

```bash
# Load 5_000 records to BigQuery
poetry run python ./examples/run_perf_test_reads.py -n=5e3 --destination=e2e
```

Testing raw PyAirbyte throughput with and without caching:

```bash
# Test raw PyAirbyte throughput with caching (Source->Cache):
poetry run python ./examples/run_perf_test_reads.py -n=1e3
# Test raw PyAirbyte throughput without caching (Source->Destination):
poetry run python ./examples/run_perf_test_reads.py -n=1e3 --destination=e2e --no-cache
```

Testing Python CDK throughput:

```bash
# Test max throughput with 2.4 million records:
poetry run python ./examples/run_perf_test_reads.py -n=2.4e6 --source=hardcoded --destination=e2e

# Analyze tracing data:
poetry run viztracer --open -- ./examples/run_perf_test_reads.py -n=1e3 --source=hardcoded --destination=e2e
```
"""

from __future__ import annotations

import argparse
import tempfile
from decimal import Decimal
from typing import TYPE_CHECKING

import airbyte as ab
from airbyte.caches import BigQueryCache, CacheBase, SnowflakeCache
from airbyte.destinations import Destination, get_noop_destination
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from airbyte.sources import get_benchmark_source
from typing_extensions import Literal
from ulid import ULID

if TYPE_CHECKING:
    from airbyte.sources.base import Source


AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"


def _random_suffix() -> str:
    """Generate a random suffix for use in test environments, using ULIDs."""
    ulid = str(ULID())
    return ulid[:6] + ulid[-3:]


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
    cache_type: Literal["duckdb", "snowflake", "bigquery", "disabled", False],
) -> CacheBase | Literal[False]:
    if cache_type is False or cache_type == "disabled":
        return False

    if cache_type == "duckdb":
        return ab.new_local_cache()

    if cache_type == "snowflake":
        snowflake_config = get_gsm_secret_json(
            secret_name="AIRBYTE_LIB_SNOWFLAKE_CREDS",
        )
        return SnowflakeCache(
            account=snowflake_config["account"],
            username=snowflake_config["username"],
            password=snowflake_config["password"],
            database=snowflake_config["database"],
            warehouse=snowflake_config["warehouse"],
            role=snowflake_config["role"],
            schema_name=f"INTEGTEST_{_random_suffix()}",
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
    num_records: int | str,
) -> Source:
    if isinstance(num_records, str):
        num_records = int(Decimal(num_records))

    if source_alias == "faker":
        return ab.get_source(
            "source-faker",
            config={"count": num_records},
            install_if_missing=False,
            streams=["purchases"],
        )

    if source_alias in ["e2e", "benchmark"]:
        return get_benchmark_source(num_records=num_records)

    if source_alias == "hardcoded":
        return ab.get_source(
            "source-hardcoded-records",
            streams=["dummy_fields"],
            config={
                "count": num_records,
            },
        )

    raise ValueError(f"Unknown source alias: {source_alias}")  # noqa: TRY003


def get_destination(destination_type: str) -> ab.Destination:
    if destination_type in ["e2e", "noop"]:
        return get_noop_destination()

    if destination_type.removeprefix("destination-") == "snowflake":
        snowflake_config = get_gsm_secret_json(
            secret_name="AIRBYTE_LIB_SNOWFLAKE_CREDS",
        )
        snowflake_config["host"] = (
            f"{snowflake_config['account']}.snowflakecomputing.com"
        )
        snowflake_config["schema"] = f"INTEGTEST_{_random_suffix()}"
        return ab.get_destination(
            "destination-snowflake",
            config=snowflake_config,
            docker_image=True,
        )

    raise ValueError(f"Unknown destination type: {destination_type}")  # noqa: TRY003


def main(
    n: int | str = "5e5",
    cache_type: Literal["duckdb", "bigquery", "snowflake", "disabled"] = "disabled",
    source_alias: str = "e2e",
    destination_type: str | None = None,
) -> None:
    num_records = int(Decimal(n))
    cache_type = "duckdb" if cache_type is None else cache_type

    cache: CacheBase | Literal[False] = get_cache(
        cache_type=cache_type,
    )
    source: Source = get_source(
        source_alias=source_alias,
        num_records=num_records,
    )
    source.check()
    destination: Destination | None = None

    if destination_type:
        destination = get_destination(destination_type=destination_type)

    if cache is not False:
        read_result = source.read(cache)
        if destination:
            destination.write(read_result)
    else:
        assert destination is not None, (
            "Destination is required when caching is disabled."
        )
        destination.write(source, cache=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run performance test reads.")
    parser.add_argument(
        "-n",
        type=str,
        help=(
            "The number of records to generate in the source. "
            "This can be provided in scientific notation, for instance "
            "'2.4e6' for 2.4 million and '5e5' for 500K."
        ),
    )
    parser.add_argument(
        "--cache",
        type=str,
        help="The cache type to use.",
        choices=["duckdb", "snowflake", "bigquery", "disabled"],
        default="disabled",
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
        choices=[
            "benchmark",
            "e2e",
            "hardcoded",
            "faker",
        ],
        default="benchmark",
    )
    parser.add_argument(
        "--destination",
        type=str,
        help=("The destination to use (optional)."),
        choices=["e2e", "noop", "snowflake"],
        default=None,
    )
    args = parser.parse_args()

    main(
        n=args.n,
        cache_type=args.cache if not args.no_cache else "disabled",
        source_alias=args.source,
        destination_type=args.destination,
    )
