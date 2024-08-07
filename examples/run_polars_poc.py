# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A test of Polars in PyAirbyte.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_polars_poc.py
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, cast

import airbyte as ab
import boto3
import polars as pl
from airbyte import get_source
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from typing_extensions import Literal

logger = logging.getLogger()
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Basic logging configuration
logger.setLevel(logging.INFO)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and set it for the handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
SECRET_NAME = "SECRET_SOURCE-S3_V4_JSONL_NEWLINE__CREDS"

secret_mgr = GoogleGSMSecretManager(
    project=AIRBYTE_INTERNAL_GCP_PROJECT,
    credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
)
secret_handle = secret_mgr.get_secret(SECRET_NAME)
assert secret_handle is not None, "Secret not found."
secret_config = secret_handle.parse_json()

# BUCKET_NAME = "performance-test-datasets"  # << Can't find this bucket
BUCKET_NAME = "airbyte-internal-performance"  # << From 'dataline-dev', until we find the other bucket
FILE_NAME_PREFIX = "json/no_op_source/stream1/2024_08_06_18/"
GLOB = f"{FILE_NAME_PREFIX}*.jsonl"
SAMPLE_FILE_1 = "json/no_op_source/stream1/2024_08_06_18/15836041.0.1722968833150.jsonl"
SAMPLE_FILE_2 = "json/no_op_source/stream1/2024_08_06_18/15836041.1.1722968852741.jsonl"
SAMPLE_FILES = [SAMPLE_FILE_1, SAMPLE_FILE_2]

records_processed = 0


def get_my_source() -> ab.Source:
    """Take the existing S3 config and modify it to use the perf-test bucket and glob."""
    secret_config["bucket"] = BUCKET_NAME
    secret_config["streams"][0] = {
        "name": "stream1",
        "format": {"filetype": "jsonl"},
        "globs": ["json/no_op_source/stream1/**/*.jsonl"],
    }
    return get_source(
        "source-s3",
        config=secret_config,
        streams="*",
    )


def test_my_source() -> None:
    """Test the modified S3 source."""
    source = get_my_source()
    source.check()
    source.select_all_streams()
    source.read()


def get_s3_file_names(
    bucket_name: str,
    path_prefix: str,
) -> list[str]:
    """Get the names of all files in an S3 bucket with a given prefix."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=secret_config["aws_access_key_id"],
        aws_secret_access_key=secret_config["aws_secret_access_key"],
        region_name="us-east-2",
    )

    # List all objects in the S3 bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)

    # Filter the objects to match the file prefix
    matching_files = [match["Key"] for match in response.get("Contents", [])]

    return matching_files


def get_polars_df(
    s3_file_path: str,
    *,
    lazy: bool = True,
    expire_cache: bool,
) -> pl.LazyFrame | pl.DataFrame:
    """Set up a Polars lazy DataFrame from an S3 file.

    This action will connect to the S3 bucket but it will not read data from the file until
    the DataFrame is actually used.
    """
    read_fn: Callable[..., pl.LazyFrame | pl.DataFrame]
    if lazy:
        read_fn = pl.scan_ndjson
    else:
        read_fn = pl.read_ndjson

    return read_fn(
        source=s3_file_path,  # TODO: Try to get this working with globs
        storage_options={
            "aws_access_key_id": secret_config["aws_access_key_id"],
            "aws_secret_access_key": secret_config["aws_secret_access_key"],
            "region": "us-east-2",
        },
        include_file_paths="_ab_source_file_path",
        row_index_name="_ab_record_index",
        row_index_offset=0,
        infer_schema_length=100,
        file_cache_ttl=0 if expire_cache else None,
    )


def add_custom_transforms(
    df: pl.LazyFrame | pl.DataFrame,
) -> pl.LazyFrame | pl.DataFrame:
    """Add custom transforms to the Polars lazy DataFrame."""
    return df.with_columns(
        [
            pl.lit("Hello, world!").alias("greeting"),
            pl.col("_airbyte_ab_id").str.to_uppercase(),
            pl.lit(datetime.now()).alias("current_timestamp"),
        ],
    )


def write_files(
    s3_urls: list[str],
    file_type: Literal["jsonl", "parquet"] = "jsonl",
    *,
    expire_cache: bool,
    lazy: bool = True,
) -> None:
    global records_processed
    dataframes: list[pl.DataFrame] = []
    for n, s3_url in enumerate(s3_urls, start=1):
        base_name = ".".join(Path(s3_url).name.split(".")[:-1])
        output_file = f"polars-perf-test-artifact.{base_name}.{file_type}"
        if Path(output_file).exists():
            Path(output_file).unlink()

        df: pl.LazyFrame | pl.DataFrame = get_polars_df(
            s3_file_path=f"s3://{BUCKET_NAME}/{SAMPLE_FILE_2}",
            # expected_schema=polars_stream_schema,
            expire_cache=expire_cache,
            lazy=lazy,
        )

        if lazy:
            df = add_custom_transforms(df)
            assert isinstance(df, pl.LazyFrame)
            logger.info(f"Collecting file {n} of {len(s3_urls)} to '{output_file}'...")
            df = df.collect()
            logger.info(f"Writing file {n} of {len(s3_urls)} to '{output_file}'...")

            # Write the DataFrame to a file
            if file_type == "parquet":
                df.write_parquet(output_file)
            elif file_type == "jsonl":
                df.write_ndjson(output_file)
            else:
                raise ValueError(f"Invalid file type: {file_type}")

            records_processed += df.height
            del df
        else:
            assert isinstance(df, pl.DataFrame)
            dataframes.append(df)

    if not lazy:
        combined_df: pl.DataFrame = pl.concat(dataframes)
        combined_df = cast(pl.DataFrame, add_custom_transforms(combined_df))
        if file_type == "parquet":
            combined_df.write_parquet(output_file)
        elif file_type == "jsonl":
            combined_df.write_ndjson(output_file)
        else:
            raise ValueError(f"Invalid file type: {file_type}")

        records_processed += combined_df.height


def run_polars_perf_test(
    file_type: Literal["jsonl", "parquet"] = "jsonl",
    *,
    expire_cache: bool,
    lazy: bool = True,
) -> None:
    """Run the Polars proof of concept."""
    global records_processed

    logger.info("Finding S3 files...")

    s3_urls = get_s3_file_names(BUCKET_NAME, FILE_NAME_PREFIX)
    logger.info("Creating polars dataframes...")
    start_time = time.time()

    write_files(
        s3_urls,
        file_type=file_type,
        lazy=lazy,
        expire_cache=expire_cache,
    )

    logger.info("Finished write operation.")
    elapsed_transfer_time = time.time() - start_time

    mb_per_record = 180 / (1024 * 1024)  # << 180 bytes per record, converted to MB
    logger.info(
        f"Wrote {records_processed:,} records from {len(s3_urls)} files in"
        f" {elapsed_transfer_time:,.2f} seconds."
        f" ({records_processed / elapsed_transfer_time:,.2f} records per second,"
        f" {(records_processed / elapsed_transfer_time) * mb_per_record:,.1f} MB/s,"
        f" {records_processed * mb_per_record:,.1f} MB total)"
    )


def main() -> None:
    """Run the Polars proof of concept."""
    # test_my_source()  # We don't need to run this every time - only for debugging
    run_polars_perf_test(
        file_type="parquet",
        lazy=False,
        expire_cache=False,
    )


if __name__ == "__main__":
    main()
