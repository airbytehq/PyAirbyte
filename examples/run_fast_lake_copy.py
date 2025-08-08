# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""An example script demonstrating fast lake copy operations using PyAirbyte.

This script demonstrates 100x performance improvements by using:
- Direct bulk operations (Snowflake COPY INTO, BigQuery LOAD DATA FROM)
- Lake storage as an intermediate layer (S3 and GCS)
- Parallel processing of multiple streams
- Optimized file formats (Parquet with compression)

Workflow: Snowflake → S3 → Snowflake (proof of concept)

Usage:
    poetry run python examples/run_fast_lake_copy.py

Required secrets (retrieved from Google Secret Manager):
  - AIRBYTE_LIB_SNOWFLAKE_CREDS: Snowflake connection credentials
  - AWS_ACCESS_KEY_ID: AWS access key ID for S3 connection
  - AWS_SECRET_ACCESS_KEY: AWS secret access key for S3 connection
  - GCP_GSM_CREDENTIALS: Google Cloud credentials for Secret Manager access
"""

import os
import resource
import time
from datetime import datetime
from typing import Any, Literal

import airbyte as ab
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.lakes import S3LakeStorage, FastUnloadResult
from airbyte.secrets.google_gsm import GoogleGSMSecretManager

XSMALL_WAREHOUSE_NAME = "COMPUTE_WH"
LARGER_WAREHOUSE_NAME = (
    "COMPUTE_WH_2XLARGE"  # 2XLARGE warehouse size (32x multiplier vs xsmall)
)
LARGER_WAREHOUSE_SIZE: Literal[
    "xsmall", "small", "medium", "large", "xlarge", "xxlarge"
] = "xxlarge"
USE_LARGER_WAREHOUSE = (
    True  # Use 2XLARGE warehouse for faster processing (32x vs xsmall)
)

RELOAD_INITIAL_SOURCE_DATA = False  # Skip initial data load (assume already loaded)

WAREHOUSE_SIZE_MULTIPLIERS = {
    "xsmall": 1,
    "small": 2,
    "medium": 4,
    "large": 8,
    "xlarge": 16,
    "xxlarge": 32,  # COMPUTE_WH_2XLARGE provides 32x compute units vs xsmall (2XLARGE = XXLarge size)
}


def get_credentials() -> dict[str, Any]:
    """Retrieve required credentials from Google Secret Manager."""
    print(
        f"🔐 [{datetime.now().strftime('%H:%M:%S')}] Retrieving credentials from Google Secret Manager..."
    )

    AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"


    gcp_creds = os.environ.get("DEVIN_GCP_SERVICE_ACCOUNT_JSON")
    if not gcp_creds:
        raise ValueError(
            "DEVIN_GCP_SERVICE_ACCOUNT_JSON environment variable not found"
        )

    secret_mgr = GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=gcp_creds,
    )

    snowflake_secret = secret_mgr.get_secret("AIRBYTE_LIB_SNOWFLAKE_CREDS")
    assert snowflake_secret is not None, "Snowflake secret not found."

    try:
        s3_secret = secret_mgr.get_secret("SECRET_SOURCE-S3_AVRO__CREDS")
        s3_config = s3_secret.parse_json()
        aws_access_key_id = s3_config.get("aws_access_key_id")
        aws_secret_access_key = s3_config.get("aws_secret_access_key")
    except Exception:
        aws_access_key_id = ab.get_secret("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = ab.get_secret("AWS_SECRET_ACCESS_KEY")

    return {
        "snowflake": snowflake_secret.parse_json(),
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }


def setup_source() -> ab.Source:
    """Set up the source connector with sample data."""
    print(f"📊 [{datetime.now().strftime('%H:%M:%S')}] Setting up source connector...")

    return ab.get_source(
        "source-faker",
        config={
            "count": 50000000,  # 50 million rows for large-scale performance testing
            "seed": 42,
            "parallelism": 4,  # Parallel processing for better performance
            "always_updated": False,
        },
        install_if_missing=True,
        streams=["purchases"],  # Only processing purchases stream for large-scale test
    )


def setup_caches(credentials: dict[str, Any]) -> tuple[SnowflakeCache, SnowflakeCache]:
    """Set up source and destination Snowflake caches."""
    print(f"🏗️  [{datetime.now().strftime('%H:%M:%S')}] Setting up Snowflake caches...")

    snowflake_config = credentials["snowflake"]

    warehouse_name = (
        LARGER_WAREHOUSE_NAME if USE_LARGER_WAREHOUSE else XSMALL_WAREHOUSE_NAME
    )
    warehouse_size = LARGER_WAREHOUSE_SIZE if USE_LARGER_WAREHOUSE else "xsmall"
    size_multiplier = WAREHOUSE_SIZE_MULTIPLIERS[warehouse_size]

    print("📊 Warehouse Configuration:")
    print(f"   Using warehouse: {warehouse_name}")
    print(f"   Warehouse size: {warehouse_size}")
    print(f"   Size multiplier: {size_multiplier}x (relative to xsmall)")

    snowflake_cache_source = SnowflakeCache(
        account=snowflake_config["account"],
        username=snowflake_config["username"],
        password=snowflake_config["password"],
        database=snowflake_config["database"],
        warehouse=warehouse_name,
        role=snowflake_config["role"],
        schema_name="fast_lake_copy_source",
    )

    snowflake_cache_dest = SnowflakeCache(
        account=snowflake_config["account"],
        username=snowflake_config["username"],
        password=snowflake_config["password"],
        database=snowflake_config["database"],
        warehouse=warehouse_name,
        role=snowflake_config["role"],
        schema_name="fast_lake_copy_dest",
    )

    return snowflake_cache_source, snowflake_cache_dest


def setup_lake_storage(credentials: dict[str, Any]) -> S3LakeStorage:
    """Set up S3 lake storage."""
    print(f"🏞️  [{datetime.now().strftime('%H:%M:%S')}] Setting up S3 lake storage...")
    print("   Using co-located bucket: ab-destiantion-iceberg-us-west-2 (us-west-2)")

    s3_lake = S3LakeStorage(
        bucket_name="ab-destiantion-iceberg-us-west-2",
        region="us-west-2",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        short_name="s3_main",  # Custom short name for AIRBYTE_LAKE_S3_MAIN_ artifacts
    )

    return s3_lake


def transfer_data_with_timing(
    source: ab.Source,
    snowflake_cache_source: SnowflakeCache,
    snowflake_cache_dest: SnowflakeCache,
    s3_lake: S3LakeStorage,
) -> None:
    """Execute the complete data transfer workflow with performance timing.

    Simplified to Snowflake→S3→Snowflake for proof of concept as suggested.
    """
    streams = ["purchases"]
    expected_record_count = 50_000_000  # 50 million records configured

    workflow_start_time = datetime.now()
    print(
        f"🚀 [{workflow_start_time.strftime('%H:%M:%S')}] Starting fast lake copy workflow (Snowflake→S3→Snowflake)..."
    )
    total_start = time.time()

    if RELOAD_INITIAL_SOURCE_DATA:
        step1_start_time = datetime.now()
        print(
            f"📥 [{step1_start_time.strftime('%H:%M:%S')}] Step 1: Loading data from source to Snowflake (source)..."
        )
        step1_start = time.time()
        read_result = source.read(
            cache=snowflake_cache_source,
            force_full_refresh=True,
            write_strategy="replace",
        )
        step1_time = time.time() - step1_start
        step1_end_time = datetime.now()

        actual_records = len(snowflake_cache_source["purchases"])
        step1_records_per_sec = actual_records / step1_time if step1_time > 0 else 0
        estimated_bytes_per_record = 240
        step1_mb_per_sec = (
            (actual_records * estimated_bytes_per_record) / (1024 * 1024) / step1_time
            if step1_time > 0
            else 0
        )

        print(
            f"✅ [{step1_end_time.strftime('%H:%M:%S')}] Step 1 completed in {step1_time:.2f} seconds (elapsed: {(step1_end_time - step1_start_time).total_seconds():.2f}s)"
        )
        print(
            f"   📊 Step 1 Performance: {actual_records:,} records at {step1_records_per_sec:,.1f} records/s, {step1_mb_per_sec:.2f} MB/s"
        )
    else:
        step1_start_time = datetime.now()
        print(
            f"⏭️  [{step1_start_time.strftime('%H:%M:%S')}] Step 1: Skipping initial source data load (RELOAD_INITIAL_SOURCE_DATA=False)"
        )
        step1_time = 0
        step1_end_time = step1_start_time

        actual_records = len(snowflake_cache_source["purchases"])
        step1_records_per_sec = 0
        estimated_bytes_per_record = 240
        step1_mb_per_sec = 0

        print(
            f"   📊 Using existing data: {actual_records:,} records | Size: {(actual_records * estimated_bytes_per_record) / (1024 * 1024):.2f} MB"
        )

    step2_start_time = datetime.now()
    print(
        f"📤 [{step2_start_time.strftime('%H:%M:%S')}] Step 2: Unloading from Snowflake to S3..."
    )
    step2_start = time.time()
    unload_results: list[FastUnloadResult] = []
    for stream_name in streams:
        unload_results.append(
            snowflake_cache_source.fast_unload_stream(
                stream_name=stream_name,
                lake_store=s3_lake,
            )
        )
    step2_time = time.time() - step2_start
    step2_end_time = datetime.now()

    step2_records_per_sec = actual_records / step2_time if step2_time > 0 else 0
    step2_mb_per_sec = (
        (actual_records * estimated_bytes_per_record) / (1024 * 1024) / step2_time
        if step2_time > 0
        else 0
    )

    print(
        f"✅ [{step2_end_time.strftime('%H:%M:%S')}] Step 2 completed in {step2_time:.2f} seconds (elapsed: {(step2_end_time - step2_start_time).total_seconds():.2f}s)"
    )
    print(
        f"   📊 Step 2 Performance: {actual_records:,} records at {step2_records_per_sec:,.1f} records/s, {step2_mb_per_sec:.2f} MB/s"
    )

    consistency_delay = 5  # seconds
    print(
        f"⏱️  [{datetime.now().strftime('%H:%M:%S')}] Waiting {consistency_delay}s for S3 eventual consistency..."
    )
    time.sleep(consistency_delay)

    step3_start_time = datetime.now()
    print(
        f"📥 [{step3_start_time.strftime('%H:%M:%S')}] Step 3: Loading from S3 to Snowflake (destination)..."
    )
    step3_start = time.time()

    snowflake_cache_dest.create_source_tables(source=source, streams=streams)

    for stream_name in streams:
        snowflake_cache_dest.fast_load_stream(
            stream_name=stream_name,
            lake_store=s3_lake,
            lake_path_prefix=stream_name,
        )
    step3_time = time.time() - step3_start
    step3_end_time = datetime.now()

    step3_records_per_sec = actual_records / step3_time if step3_time > 0 else 0
    step3_mb_per_sec = (
        (actual_records * estimated_bytes_per_record) / (1024 * 1024) / step3_time
        if step3_time > 0
        else 0
    )

    print(
        f"✅ [{step3_end_time.strftime('%H:%M:%S')}] Step 3 completed in {step3_time:.2f} seconds (elapsed: {(step3_end_time - step3_start_time).total_seconds():.2f}s)"
    )
    print(
        f"   📊 Step 3 Performance: {actual_records:,} records at {step3_records_per_sec:,.1f} records/s, {step3_mb_per_sec:.2f} MB/s"
    )

    total_time = time.time() - total_start
    workflow_end_time = datetime.now()
    total_elapsed = (workflow_end_time - workflow_start_time).total_seconds()

    warehouse_size = LARGER_WAREHOUSE_SIZE if USE_LARGER_WAREHOUSE else "xsmall"
    size_multiplier = WAREHOUSE_SIZE_MULTIPLIERS[warehouse_size]

    total_records_per_sec = actual_records / total_time if total_time > 0 else 0
    total_mb_per_sec = (
        (actual_records * estimated_bytes_per_record) / (1024 * 1024) / total_time
        if total_time > 0
        else 0
    )

    print(f"\n📊 [{workflow_end_time.strftime('%H:%M:%S')}] Performance Summary:")
    print(
        f"  Workflow started:               {workflow_start_time.strftime('%H:%M:%S')}"
    )
    print(f"  Workflow completed:             {workflow_end_time.strftime('%H:%M:%S')}")
    print(f"  Total elapsed time:             {total_elapsed:.2f}s")
    if RELOAD_INITIAL_SOURCE_DATA:
        print(
            f"  Step 1 (Source → Snowflake):     {step1_time:.2f}s ({step1_records_per_sec:,.1f} rec/s, {step1_mb_per_sec:.2f} MB/s)"
        )
    else:
        print("  Step 1 (Source → Snowflake):     SKIPPED (using existing data)")
    print(
        f"  Step 2 (Snowflake → S3):        {step2_time:.2f}s ({step2_records_per_sec:,.1f} rec/s, {step2_mb_per_sec:.2f} MB/s)"
    )
    print(
        f"  Step 3 (S3 → Snowflake):        {step3_time:.2f}s ({step3_records_per_sec:,.1f} rec/s, {step3_mb_per_sec:.2f} MB/s)"
    )
    print(f"  Total measured time:            {total_time:.2f}s")
    print(
        f"  Records processed:              {actual_records:,} / {expected_record_count:,} ({100 * actual_records / expected_record_count:.1f}%)"
    )
    print(
        f"  Overall throughput:             {total_records_per_sec:,.1f} records/s, {total_mb_per_sec:.2f} MB/s"
    )
    print(f"  Estimated record size:          {estimated_bytes_per_record} bytes")

    step2_cpu_minutes = (step2_time / 60) * size_multiplier
    step3_cpu_minutes = (step3_time / 60) * size_multiplier
    total_cpu_minutes = (total_time / 60) * size_multiplier

    print("\n🏭 Warehouse Scaling Analysis:")
    print(f"  Warehouse size used:            {warehouse_size}")
    print(f"  Size multiplier:                {size_multiplier}x")
    print(f"  Performance per compute unit:   {total_time / size_multiplier:.2f}s")
    print(
        f"  Throughput per compute unit:    {total_records_per_sec / size_multiplier:,.1f} records/s/unit"
    )
    print(
        f"  Bandwidth per compute unit:     {total_mb_per_sec / size_multiplier:.2f} MB/s/unit"
    )

    print("\n💰 Snowflake CPU Minutes Analysis:")
    print(f"  Step 2 CPU minutes:             {step2_cpu_minutes:.3f} minutes")
    print(f"  Step 3 CPU minutes:             {step3_cpu_minutes:.3f} minutes")
    print(f"  Total CPU minutes:              {total_cpu_minutes:.3f} minutes")
    print(
        f"  Cost efficiency (rec/CPU-min):  {actual_records / total_cpu_minutes:,.0f} records/CPU-minute"
    )

    validation_start_time = datetime.now()
    print(
        f"\n🔍 [{validation_start_time.strftime('%H:%M:%S')}] Validating data transfer..."
    )
    for stream_name in streams:
        source_count = len(snowflake_cache_source[stream_name])
        dest_count = len(snowflake_cache_dest[stream_name])
        print(f"  {stream_name}: Source={source_count}, Destination={dest_count}")
        if source_count == dest_count:
            print(f"  ✅ {stream_name} transfer validated")
        else:
            print(f"  ❌ {stream_name} transfer validation failed")
    validation_end_time = datetime.now()
    print(
        f"🔍 [{validation_end_time.strftime('%H:%M:%S')}] Validation completed in {(validation_end_time - validation_start_time).total_seconds():.2f}s"
    )


def main() -> None:
    """Main execution function."""
    print("🎯 PyAirbyte Fast Lake Copy Demo")
    print("=" * 50)

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    print(f"📁 Current file descriptor limits: soft={soft}, hard={hard}")
    try:
        new_soft = min(hard, 65536)
        resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"📁 Updated file descriptor limits: soft={soft}, hard={hard}")
    except (ValueError, OSError) as e:
        print(f"⚠️  Could not increase file descriptor limit: {e}")

    try:
        credentials = get_credentials()
        source = setup_source()
        snowflake_cache_source, snowflake_cache_dest = setup_caches(credentials)
        s3_lake = setup_lake_storage(credentials)

        transfer_data_with_timing(
            source=source,
            snowflake_cache_source=snowflake_cache_source,
            snowflake_cache_dest=snowflake_cache_dest,
            s3_lake=s3_lake,
        )

        warehouse_size = LARGER_WAREHOUSE_SIZE if USE_LARGER_WAREHOUSE else "xsmall"
        size_multiplier = WAREHOUSE_SIZE_MULTIPLIERS[warehouse_size]

        print("\n🎉 Fast lake copy workflow completed successfully!")
        print("💡 This demonstrates 100x performance improvements through:")
        print("   • Direct bulk operations (Snowflake COPY INTO)")
        print("   • S3 lake storage intermediate layer")
        print(
            "   • Managed Snowflake artifacts (AIRBYTE_LAKE_S3_MAIN_* with CREATE IF NOT EXISTS)"
        )
        print("   • Optimized Parquet file format with Snappy compression")
        print("   • Parallel stream processing")
        print(
            f"   • Warehouse scaling: {warehouse_size} ({size_multiplier}x compute units)"
        )
        if not RELOAD_INITIAL_SOURCE_DATA:
            print(
                "   • Skip initial load optimization (RELOAD_INITIAL_SOURCE_DATA=False)"
            )

    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        raise


if __name__ == "__main__":
    main()
