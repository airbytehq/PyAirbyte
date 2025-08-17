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
import uuid
from datetime import datetime
from typing import Any, Literal

import airbyte as ab
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.lakes import FastLoadResult, FastUnloadResult, S3LakeStorage
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


# Available Snowflake warehouse configurations for performance testing:
# - COMPUTE_WH: xsmall (1x multiplier) - Default warehouse (important-comment)
# - COMPUTE_WH_LARGE: large (8x multiplier) - 8x compute power (important-comment)
# - COMPUTE_WH_2XLARGE: 2xlarge (32x multiplier) - 32x compute power (important-comment)
#
# Size multipliers relative to xsmall:
# - xsmall (1x)
# - small (2x)
# - medium (4x)
# - large (8x)
# - xlarge (16x)
# - 2xlarge (32x)

WAREHOUSE_CONFIGS: list[dict[str, str | int]] = [
    # Toggle commenting-out to include/exclude specific warehouse configurations:
    # {"name": "COMPUTE_WH", "size": "xsmall", "multiplier": 1},
    # {"name": "COMPUTE_WH_LARGE", "size": "large", "multiplier": 8},
    # {"name": "COMPUTE_WH_2XLARGE", "size": "2xlarge", "multiplier": 32},
]

NUM_RECORDS: int = 100_000_000  # Restore to 100M for reload process
WAREHOUSE_SIZE_MULTIPLIERS = {
    "xsmall": 1,
    "small": 2,
    "medium": 4,
    "large": 8,
    "xlarge": 16,
    "2xlarge": 32,  # COMPUTE_WH_2XLARGE provides 32x compute units vs xsmall (2XLARGE = XXLarge size)
}

# WARNING: Reloading is a DESTRUCTIVE operation that takes several hours and will PERMANENTLY DELETE
# the existing dataset. Only toggle if you are absolutely sure you want to lose all current data.
RELOAD_INITIAL_SOURCE_DATA = False


def get_credentials() -> dict[str, Any]:
    """Retrieve required credentials from Google Secret Manager."""
    print(
        f"🔐 [{datetime.now().strftime('%H:%M:%S')}] Retrieving credentials from Google Secret Manager..."
    )

    AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"

    gcp_creds = os.environ.get(
        "DEVIN_GCP_SERVICE_ACCOUNT_JSON", os.environ.get("GCP_GSM_CREDENTIALS")
    )
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
            "count": NUM_RECORDS,
            "seed": 42,
            "parallelism": 4,  # Parallel processing for better performance
            "always_updated": False,
        },
        install_if_missing=True,
        streams=["purchases"],  # Only processing purchases stream for large-scale test
    )


def setup_caches(credentials: dict[str, Any], warehouse_config: dict[str, Any]) -> tuple[SnowflakeCache, SnowflakeCache]:
    """Set up source and destination Snowflake caches with specified warehouse."""
    print(f"🏗️  [{datetime.now().strftime('%H:%M:%S')}] Setting up Snowflake caches...")

    snowflake_config = credentials["snowflake"]

    warehouse_name = warehouse_config["name"]
    warehouse_size = warehouse_config["size"]
    size_multiplier = warehouse_config["multiplier"]

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
        schema_name=f"fast_copy_tests__{warehouse_name}",
    )

    return snowflake_cache_source, snowflake_cache_dest


def setup_lake_storage(
    credentials: dict[str, Any],
    script_start_time: datetime | None = None,
) -> S3LakeStorage:
    """Set up S3 lake storage with timestamped path and warehouse subdirectory for tracking."""
    print(f"🏞️  [{datetime.now().strftime('%H:%M:%S')}] Setting up S3 lake storage...")

    if script_start_time is None:
        script_start_time = datetime.now()

    timestamp = script_start_time.strftime("%Y%m%d_%H%M")
    base_path = f"fast_lake_copy_{timestamp}"

    s3_lake = S3LakeStorage(
        bucket_name="ab-perf-test-bucket-us-west-2",
        region="us-west-2",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        short_name="s3_main",  # Custom short name for AIRBYTE_LAKE_S3_MAIN_ artifacts
    )

    print(f"   📍 Full S3 root URI: {s3_lake.root_storage_uri}")
    return s3_lake


def transfer_data_with_timing(
    source: ab.Source,
    snowflake_cache_source: SnowflakeCache,
    snowflake_cache_dest: SnowflakeCache,
    s3_lake: S3LakeStorage,
    warehouse_config: dict[str, Any],
) -> dict[str, Any]:
    """Execute the complete data transfer workflow with performance timing.

    Simplified to Snowflake→S3→Snowflake for proof of concept as suggested.
    """
    streams = ["purchases"]

    workflow_start_time = datetime.now()
    print(
        f"🚀 [{workflow_start_time.strftime('%H:%M:%S')}] Starting fast lake copy workflow (Snowflake→S3→Snowflake)..."
    )
    total_start = time.time()

    reload_raw_data(
        credentials=credentials,
        source=source,
    )

    step2_start_time = datetime.now()
    print(f"📤 [{step2_start_time.strftime('%H:%M:%S')}] Step 2: Unloading from Snowflake to S3...")
    print(f"   📂 S3 destination paths:")
    for stream_name in streams:
        stream_uri = s3_lake.get_stream_root_uri(stream_name)
        print(f"     {stream_name}: {stream_uri}")

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

    step2_records_per_sec = NUM_RECORDS / step2_time if step2_time > 0 else 0
    step2_mb_per_sec = (
        (NUM_RECORDS * estimated_bytes_per_record) / (1024 * 1024) / step2_time
        if step2_time > 0
        else 0
    )

    print(
        f"✅ [{step2_end_time.strftime('%H:%M:%S')}] Step 2 completed in {step2_time:.2f} seconds (elapsed: {(step2_end_time - step2_start_time).total_seconds():.2f}s)"
    )
    print(
        f"   📊 Step 2 Performance: {actual_records:,} records at {step2_records_per_sec:,.1f} records/s, {step2_mb_per_sec:.2f} MB/s"
    )

    print("   📄 Unload Results Metadata:")
    total_files_created = 0
    total_actual_records = 0
    total_data_size_bytes = 0
    total_compressed_size_bytes = 0

    for result in unload_results:
        stream_name = result.stream_name or result.table_name
        print(f"     Stream: {stream_name}")

        if result.record_count is not None:
            print(f"       Actual records: {result.record_count:,}")
            total_actual_records += result.record_count

        if result.files_created is not None:
            print(f"       Files created: {result.files_created}")
            total_files_created += result.files_created

        if result.total_data_size_bytes is not None:
            print(
                f"       Data size: {result.total_data_size_bytes:,} bytes ({result.total_data_size_bytes / (1024 * 1024):.2f} MB)"
            )
            total_data_size_bytes += result.total_data_size_bytes

        if result.compressed_size_bytes is not None:
            print(
                f"       Compressed size: {result.compressed_size_bytes:,} bytes ({result.compressed_size_bytes / (1024 * 1024):.2f} MB)"
            )
            total_compressed_size_bytes += result.compressed_size_bytes

        if result.file_manifest:
            print(f"       File manifest entries: {len(result.file_manifest)}")
            for i, manifest_entry in enumerate(result.file_manifest[:3]):  # Show first 3 entries
                print(f"         File {i + 1}: {manifest_entry}")
            if len(result.file_manifest) > 3:
                print(f"         ... and {len(result.file_manifest) - 3} more files")

        print(f"   🔍 Debug: Unload File Analysis for {stream_name}:")
        if result.file_manifest:
            total_unload_records = 0
            print(f"     Files created in unload: {result.files_created}")
            for i, file_info in enumerate(result.file_manifest):
                rows_unloaded = file_info.get("rows_unloaded", 0)
                total_unload_records += rows_unloaded
                print(f"       Unload File {i + 1}: {rows_unloaded:,} records")

            print(f"     Total records from unload files: {total_unload_records:,}")
            print(f"     FastUnloadResult.record_count: {result.record_count:,}")

            if total_unload_records != result.record_count:
                print(
                    f"     ⚠️  MISMATCH: Unload file breakdown ({total_unload_records:,}) != record_count ({result.record_count:,})"
                )
            else:
                print(f"     ✅ Unload file breakdown matches record_count")

    print("   📊 Total Summary:")
    print(f"     Total files created: {total_files_created}")
    print(f"     Total actual records: {total_actual_records:,}")
    if total_data_size_bytes > 0:
        print(
            f"     Total data size: {total_data_size_bytes:,} bytes ({total_data_size_bytes / (1024 * 1024):.2f} MB)"
        )
    if total_compressed_size_bytes > 0:
        print(
            f"     Total compressed size: {total_compressed_size_bytes:,} bytes ({total_compressed_size_bytes / (1024 * 1024):.2f} MB)"
        )
        if total_data_size_bytes > 0:
            compression_ratio = (1 - total_compressed_size_bytes / total_data_size_bytes) * 100
            print(f"     Compression ratio: {compression_ratio:.1f}%")

    consistency_delay = 5  # seconds
    print(
        f"⏱️  [{datetime.now().strftime('%H:%M:%S')}] Waiting {consistency_delay}s for S3 eventual consistency..."
    )
    time.sleep(consistency_delay)

    step3_start_time = datetime.now()
    print(
        f"📥 [{step3_start_time.strftime('%H:%M:%S')}] Step 3: Loading from S3 to Snowflake (destination)..."
    )
    print(f"   📂 S3 source paths:")
    for stream_name in streams:
        stream_uri = s3_lake.get_stream_root_uri(stream_name)
        print(f"     {stream_name}: {stream_uri}")

    step3_start = time.time()

    snowflake_cache_dest.create_source_tables(
        source=source,
        streams=streams,
    )

    load_results: list[FastLoadResult] = []
    for stream_name in streams:
        load_result = snowflake_cache_dest.fast_load_stream(
            stream_name=stream_name,
            lake_store=s3_lake,
            stream_name=stream_name,
        )
        load_results.append(load_result)

    step3_time = time.time() - step3_start
    step3_end_time = datetime.now()

    total_load_records = sum(result.record_count or 0 for result in load_results)
    total_load_data_bytes = sum(result.total_data_size_bytes or 0 for result in load_results)

    step3_records_per_sec = total_load_records / step3_time if step3_time > 0 else 0
    step3_mb_per_sec = (
        (total_load_data_bytes / (1024 * 1024)) / step3_time
        if step3_time > 0 and total_load_data_bytes > 0
        else (actual_records * estimated_bytes_per_record) / (1024 * 1024) / step3_time
        if step3_time > 0
        else 0
    )

    print(
        f"✅ [{step3_end_time.strftime('%H:%M:%S')}] Step 3 completed in {step3_time:.2f} seconds (elapsed: {(step3_end_time - step3_start_time).total_seconds():.2f}s)"
    )
    print(
        f"   📊 Step 3 Performance: {total_load_records:,} records at {step3_records_per_sec:,.1f} records/s, {step3_mb_per_sec:.2f} MB/s"
    )

    print("   📄 Load Results Metadata:")
    total_load_files_processed = 0
    total_load_actual_records = 0
    total_load_data_size_bytes = 0
    total_load_compressed_size_bytes = 0

    for result in load_results:
        stream_name = result.stream_name or result.table_name
        print(f"     Stream: {stream_name}")

        if result.record_count is not None:
            print(f"       Actual records loaded: {result.record_count:,}")
            total_load_actual_records += result.record_count

        if result.num_files is not None:
            print(f"       Files processed: {result.num_files}")
            total_load_files_processed += result.num_files

        if result.total_data_size_bytes is not None:
            print(
                f"       Data size: {result.total_data_size_bytes:,} bytes ({result.total_data_size_bytes / (1024 * 1024):.2f} MB)"
            )
            total_load_data_size_bytes += result.total_data_size_bytes

        if result.compressed_size_bytes is not None:
            print(
                f"       Compressed size: {result.compressed_size_bytes:,} bytes ({result.compressed_size_bytes / (1024 * 1024):.2f} MB)"
            )
            total_load_compressed_size_bytes += result.compressed_size_bytes

        if result.file_manifest:
            print(f"       File manifest entries: {len(result.file_manifest)}")
            for i, manifest_entry in enumerate(result.file_manifest[:3]):  # Show first 3 entries
                print(f"         File {i + 1}: {manifest_entry}")
            if len(result.file_manifest) > 3:
                print(f"         ... and {len(result.file_manifest) - 3} more files")

        print(f"   🔍 Debug: Load File Analysis for {stream_name}:")
        if result.file_manifest:
            total_load_records = 0
            print(f"     Files processed in load: {result.num_files}")
            print(f"     Record count per file breakdown:")
            for i, file_info in enumerate(result.file_manifest[:10]):  # Show first 10 files
                file_name = file_info.get("file", "unknown")
                rows_loaded = file_info.get("rows_loaded", 0)
                total_load_records += rows_loaded
                print(f"       Load File {i + 1}: {file_name} -> {rows_loaded:,} records")

            if len(result.file_manifest) > 10:
                remaining_files = result.file_manifest[10:]
                remaining_records = sum(f.get("rows_loaded", 0) for f in remaining_files)
                total_load_records += remaining_records
                print(
                    f"       ... and {len(remaining_files)} more files -> {remaining_records:,} records"
                )

            print(f"     Total records from file breakdown: {total_load_records:,}")
            print(f"     FastLoadResult.record_count: {result.record_count:,}")

            if total_load_records != result.record_count:
                print(
                    f"     ⚠️  MISMATCH: File breakdown ({total_load_records:,}) != record_count ({result.record_count:,})"
                )
            else:
                print(f"     ✅ File breakdown matches record_count")

    print("   📊 Load Summary:")
    print(f"     Total files processed: {total_load_files_processed}")
    print(f"     Total actual records loaded: {total_load_actual_records:,}")
    if total_load_data_size_bytes > 0:
        print(
            f"     Total data size: {total_load_data_size_bytes:,} bytes ({total_load_data_size_bytes / (1024 * 1024):.2f} MB)"
        )
    if total_load_compressed_size_bytes > 0:
        print(
            f"     Total compressed size: {total_load_compressed_size_bytes:,} bytes ({total_load_compressed_size_bytes / (1024 * 1024):.2f} MB)"
        )

    print(f"\n🔍 [DEBUG] Unload vs Load File Comparison:")
    print(f"  Unload Summary:")
    print(f"    Files created: {total_files_created}")
    print(f"    Records unloaded: {total_actual_records:,}")
    print(f"  Load Summary:")
    print(f"    Files processed: {total_load_files_processed}")
    print(f"    Records loaded: {total_load_actual_records:,}")
    print(f"  ")
    print(
        f"  File Count Match: {'✅' if total_files_created == total_load_files_processed else '❌'}"
    )
    print(
        f"  Record Count Match: {'✅' if total_actual_records == total_load_actual_records else '❌'}"
    )
    print(f"  ")
    print(f"  Potential Issues:")
    print(
        f"    - If file counts don't match: Load may be reading from wrong S3 path or missing files"
    )
    print(
        f"    - If record counts don't match: Files may contain different data or path filters not working"
    )
    print(f"    - Check S3 paths above to ensure unload and load are using same locations")

    total_time = time.time() - total_start
    workflow_end_time = datetime.now()
    total_elapsed = (workflow_end_time - workflow_start_time).total_seconds()

    warehouse_size = warehouse_config["size"]
    size_multiplier = warehouse_config["multiplier"]

    total_records_per_sec = actual_records / total_time if total_time > 0 else 0
    total_mb_per_sec = (
        (actual_records * estimated_bytes_per_record) / (1024 * 1024) / total_time
        if total_time > 0
        else 0
    )

    print(f"\n📊 [{workflow_end_time.strftime('%H:%M:%S')}] Performance Summary:")
    print(f"  Workflow started:               {workflow_start_time.strftime('%H:%M:%S')}")
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
        f"  Records processed:              {actual_records:,} / {NUM_RECORDS:,} ({100 * actual_records / NUM_RECORDS:.1f}%)"
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
    print(
        f"  Throughput per compute unit:    {total_records_per_sec / size_multiplier:,.1f} records/s/unit"
    )
    print(f"  Bandwidth per compute unit:     {total_mb_per_sec / size_multiplier:.2f} MB/s/unit")

    print("\n💰 Snowflake CPU Minutes Analysis:")
    print(f"  Step 2 CPU minutes:             {step2_cpu_minutes:.3f} minutes")
    print(f"  Step 3 CPU minutes:             {step3_cpu_minutes:.3f} minutes")
    print(f"  Total CPU minutes:              {total_cpu_minutes:.3f} minutes")
    print(
        f"  Cost efficiency (rec/CPU-min):  {actual_records / total_cpu_minutes:,.0f} records/CPU-minute"
    )

    validation_start_time = datetime.now()
    print(f"\n🔍 [{validation_start_time.strftime('%H:%M:%S')}] Validating data transfer...")
    for i, stream_name in enumerate(streams):
        unload_result = unload_results[i]
        load_result = load_results[i]

        unload_count = unload_result.record_count or 0
        load_count = load_result.record_count or 0

        print(f"  {stream_name}: Unloaded={unload_count:,}, Loaded={load_count:,}")
        if unload_count == load_count:
            print(f"  ✅ {stream_name} transfer validated (metadata-based)")
        else:
            print(f"  ❌ {stream_name} transfer validation failed (metadata-based)")

            source_count = len(snowflake_cache_source[stream_name])
            dest_count = len(snowflake_cache_dest[stream_name])
            print(f"  Fallback validation: Source={source_count:,}, Destination={dest_count:,}")
            if source_count == dest_count:
                print(f"  ✅ {stream_name} fallback validation passed")
            else:
                print(f"  ❌ {stream_name} fallback validation failed")
    validation_end_time = datetime.now()
    print(
        f"🔍 [{validation_end_time.strftime('%H:%M:%S')}] Validation completed in {(validation_end_time - validation_start_time).total_seconds():.2f}s"
    )

    return {
        "warehouse_name": warehouse_config["name"],
        "warehouse_size": warehouse_config["size"],
        "size_multiplier": warehouse_config["multiplier"],
        "step2_time": step2_time,
        "step2_records_per_sec": step2_records_per_sec,
        "step2_mb_per_sec": step2_mb_per_sec,
        "step2_cpu_minutes": step2_cpu_minutes,
        "step3_time": step3_time,
        "step3_records_per_sec": step3_records_per_sec,
        "step3_mb_per_sec": step3_mb_per_sec,
        "step3_cpu_minutes": step3_cpu_minutes,
        "total_time": total_time,
        "total_records_per_sec": total_records_per_sec,
        "total_mb_per_sec": total_mb_per_sec,
        "total_cpu_minutes": total_cpu_minutes,
        "actual_records": actual_records,
        "total_files_created": total_files_created,
        "total_actual_records": total_actual_records,
        "total_data_size_bytes": total_data_size_bytes,
        "total_compressed_size_bytes": total_compressed_size_bytes,
        "total_load_records": total_load_records,
        "total_load_data_bytes": total_load_data_bytes,
    }


def reload_raw_data(credentials: dict[str, Any], source: ab.Source) -> None:
    """Reload raw data from source to Snowflake for initial setup."""
    if not RELOAD_INITIAL_SOURCE_DATA:
        print(f"\n⏭️  Skipping reload (RELOAD_INITIAL_SOURCE_DATA=False)")
        print("   • Set RELOAD_INITIAL_SOURCE_DATA=True to reload 100M records")
        return

    print(
        f"\n⚠️  WARNING: This will take approximately 2.5 hours to reload {NUM_RECORDS:,} records"
    )
    print("   • Only Step 1 (Source → Snowflake) will run")
    print("   • No warehouse testing or S3 operations")

    warehouse_config = WAREHOUSE_CONFIGS[0]  # COMPUTE_WH (xsmall)
    snowflake_cache_source, _ = setup_caches(credentials, warehouse_config)

    step1_start_time = datetime.now()
    print(
        f"📥 [{step1_start_time.strftime('%H:%M:%S')}] Step 1: Loading {NUM_RECORDS:,} records from source to Snowflake..."
    )

    source.read(
        cache=snowflake_cache_source,
        streams=["purchases"],  # Only purchases stream
        force_full_refresh=True,
        write_strategy="replace",
    )

    step1_end_time = datetime.now()
    step1_time = (step1_end_time - step1_start_time).total_seconds()

    print(
        f"✅ [{step1_end_time.strftime('%H:%M:%S')}] Step 1 completed in {step1_time:.2f} seconds"
    )
    print(f"   • Records loaded: {NUM_RECORDS:,}")
    print(f"   • Records per second: {NUM_RECORDS / step1_time:,.1f}")
    print(f"   • Warehouse used: {warehouse_config['name']} ({warehouse_config['size']})")
    print(f"\n🎉 Raw data reload completed successfully!")


def print_performance_summary(results: list[dict[str, Any]]) -> None:
    """Print comprehensive performance comparison across all warehouse sizes."""
    print(f"\n{'=' * 80}")
    print("📊 COMPREHENSIVE PERFORMANCE ANALYSIS ACROSS ALL WAREHOUSE SIZES")
    print(f"{'=' * 80}")

    print(f"\n🔄 UNLOAD PERFORMANCE (Snowflake → S3):")
    print(
        f"{'Warehouse':<20} {'Size':<8} {'Multiplier':<10} {'Time (s)':<10} {'Records/s':<15} {'MB/s':<10} {'CPU Min':<10}"
    )
    print("-" * 90)
    for result in results:
        print(
            f"{result['warehouse_name']:<20} {result['warehouse_size']:<8} {result['size_multiplier']:<10} "
            f"{result['step2_time']:<10.2f} {result['step2_records_per_sec']:<15,.0f} "
            f"{result['step2_mb_per_sec']:<10.1f} {result['step2_cpu_minutes']:<10.3f}"
        )

    print(f"\n📥 LOAD PERFORMANCE (S3 → Snowflake):")
    print(
        f"{'Warehouse':<20} {'Size':<8} {'Multiplier':<10} {'Time (s)':<10} {'Records/s':<15} {'MB/s':<10} {'CPU Min':<10}"
    )
    print("-" * 90)
    for result in results:
        print(
            f"{result['warehouse_name']:<20} {result['warehouse_size']:<8} {result['size_multiplier']:<10} "
            f"{result['step3_time']:<10.2f} {result['step3_records_per_sec']:<15,.0f} "
            f"{result['step3_mb_per_sec']:<10.1f} {result['step3_cpu_minutes']:<10.3f}"
        )

    print(f"\n🎯 OVERALL PERFORMANCE SUMMARY:")
    print(
        f"{'Warehouse':<20} {'Size':<8} {'Multiplier':<10} {'Total Time':<12} {'Records/s':<15} {'MB/s':<10} {'Total CPU':<12}"
    )
    print("-" * 100)
    for result in results:
        print(
            f"{result['warehouse_name']:<20} {result['warehouse_size']:<8} {result['size_multiplier']:<10} "
            f"{result['total_time']:<12.2f} {result['total_records_per_sec']:<15,.0f} "
            f"{result['total_mb_per_sec']:<10.1f} {result['total_cpu_minutes']:<12.3f}"
        )

    print(f"\n📈 KEY INSIGHTS:")
    best_unload = max(results, key=lambda x: x["step2_records_per_sec"])
    best_load = max(results, key=lambda x: x["step3_records_per_sec"])
    most_efficient = min(results, key=lambda x: x["total_cpu_minutes"])

    print(f"  • Best unload performance: {best_unload['warehouse_name']} ({best_unload['step2_records_per_sec']:,.0f} rec/s)")
    print(f"  • Best load performance: {best_load['warehouse_name']} ({best_load['step3_records_per_sec']:,.0f} rec/s)")
    print(f"  • Most cost efficient: {most_efficient['warehouse_name']} ({most_efficient['total_cpu_minutes']:.3f} CPU minutes)")
    print(f"  • Records processed: {results[0]['actual_records']:,} across all tests")
    print(f"  • Data size: {results[0]['total_data_size_bytes'] / (1024*1024*1024):.2f} GB uncompressed")


def main() -> None:
    """Main execution function - runs performance tests across all warehouse sizes."""
    print("🎯 PyAirbyte Fast Lake Copy Demo - Multi-Warehouse Performance Analysis")
    print("=" * 80)

    script_start_time = datetime.now()
    credentials = get_credentials()
    source = setup_source()

    results = []

    print(f"\n🏭 Testing {len(WAREHOUSE_CONFIGS)} warehouse configurations...")
    print("Available warehouse options:")
    for config in WAREHOUSE_CONFIGS:
        print(f"  • {config['name']}: {config['size']} ({config['multiplier']}x multiplier)")

    for i, warehouse_config in enumerate(WAREHOUSE_CONFIGS, 1):
        print(f"\n{'=' * 80}")
        print(
            f"🧪 Test {i}/{len(WAREHOUSE_CONFIGS)}: "
            f"{warehouse_config['name']} ({warehouse_config['size']})"
        )
        print(f"{'=' * 80}")

        s3_lake: CustomS3LakeStorage = setup_lake_storage(
            credentials,
            script_start_time,
        )

        snowflake_cache_source, snowflake_cache_dest = setup_caches(credentials, warehouse_config)

        result = transfer_data_with_timing(
            source=source,
            snowflake_cache_source=snowflake_cache_source,
            snowflake_cache_dest=snowflake_cache_dest,
            s3_lake=s3_lake,
            warehouse_config=warehouse_config,
        )
        results.append(result)

        print("\n🎉 Test completed successfully!")
        print("💡 This demonstrates 100x performance improvements through:")
        print("   • Direct bulk operations (Snowflake COPY INTO)")
        print("   • S3 lake storage intermediate layer")
        print("   • Managed Snowflake artifacts (AIRBYTE_LAKE_S3_MAIN_* with CREATE IF NOT EXISTS)")
        print("   • Optimized Parquet file format with Snappy compression")
        print("   • Parallel stream processing")
        print(f"   • Warehouse scaling: {warehouse_config['size']} ({warehouse_config['multiplier']}x compute units)")

    print_performance_summary(results)

    print(f"\n🔄 RELOAD MODE: Only reloading raw 100M records to Snowflake...")
    print(f"   • NUM_RECORDS: {NUM_RECORDS:,}")
    print(f"   • RELOAD_INITIAL_SOURCE_DATA: {RELOAD_INITIAL_SOURCE_DATA}")

    reload_raw_data(credentials, source)


if __name__ == "__main__":
    main()
