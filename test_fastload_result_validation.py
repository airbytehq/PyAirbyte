#!/usr/bin/env python3
"""Simple validation test for FastLoadResult implementation."""

import os
import json
import time
from sqlalchemy import create_engine, text
from airbyte.secrets.google_gsm import GoogleGSMSecretManager
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.lakes import S3LakeStorage, FastLoadResult


def test_fastload_result_implementation():
    """Test FastLoadResult implementation with actual Snowflake operations."""
    print("🔍 Testing FastLoadResult implementation...")
    
    gsm = GoogleGSMSecretManager(
        project="dataline-integration-testing",
        credentials_json=os.environ.get("DEVIN_GCP_SERVICE_ACCOUNT_JSON"),
    )
    snowflake_creds = json.loads(gsm.get_secret("AIRBYTE_LIB_SNOWFLAKE_CREDS"))
    
    cache = SnowflakeCache(
        account=snowflake_creds["account"],
        username=snowflake_creds["username"],
        password=snowflake_creds["password"],
        warehouse="COMPUTE_WH",
        database=snowflake_creds["database"],
        role=snowflake_creds["role"],
        schema_name="FAST_LAKE_COPY_SOURCE",
    )
    
    s3_lake = S3LakeStorage(
        bucket_name="ab-destiantion-iceberg-us-west-2",
        region="us-west-2",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        short_name="test_validation",
    )
    
    test_table = "TEST_FASTLOAD_VALIDATION"
    test_records = 1000
    
    qualified_test_table = f"{cache.database}.{cache.schema_name}.{test_table}"
    dest_table = f"{test_table}_DEST"
    qualified_dest_table = f"{cache.database}.{cache.schema_name}.{dest_table}"
    
    try:
        print(f"📊 Creating test table with {test_records:,} records...")
        
        cache.execute_sql(f"DROP TABLE IF EXISTS {qualified_test_table}")
        
        create_sql = f"""
            CREATE TABLE {qualified_test_table} (
                id INTEGER,
                name STRING,
                amount DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """
        cache.execute_sql(create_sql)
        
        insert_sql = f"""
            INSERT INTO {qualified_test_table} (id, name, amount)
            SELECT 
                seq4() as id,
                'test_record_' || seq4() as name,
                (seq4() * 12.34) as amount
            FROM TABLE(GENERATOR(ROWCOUNT => {test_records}))
        """
        cache.execute_sql(insert_sql)
        
        print("📤 Unloading test data to S3...")
        unload_result = cache.fast_unload_table(
            table_name=test_table,
            lake_store=s3_lake,
            lake_path_prefix="test_fastload_validation",
        )
        
        print(f"✅ Unload completed: {unload_result.actual_record_count:,} records")
        
        cache.execute_sql(f"DROP TABLE IF EXISTS {qualified_dest_table}")
        
        create_dest_sql = f"""
            CREATE TABLE {qualified_dest_table} (
                id INTEGER,
                name STRING,
                amount DECIMAL(10,2),
                created_at TIMESTAMP
            )
        """
        cache.execute_sql(create_dest_sql)
        
        print("📥 Loading data from S3 using FastLoadResult...")
        load_result = cache.fast_load_table(
            table_name=dest_table,
            lake_store=s3_lake,
            lake_path_prefix="test_fastload_validation",
        )
        
        print(f"\n📊 FastLoadResult Validation:")
        print(f"  Type: {type(load_result).__name__}")
        print(f"  Table name: {load_result.table_name}")
        print(f"  Lake path prefix: {load_result.lake_path_prefix}")
        print(f"  Actual record count: {load_result.actual_record_count}")
        print(f"  Files processed: {load_result.files_processed}")
        print(f"  Total data size: {load_result.total_data_size_bytes}")
        print(f"  Compressed size: {load_result.compressed_size_bytes}")
        print(f"  File manifest entries: {len(load_result.file_manifest) if load_result.file_manifest else 0}")
        
        if load_result.file_manifest:
            print(f"  Sample manifest entry: {load_result.file_manifest[0]}")
        
        actual_table_count = cache.execute_sql(f"SELECT COUNT(*) FROM {qualified_dest_table}").fetchone()[0]
        
        print(f"\n🔍 Validation Results:")
        print(f"  Expected records: {test_records:,}")
        print(f"  Unloaded records: {unload_result.actual_record_count:,}")
        print(f"  FastLoadResult count: {load_result.actual_record_count:,}")
        print(f"  Actual table count: {actual_table_count:,}")
        
        if (test_records == unload_result.actual_record_count == 
            load_result.actual_record_count == actual_table_count):
            print("✅ All counts match - FastLoadResult implementation is working correctly!")
            return True
        else:
            print("❌ Count mismatch detected - FastLoadResult needs investigation")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        try:
            cache.execute_sql(f"DROP TABLE IF EXISTS {qualified_test_table}")
            cache.execute_sql(f"DROP TABLE IF EXISTS {qualified_dest_table}")
        except:
            pass


if __name__ == "__main__":
    print("🎯 Starting FastLoadResult validation test...")
    success = test_fastload_result_implementation()
    
    if success:
        print("\n🎉 FastLoadResult validation test PASSED!")
        print("✅ FastLoadResult class is capturing accurate metadata from Snowflake COPY INTO operations")
    else:
        print("\n💥 FastLoadResult validation test FAILED!")
        print("❌ FastLoadResult implementation needs debugging")
