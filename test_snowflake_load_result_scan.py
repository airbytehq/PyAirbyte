#!/usr/bin/env python3
"""Test script to validate Snowflake COPY INTO load operations and RESULT_SCAN() schema."""

import os
import json
from sqlalchemy import create_engine, text
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


def test_copy_into_load_result_scan():
    """Test COPY INTO load with RESULT_SCAN() to validate metadata capture approach."""
    print("🔍 Testing Snowflake COPY INTO load and RESULT_SCAN() metadata capture...")
    
    gsm = GoogleGSMSecretManager(
        project="dataline-integration-testing",
        credentials_json=os.environ.get("DEVIN_GCP_SERVICE_ACCOUNT_JSON"),
    )
    snowflake_creds = json.loads(gsm.get_secret("AIRBYTE_LIB_SNOWFLAKE_CREDS"))
    
    connection_url = (
        f"snowflake://{snowflake_creds['username']}:{snowflake_creds['password']}"
        f"@{snowflake_creds['account']}/{snowflake_creds['database']}/airbyte_raw"
        f"?warehouse=COMPUTE_WH&role={snowflake_creds['role']}"
    )
    
    engine = create_engine(connection_url)
    
    with engine.connect() as connection:
        print("✅ Connection established")
        
        connection.execute(text(f"USE DATABASE {snowflake_creds['database']}"))
        
        schemas_result = connection.execute(text("SHOW SCHEMAS"))
        schemas = schemas_result.fetchall()
        print("📋 Available schemas:")
        for schema in schemas[:10]:  # Show first 10 schemas
            print(f"  - {schema[1]}")
        
        target_schema = None
        schema_names = [schema[1] for schema in schemas]
        
        for preferred_schema in ["FAST_LAKE_COPY_SOURCE", "FAST_LAKE_COPY_DEST", "PUBLIC"]:
            if preferred_schema in schema_names:
                target_schema = preferred_schema
                break
        
        if target_schema:
            connection.execute(text(f"USE SCHEMA {target_schema}"))
            print(f"📋 Using schema: {target_schema}")
        else:
            print("❌ No suitable schema found")
            return False, None
        
        test_table = "TEST_LOAD_RESULT_SCAN"
        test_stage = "TEST_LOAD_STAGE"
        
        try:
            connection.execute(text(f"DROP TABLE IF EXISTS {test_table}"))
            connection.execute(text(f"DROP STAGE IF EXISTS {test_stage}"))
            
            create_sql = f"""
                CREATE TEMPORARY TABLE {test_table} (
                    id INTEGER,
                    name STRING,
                    amount DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                )
            """
            connection.execute(text(create_sql))
            print(f"📋 Created test table: {test_table}")
            
            stage_sql = f"""
                CREATE TEMPORARY STAGE {test_stage}
                URL = 's3://ab-destiantion-iceberg-us-west-2/test_load_result_scan/'
                CREDENTIALS = (
                    AWS_KEY_ID = '{os.environ.get("AWS_ACCESS_KEY_ID")}'
                    AWS_SECRET_KEY = '{os.environ.get("AWS_SECRET_ACCESS_KEY")}'
                )
                FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY)
            """
            connection.execute(text(stage_sql))
            print(f"📋 Created test stage: {test_stage}")
            
            source_table = "TEST_SOURCE_DATA"
            connection.execute(text(f"DROP TABLE IF EXISTS {source_table}"))
            
            create_source_sql = f"""
                CREATE TEMPORARY TABLE {source_table} (
                    id INTEGER,
                    name STRING,
                    amount DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                )
            """
            connection.execute(text(create_source_sql))
            
            test_record_count = 10000
            insert_sql = f"""
                INSERT INTO {source_table} (id, name, amount)
                SELECT 
                    seq4() as id,
                    'test_record_' || seq4() as name,
                    (seq4() * 10.50) as amount
                FROM TABLE(GENERATOR(ROWCOUNT => {test_record_count}))
            """
            connection.execute(text(insert_sql))
            print(f"📊 Created source data: {test_record_count:,} records")
            
            unload_sql = f"""
                COPY INTO @{test_stage}/test_data/
                FROM {source_table}
                FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY)
                OVERWRITE = TRUE
            """
            connection.execute(text(unload_sql))
            print("📤 Unloaded data to stage")
            
            print("🚀 Executing COPY INTO load...")
            load_sql = f"""
                COPY INTO {test_table}
                FROM @{test_stage}/test_data/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = FALSE
            """
            
            load_result = connection.execute(text(load_sql))
            print("✅ COPY INTO load completed")
            
            print("🔍 Querying RESULT_SCAN() for load metadata...")
            result_scan_sql = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
            result_scan_result = connection.execute(text(result_scan_sql))
            
            columns = list(result_scan_result.keys())
            print(f"📋 RESULT_SCAN columns: {columns}")
            
            rows = result_scan_result.fetchall()
            print(f"📊 Found {len(rows)} result rows")
            
            if rows:
                print("\n📄 COPY INTO Load Metadata:")
                for i, row in enumerate(rows):
                    row_dict = dict(row._mapping) if hasattr(row, '_mapping') else dict(row)
                    print(f"\n  File {i+1}:")
                    for key, value in row_dict.items():
                        print(f"    {key}: {value} ({type(value).__name__})")
                
                total_rows_loaded = sum(row_dict.get('ROWS_LOADED', 0) for row in rows)
                total_rows_parsed = sum(row_dict.get('ROWS_PARSED', 0) for row in rows)
                files_processed = len(rows)
                
                print(f"\n🎯 Key Load Metadata Summary:")
                print(f"  total_rows_loaded: {total_rows_loaded}")
                print(f"  total_rows_parsed: {total_rows_parsed}")
                print(f"  files_processed: {files_processed}")
                
                actual_count_result = connection.execute(text(f"SELECT COUNT(*) FROM {test_table}"))
                actual_count = actual_count_result.fetchone()[0]
                print(f"  actual_table_count: {actual_count}")
                
                validation_passed = True
                
                if total_rows_loaded != actual_count:
                    print(f"❌ VALIDATION FAILED: total_rows_loaded ({total_rows_loaded}) != actual_count ({actual_count})")
                    validation_passed = False
                else:
                    print(f"✅ VALIDATION PASSED: total_rows_loaded matches actual_count ({actual_count})")
                
                if total_rows_loaded != test_record_count:
                    print(f"❌ VALIDATION FAILED: total_rows_loaded ({total_rows_loaded}) != expected_count ({test_record_count})")
                    validation_passed = False
                else:
                    print(f"✅ VALIDATION PASSED: total_rows_loaded matches expected_count ({test_record_count})")
                
                return validation_passed, {
                    'actual_record_count': total_rows_loaded,
                    'files_processed': files_processed,
                    'total_rows_parsed': total_rows_parsed,
                    'file_manifest': [dict(row._mapping) if hasattr(row, '_mapping') else dict(row) for row in rows],
                }
            else:
                print("❌ VALIDATION FAILED: No metadata rows returned from RESULT_SCAN()")
                return False, None
                
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False, None
        
        finally:
            try:
                connection.execute(text(f"DROP TABLE IF EXISTS {test_table}"))
                connection.execute(text(f"DROP TABLE IF EXISTS {source_table}"))
            except:
                pass


if __name__ == "__main__":
    print("🎯 Starting COPY INTO load RESULT_SCAN() validation test...")
    success, metadata = test_copy_into_load_result_scan()
    
    if success:
        print("\n🎉 COPY INTO load RESULT_SCAN() validation test PASSED!")
        print("✅ Connection context manager approach confirmed working for loads")
        print("✅ COPY INTO load metadata capture validated")
        print("✅ FastLoadResult implementation approach validated")
        if metadata:
            print(f"✅ Sample load metadata: {metadata}")
    else:
        print("\n💥 COPY INTO load RESULT_SCAN() validation test FAILED!")
        print("❌ Connection context manager approach needs investigation for loads")
