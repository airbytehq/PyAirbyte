#!/usr/bin/env python3
"""Test script to observe Snowflake COPY INTO load RESULT_SCAN() metadata schema."""

import os
import json
from sqlalchemy import create_engine, text
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


def test_load_metadata_schema():
    """Test COPY INTO load with RESULT_SCAN() to observe metadata schema."""
    print("🔍 Testing Snowflake COPY INTO load RESULT_SCAN() metadata schema...")
    
    gsm = GoogleGSMSecretManager(
        project="dataline-integration-testing",
        credentials_json=os.environ.get("DEVIN_GCP_SERVICE_ACCOUNT_JSON"),
    )
    snowflake_creds = json.loads(gsm.get_secret("AIRBYTE_LIB_SNOWFLAKE_CREDS"))
    
    connection_url = (
        f"snowflake://{snowflake_creds['username']}:{snowflake_creds['password']}"
        f"@{snowflake_creds['account']}/{snowflake_creds['database']}/FAST_LAKE_COPY_SOURCE"
        f"?warehouse=COMPUTE_WH&role={snowflake_creds['role']}"
    )
    
    engine = create_engine(connection_url)
    
    with engine.connect() as connection:
        print("✅ Connection established")
        
        test_table = "TEST_LOAD_METADATA"
        
        try:
            connection.execute(text(f"DROP TABLE IF EXISTS {test_table}"))
            
            create_sql = f"""
                CREATE TEMPORARY TABLE {test_table} (
                    id INTEGER,
                    name STRING,
                    amount DECIMAL(10,2)
                )
            """
            connection.execute(text(create_sql))
            
            test_record_count = 1000
            insert_sql = f"""
                INSERT INTO {test_table} (id, name, amount)
                SELECT 
                    seq4() as id,
                    'record_' || seq4() as name,
                    (seq4() * 2.50) as amount
                FROM TABLE(GENERATOR(ROWCOUNT => {test_record_count}))
            """
            connection.execute(text(insert_sql))
            print(f"📊 Created test data: {test_record_count:,} records")
            
            internal_stage = f"@%{test_table}"
            
            unload_sql = f"""
                COPY INTO {internal_stage}/backup/
                FROM {test_table}
                FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY)
                OVERWRITE = TRUE
            """
            connection.execute(text(unload_sql))
            print("📤 Unloaded data to internal stage")
            
            connection.execute(text(f"DELETE FROM {test_table}"))
            print("🗑️  Cleared table for load test")
            
            print("🚀 Executing COPY INTO load...")
            load_sql = f"""
                COPY INTO {test_table}
                FROM {internal_stage}/backup/
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
                print("\n📄 COPY INTO Load Metadata Schema:")
                for i, row in enumerate(rows):
                    row_dict = dict(row._mapping) if hasattr(row, '_mapping') else dict(row)
                    print(f"\n  File {i+1} metadata:")
                    for key, value in row_dict.items():
                        print(f"    {key}: {value} ({type(value).__name__})")
                
                first_row = dict(rows[0]._mapping) if hasattr(rows[0], '_mapping') else dict(rows[0])
                available_fields = list(first_row.keys())
                
                print(f"\n🎯 Available Fields for FastLoadResult Implementation:")
                for field in available_fields:
                    print(f"  - {field}")
                
                field_mapping = {
                    'ROWS_LOADED': 'actual_record_count',
                    'ROWS_PARSED': 'total_rows_parsed', 
                    'FILE': 'file_name',
                    'STATUS': 'file_status',
                    'ERROR_LIMIT': 'error_limit',
                    'ERRORS_SEEN': 'errors_seen',
                    'FIRST_ERROR': 'first_error',
                    'FIRST_ERROR_LINE': 'first_error_line',
                    'FIRST_ERROR_CHARACTER': 'first_error_character',
                    'FIRST_ERROR_COLUMN_NAME': 'first_error_column_name'
                }
                
                print(f"\n🔧 FastLoadResult Field Mapping:")
                for snowflake_field, fastload_field in field_mapping.items():
                    if snowflake_field in available_fields:
                        print(f"  {snowflake_field} -> {fastload_field}")
                
                total_rows_loaded = sum(dict(row._mapping).get('ROWS_LOADED', 0) if hasattr(row, '_mapping') else dict(row).get('ROWS_LOADED', 0) for row in rows)
                files_processed = len(rows)
                
                print(f"\n📊 Load Metadata Summary:")
                print(f"  total_rows_loaded: {total_rows_loaded}")
                print(f"  files_processed: {files_processed}")
                
                actual_count_result = connection.execute(text(f"SELECT COUNT(*) FROM {test_table}"))
                actual_count = actual_count_result.fetchone()[0]
                print(f"  actual_table_count: {actual_count}")
                
                if total_rows_loaded == actual_count == test_record_count:
                    print("✅ All counts match - RESULT_SCAN() load metadata is accurate!")
                    return True, {
                        'schema': available_fields,
                        'field_mapping': field_mapping,
                        'sample_row': first_row,
                        'total_rows_loaded': total_rows_loaded,
                        'files_processed': files_processed,
                    }
                else:
                    print(f"❌ Count mismatch: loaded={total_rows_loaded}, actual={actual_count}, expected={test_record_count}")
                    return False, None
            else:
                print("❌ No metadata rows returned from RESULT_SCAN()")
                return False, None
                
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False, None
        
        finally:
            try:
                connection.execute(text(f"DROP TABLE IF EXISTS {test_table}"))
            except:
                pass


if __name__ == "__main__":
    print("🎯 Starting COPY INTO load metadata schema test...")
    success, metadata = test_load_metadata_schema()
    
    if success:
        print("\n🎉 COPY INTO load metadata schema test PASSED!")
        print("✅ FastLoadResult schema identified and field mapping created")
        if metadata:
            print(f"✅ Available fields: {metadata['schema']}")
            print(f"✅ Field mapping: {metadata['field_mapping']}")
    else:
        print("\n💥 COPY INTO load metadata schema test FAILED!")
