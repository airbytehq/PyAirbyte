#!/usr/bin/env python3
"""Simple test script to observe Snowflake COPY INTO load RESULT_SCAN() schema."""

import os
import json
from sqlalchemy import create_engine, text
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


def test_simple_load_result_scan():
    """Test COPY INTO load with RESULT_SCAN() using internal table stages."""
    print("🔍 Testing Snowflake COPY INTO load RESULT_SCAN() schema...")
    
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
        
        source_table = "TEST_LOAD_SOURCE"
        dest_table = "TEST_LOAD_DEST"
        
        try:
            connection.execute(text(f"DROP TABLE IF EXISTS {source_table}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {dest_table}"))
            
            create_source_sql = f"""
                CREATE TEMPORARY TABLE {source_table} (
                    id INTEGER,
                    name STRING,
                    amount DECIMAL(10,2)
                )
            """
            connection.execute(text(create_source_sql))
            
            test_record_count = 5000
            insert_sql = f"""
                INSERT INTO {source_table} (id, name, amount)
                SELECT 
                    seq4() as id,
                    'record_' || seq4() as name,
                    (seq4() * 5.25) as amount
                FROM TABLE(GENERATOR(ROWCOUNT => {test_record_count}))
            """
            connection.execute(text(insert_sql))
            print(f"📊 Created source data: {test_record_count:,} records")
            
            create_dest_sql = f"""
                CREATE TEMPORARY TABLE {dest_table} (
                    id INTEGER,
                    name STRING,
                    amount DECIMAL(10,2)
                )
            """
            connection.execute(text(create_dest_sql))
            
            internal_stage = f"@%{source_table}"
            
            unload_sql = f"""
                COPY INTO {internal_stage}/data/
                FROM {source_table}
                FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY)
                OVERWRITE = TRUE
            """
            connection.execute(text(unload_sql))
            print("📤 Unloaded data to internal stage")
            
            print("🚀 Executing COPY INTO load...")
            load_sql = f"""
                COPY INTO {dest_table}
                FROM {internal_stage}/data/
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
                
                print(f"\n🎯 Available Fields for FastLoadResult:")
                for field in available_fields:
                    print(f"  - {field}")
                
                total_rows_loaded = sum(dict(row._mapping).get('ROWS_LOADED', 0) if hasattr(row, '_mapping') else dict(row).get('ROWS_LOADED', 0) for row in rows)
                total_rows_parsed = sum(dict(row._mapping).get('ROWS_PARSED', 0) if hasattr(row, '_mapping') else dict(row).get('ROWS_PARSED', 0) for row in rows)
                files_processed = len(rows)
                
                print(f"\n📊 Load Metadata Summary:")
                print(f"  total_rows_loaded: {total_rows_loaded}")
                print(f"  total_rows_parsed: {total_rows_parsed}")
                print(f"  files_processed: {files_processed}")
                
                actual_count_result = connection.execute(text(f"SELECT COUNT(*) FROM {dest_table}"))
                actual_count = actual_count_result.fetchone()[0]
                print(f"  actual_table_count: {actual_count}")
                
                if total_rows_loaded == actual_count == test_record_count:
                    print("✅ All counts match - RESULT_SCAN() load metadata is accurate!")
                    return True, {
                        'schema': available_fields,
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
                connection.execute(text(f"DROP TABLE IF EXISTS {source_table}"))
                connection.execute(text(f"DROP TABLE IF EXISTS {dest_table}"))
            except:
                pass


if __name__ == "__main__":
    print("🎯 Starting simple COPY INTO load RESULT_SCAN() test...")
    success, metadata = test_simple_load_result_scan()
    
    if success:
        print("\n🎉 COPY INTO load RESULT_SCAN() test PASSED!")
        print("✅ FastLoadResult schema identified")
        if metadata:
            print(f"✅ Available fields: {metadata['schema']}")
            print(f"✅ Sample metadata: {metadata['sample_row']}")
    else:
        print("\n💥 COPY INTO load RESULT_SCAN() test FAILED!")
