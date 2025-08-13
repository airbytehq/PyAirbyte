#!/usr/bin/env python3
"""Helper script for manually testing source connectors with small record counts.

This script demonstrates how to invoke source connectors directly to examine their
message output, which is useful for debugging connector behavior and message formats.

Usage:
    python examples/manual_connector_test.py

The script will create temporary config and catalog files, invoke the source connector,
and save the output to a file for examination.
"""

import json
import subprocess
import tempfile
from pathlib import Path


def create_config_file(config_type: str = "INFINITE_FEED", max_records: int = 5) -> Path:
    """Create a temporary config file for the source connector."""
    if config_type == "INFINITE_FEED":
        config = {
            "type": "INFINITE_FEED",
            "max_records": max_records,
            "seed": 0,
            "message_interval": 1000
        }
    elif config_type == "BENCHMARK":
        config = {
            "type": "BENCHMARK",
            "schema": "FIVE_STRING_COLUMNS",
            "terminationCondition": {
                "type": "MAX_RECORDS",
                "max": max_records
            }
        }
    else:
        raise ValueError(f"Unknown config type: {config_type}")
    
    config_file = Path(tempfile.mktemp(suffix=".json"))
    config_file.write_text(json.dumps(config, indent=2))
    return config_file


def create_catalog_file() -> Path:
    """Create a temporary catalog file for the source connector."""
    catalog = {
        "streams": [
            {
                "stream": {
                    "name": "data",
                    "json_schema": {},
                    "supported_sync_modes": ["full_refresh"]
                },
                "sync_mode": "full_refresh",
                "destination_sync_mode": "overwrite"
            }
        ]
    }
    
    catalog_file = Path(tempfile.mktemp(suffix=".json"))
    catalog_file.write_text(json.dumps(catalog, indent=2))
    return catalog_file


def run_source_connector(
    source_image: str = "airbyte/source-e2e-test:dev",
    config_type: str = "INFINITE_FEED",
    max_records: int = 5,
    output_file: str = "/tmp/manual_source_output.jsonl"
) -> None:
    """Run the source connector and capture its output."""
    
    config_file = create_config_file(config_type, max_records)
    catalog_file = create_catalog_file()
    
    try:
        cmd = [
            "docker", "run", "--rm",
            "-v", f"{config_file}:/tmp/config.json",
            "-v", f"{catalog_file}:/tmp/catalog.json",
            source_image,
            "read",
            "--config", "/tmp/config.json",
            "--catalog", "/tmp/catalog.json"
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        print(f"Config type: {config_type}")
        print(f"Max records: {max_records}")
        print(f"Output file: {output_file}")
        
        with open(output_file, "w") as f:
            result = subprocess.run(
                cmd,
                stdout=f,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60
            )
        
        if result.returncode == 0:
            print(f"✅ Source connector completed successfully")
            print(f"📄 Output saved to: {output_file}")
            
            output_path = Path(output_file)
            if output_path.exists():
                lines = output_path.read_text().strip().split('\n')
                print(f"📊 Generated {len(lines)} lines of output")
                print("🔍 First 3 lines:")
                for i, line in enumerate(lines[:3]):
                    print(f"  {i+1}: {line}")
        else:
            print(f"❌ Source connector failed with exit code: {result.returncode}")
            print(f"stderr: {result.stderr}")
    
    finally:
        config_file.unlink(missing_ok=True)
        catalog_file.unlink(missing_ok=True)


if __name__ == "__main__":
    print("🧪 Manual Source Connector Test")
    print("=" * 40)
    
    print("\n📋 Testing with INFINITE_FEED configuration...")
    run_source_connector(
        config_type="INFINITE_FEED",
        max_records=5,
        output_file="/tmp/manual_source_infinite_feed.jsonl"
    )
    
    print("\n📋 Testing with BENCHMARK configuration...")
    run_source_connector(
        config_type="BENCHMARK", 
        max_records=5,
        output_file="/tmp/manual_source_benchmark.jsonl"
    )
    
    print("\n✨ Manual testing complete!")
    print("💡 Examine the output files to understand connector message formats")
