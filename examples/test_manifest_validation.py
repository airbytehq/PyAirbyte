#!/usr/bin/env python3
"""Test script to validate the Rick & Morty manifest format."""

import yaml
from pathlib import Path

def test_manifest_validation():
    """Test that the Rick & Morty manifest can be loaded and validated."""
    print("Testing Rick & Morty manifest validation")
    print("=" * 40)
    
    try:
        manifest_path = Path(__file__).parent / "rick_morty_manifest.yaml"
        with open(manifest_path, "r") as f:
            manifest_config = yaml.safe_load(f)
        
        print("✓ Manifest loaded successfully")
        print(f"Version: {manifest_config.get('version')}")
        print(f"Type: {manifest_config.get('type')}")
        print(f"Streams: {len(manifest_config.get('streams', []))}")
        
        from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
        
        import copy
        manifest_copy = copy.deepcopy(manifest_config)
        manifest_config["__injected_declarative_manifest"] = manifest_copy
        manifest_config["config"] = {}
        
        source = ManifestDeclarativeSource(
            source_config=manifest_config,
            emit_connector_builder_messages=True
        )
        
        print("✓ ManifestDeclarativeSource created successfully")
        
        check_result = source.check_connection(logger=source.logger, config={})
        print(f"✓ Connection check: {check_result.status.name}")
        if check_result.status.name != "SUCCEEDED":
            print(f"  Message: {check_result.message}")
        
        streams = list(source.streams({}))
        print(f"✓ Found {len(streams)} streams:")
        for stream in streams:
            print(f"  - {stream.name}")
        
        return manifest_config
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_manifest_validation()
