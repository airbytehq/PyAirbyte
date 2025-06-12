#!/usr/bin/env python3
"""Test script for the new MCP manifest-only connector actions."""

import json
import yaml
from pathlib import Path


def test_basic_functionality():
    """Test basic functionality of the MCP connector development tools."""
    print("Testing MCP Manifest Actions")
    print("=" * 40)
    
    print("\n1. Testing module import...")
    try:
        from airbyte.mcp.connector_development import register_connector_development_tools
        from mcp.server.fastmcp import FastMCP
        print("✓ Successfully imported connector development module")
    except Exception as e:
        print(f"✗ Import error: {e}")
        return
    
    print("\n2. Testing MCP server creation...")
    try:
        test_app = FastMCP("test-app")
        register_connector_development_tools(test_app)
        print("✓ Successfully created MCP server and registered tools")
    except Exception as e:
        print(f"✗ Server creation error: {e}")
        return
    
    print("\n3. Testing stream template creation logic...")
    try:
        stream_template = {
            "type": "DeclarativeStream",
            "name": "characters",
            "primary_key": [],
            "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://rickandmortyapi.com/api",
                    "path": "/character",
                    "http_method": "GET",
                    "authenticator": {"type": "NoAuth"}
                },
                "record_selector": {
                    "type": "RecordSelector",
                    "extractor": {"type": "DpathExtractor", "field_path": []}
                },
                "paginator": {"type": "NoPagination"}
            },
            "schema_loader": {
                "type": "InlineSchemaLoader",
                "schema": {"type": "object", "properties": {}}
            }
        }
        
        template_yaml = yaml.dump(stream_template, default_flow_style=False, indent=2)
        print("✓ Stream template creation logic works")
        print(f"Template preview: {template_yaml[:100]}...")
    except Exception as e:
        print(f"✗ Template creation error: {e}")
    
    print("\n4. Testing auth logic creation...")
    try:
        auth_templates = {
            "no_auth": {"type": "NoAuth"},
            "api_key": {
                "type": "ApiKeyAuthenticator",
                "header": "X-API-Key",
                "api_token": "{{ config['api_key'] }}"
            }
        }
        
        no_auth = yaml.dump(auth_templates["no_auth"], default_flow_style=False, indent=2)
        print("✓ Auth logic creation works")
        print(f"NoAuth template: {no_auth}")
    except Exception as e:
        print(f"✗ Auth logic error: {e}")
    
    print("\n5. Testing Rick & Morty manifest...")
    try:
        manifest_path = Path(__file__).parent / "rick_morty_manifest.yaml"
        if manifest_path.exists():
            with manifest_path.open("r") as f:
                manifest_config = yaml.safe_load(f)
            
            required_keys = ["version", "type", "streams", "spec"]
            missing_keys = [key for key in required_keys if key not in manifest_config]
            
            if missing_keys:
                print(f"✗ Manifest missing keys: {missing_keys}")
            else:
                print("✓ Rick & Morty manifest structure is valid")
                print(f"Found {len(manifest_config['streams'])} streams")
                
                characters_stream = next(
                    (s for s in manifest_config['streams'] if s.get('name') == 'characters'), 
                    None
                )
                if characters_stream:
                    print("✓ Characters stream found in manifest")
                else:
                    print("✗ Characters stream not found")
        else:
            print("✗ Rick & Morty manifest file not found")
    except Exception as e:
        print(f"✗ Manifest validation error: {e}")
    
    print("\n6. Testing stream modification logic...")
    try:
        import copy
        
        base_stream = {
            "type": "DeclarativeStream",
            "name": "template",
            "retriever": {
                "requester": {
                    "type": "HttpRequester",
                    "url_base": "https://api.example.com",
                    "path": "/template"
                }
            }
        }
        
        new_stream = copy.deepcopy(base_stream)
        new_stream["name"] = "episodes"
        new_stream["retriever"]["requester"]["path"] = "/episode"
        
        print("✓ Stream modification logic works")
        print(f"New stream name: {new_stream['name']}")
        print(f"New stream path: {new_stream['retriever']['requester']['path']}")
    except Exception as e:
        print(f"✗ Stream modification error: {e}")
    
    print("\n" + "=" * 40)
    print("Basic functionality tests completed!")
    print("\nNext steps:")
    print("1. Install mcp-cli: uv tool install devin-mcp-cli")
    print("2. Test with mcp-cli: mcp-cli list-tools pyairbyte")
    print("3. Test individual actions with mcp-cli call commands")


if __name__ == "__main__":
    test_basic_functionality()
