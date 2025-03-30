# HTTP Caching for PyAirbyte

This module provides HTTP caching functionality for Airbyte connectors using mitmproxy's Python API.

## Serialization Formats

### Native Format

The native format is mitmproxy's native serialization format (.mitm). This format is used to store HTTP 
flows (requests and responses) in a format that is natively readable by mitmproxy tools. 
Using this format allows for interoperability with other tools in the mitmproxy ecosystem.

### JSON Format

The JSON format provides a human-readable alternative for storing HTTP flows. This makes it easier
to inspect and debug cached responses.

## HAL Format (Not Currently Implemented)

HAL (Hypertext Application Language) is a standard format for representing hypermedia APIs in JSON 
or XML. It's designed to make APIs more discoverable and self-documenting by including links to 
related resources. Some key features of HAL:

1. It includes hyperlinks to related resources via `_links` property
2. It can include embedded resources via `_embedded` property
3. It's a common format for RESTful APIs

HAL format is not currently implemented in PyAirbyte's HTTP caching system, but may be considered
for future implementations where API discoverability is important.

## Usage

```python
from airbyte import get_source, AirbyteConnectorCache

# Create an HTTP cache
cache = AirbyteConnectorCache(
    mode="read_write",
    serialization_format="native"  # Use mitmproxy's native format
)

# Use the cache with a source
source = get_source(
    name="source-github",
    config={"repository": "airbytehq/airbyte"},
    http_cache=cache
)

# The source will now use the HTTP cache for all requests
```

## Cache Modes

The HTTP cache supports four modes:

1. **Read Only**: Only read from the cache. If a request is not in the cache, it will be made to the server.
2. **Write Only**: Only write to the cache. All requests will be made to the server and cached.
3. **Read/Write**: Read from the cache if available, otherwise make the request to the server and cache it.
4. **Read Only Fail on Miss**: Only read from the cache. If a request is not in the cache, an exception will be raised.

## Configuration

By default, cache files are stored in a local directory called `.airbyte-http-cache`. This can be overridden using:

1. The `cache_dir` parameter when creating an `AirbyteConnectorCache` instance
2. The `AIRBYTE_HTTP_CACHE_DIR` environment variable

For separate read and write directories, you can use:

1. The `read_dir` parameter when creating an `AirbyteConnectorCache` instance
2. The `AIRBYTE_HTTP_CACHE_READ_DIR` environment variable
