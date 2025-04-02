# HTTP Caching for PyAirbyte

This module provides HTTP caching functionality for Airbyte connectors using mitmproxy's CLI tool.

## Serialization Formats

### Native Format

The native format is mitmproxy's native serialization format (.mitm). This format is used to store HTTP 
flows (requests and responses) in a format that is natively readable by mitmproxy tools. 
Using this format allows for interoperability with other tools in the mitmproxy ecosystem.

### JSON Format

The JSON format provides a human-readable alternative for storing HTTP flows. This makes it easier
to inspect and debug cached responses.

## HAR Format (Not Currently Implemented)

HAR (HTTP Archive) is a standard format for logging HTTP transactions in JSON format.
It's commonly used by browser developer tools and HTTP monitoring applications to capture
detailed information about web requests and responses. Some key features of HAR:

1. It records complete request and response data including headers, content, and timing
2. It's human-readable and can be analyzed with various tools
3. It's widely supported by browser developer tools and HTTP analysis applications

HAR format is not currently implemented in PyAirbyte's HTTP caching system, but may be considered
for future implementations where detailed HTTP transaction logging is important.

## Usage

```python
from airbyte import get_source, AirbyteConnectorCache

# Create an HTTP cache
cache = AirbyteConnectorCache(
    mode="read_write",
    serialization_format="native"  # Use mitmproxy's native format
)

# Start the proxy
port = cache.start()
print(f"HTTP cache started on port {port}")

# Use the cache with a source
source = get_source(
    name="source-github",
    config={"repository": "airbytehq/airbyte"},
    http_cache=cache,
    docker_image=True,
    use_host_network=True,  # Important for Docker containers to access the proxy
)

# The source will now use the HTTP cache for all requests
source.read()

# Stop the proxy when done
cache.stop()
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

## Implementation Details

The HTTP caching system uses mitmproxy's CLI tool (`mitmdump`) rather than its Python API. This approach offers several advantages:

1. **Simplified Dependencies**: Reduces dependency on mitmproxy's internal Python API which can change between versions
2. **Better Isolation**: The proxy runs in a separate process, preventing potential conflicts with the main application
3. **Improved Stability**: CLI interfaces tend to be more stable than internal APIs

The system works by:

1. Starting a `mitmdump` process with a custom script
2. Setting up environment variables to configure the caching behavior
3. Intercepting HTTP(S) requests through the proxy
4. Either serving cached responses or forwarding requests to the server based on the cache mode

## Requirements

- mitmproxy must be installed and available in the PATH
- For Docker containers, the `use_host_network=True` parameter should be set when creating a source to allow the container to access the proxy on the host machine
