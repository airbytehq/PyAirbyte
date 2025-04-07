# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""HTTP caching for Airbyte connectors."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, cast

from mitmproxy.io import FlowReader, FlowWriter

from airbyte.constants import DEFAULT_HTTP_CACHE_DIR, DEFAULT_HTTP_CACHE_READ_DIR
from airbyte.http_caching.mitm_proxy import (
    build_combined_ca_bundle,
    find_free_port,
    get_proxy_host,
    mitm_get_proxy_env_vars,
    mitm_start_proxy,
    mitm_stop_proxy,
)
from airbyte.http_caching.modes import HttpCacheMode, SerializationFormat


if TYPE_CHECKING:
    import tempfile

    from mitmproxy.http import HTTPFlow


class AirbyteConnectorCache:
    """Cache for Airbyte connector HTTP requests and responses.

    This class manages an HTTP proxy that intercepts requests from connectors and either
    serves them from the cache or forwards them to the server based on the cache mode.
    """

    def __init__(
        self,
        cache_dir: str | Path | None = None,
        read_dir: str | Path | None = None,
        mode: str | HttpCacheMode = HttpCacheMode.READ_WRITE,
        serialization_format: str | SerializationFormat = SerializationFormat.NATIVE,
    ) -> None:
        """Initialize the cache.

        Args:
            cache_dir: The directory where cache files are stored for writing. If not provided,
                       the default directory will be used.
            read_dir: Optional separate directory for reading cached responses. If not provided,
                      cache_dir will be used for both reading and writing.
            mode: The cache mode to use. Can be one of 'read_only', 'write_only',
                  'read_write', or 'read_only_fail_on_miss'.
            serialization_format: The format to use for serializing cached data. Can be
                                 'json' or 'native'.
        """
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_HTTP_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if read_dir:
            self.read_dir = Path(read_dir)
        elif DEFAULT_HTTP_CACHE_READ_DIR:
            self.read_dir = DEFAULT_HTTP_CACHE_READ_DIR
        else:
            self.read_dir = self.cache_dir

        self.read_dir.mkdir(parents=True, exist_ok=True)

        self.mode = HttpCacheMode(mode) if isinstance(mode, str) else mode

        self.serialization_format = (
            SerializationFormat(serialization_format)
            if isinstance(serialization_format, str)
            else serialization_format
        )

        self._proxy_port: int | None = None
        self._mitm_process: subprocess.Popen | None = None
        self._temp_dir: tempfile.TemporaryDirectory | None = None

        self._init_cache_dir()

    def _init_cache_dir(self) -> None:
        """Initialize the cache directory by invoking mitmdump with --version."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.ca_cert_path.exists():
            # If the (combined) CA certificate already exists, no need to create it again
            return

        if not self.mitmproxy_ca_cert_path.exists():
            # Create the CA certificate if it doesn't exist
            # Run mitmproxy to generate the certificate
            subprocess.run(
                ["mitmdump", "--set", f"confdir={self.cache_dir}", "--version"],
                check=True,
                capture_output=True,
                text=True,
            )

        if not self.mitmproxy_ca_cert_path.exists():
            raise ValueError("Cert file could not be created.")

        combined_cert_path = self.ca_cert_path
        build_combined_ca_bundle(
            mitm_cert_path=self.mitmproxy_ca_cert_path,
            output_cert_path=combined_cert_path,
        )
        if not combined_cert_path.exists():
            raise ValueError("Combined cert file could not be created.")

    @property
    def mitmproxy_ca_cert_path(self) -> Path:
        """Return the path to the CA certificate file."""
        return self.cache_dir / "mitmproxy-ca-cert.pem"

    @property
    def ca_cert_path(self) -> Path:
        """Return the path to the CA certificate file."""
        return self.cache_dir / "mitmproxy-ca-cert-combined.pem"

    @property
    def proxy_host(self) -> str:
        """Return 'localhost' or another host name."""
        return get_proxy_host()

    @property
    def proxy_port(self) -> int:
        """Return the port number the proxy is listening on."""
        if self._proxy_port is None:
            self._proxy_port = find_free_port()
        assert self._proxy_port is not None, "Proxy port should be set at this point"
        return self._proxy_port

    def get_proxy_env_vars(self) -> dict[str, str]:
        """Get the environment variables to apply to processes using the HTTP proxy."""
        return mitm_get_proxy_env_vars(self.proxy_port)

    def start(self) -> int:
        """Start the HTTP proxy.

        Returns:
            The port number the proxy is listening on.
        """
        if self._mitm_process is not None and self._proxy_port is not None:
            # Proxy is already running
            return self._proxy_port

        self._mitm_process = mitm_start_proxy(
            cache_dir=self.cache_dir,
            read_dir=self.read_dir,
            mode=self.mode,
            serialization_format=self.serialization_format.value,
            proxy_port=self.proxy_port,
            stop_callback=self.stop,
        )

        return self.proxy_port

    def get_docker_volumes(self) -> dict[str, str]:
        """Get the Docker volume mappings needed for the proxy.

        Returns:
            A dictionary of host paths to container paths for Docker volume mappings.
        """
        volumes = {}

        # Add the CA certificate volume if available
        if not self.ca_cert_path:
            raise ValueError("CA certificate path is not set.")

        if not self.ca_cert_path.exists():
            raise ValueError(f"CA certificate path does not exist: {self.ca_cert_path}")

        volumes[str(self.ca_cert_path.absolute())] = (
            "/usr/local/lib/python3.11/site-packages/certifi/cacert.pem"
        )
        return volumes

    def stop(self) -> None:
        """Stop the HTTP proxy."""
        mitm_stop_proxy(
            mitm_process=self._mitm_process,
            stop_callback=self.stop,
        )
        self._mitm_process = None
        self._proxy_port = None

    def consolidate(
        self,
        *,
        with_har_export: bool = True,
    ) -> None:
        """Consolidate and deduplicate all .flows files in the session cache.

        Including previously consolidated files with '-to-' in their names.
        """
        session_dir = self.cache_dir / "sessions"
        flow_files = sorted(session_dir.glob("*.flows"))
        if not flow_files:
            print(
                f"No thing to do during cache consolidation. No flows files found in: {session_dir}"
            )
            return

        # Detect min and max timestamps based on file names
        min_ts, max_ts = None, None

        def extract_stem_range(stem: str) -> tuple[str, str]:
            if "-to-" in stem:
                return stem.split("-to-")[0], stem.split("-to-")[1]
            return stem, stem

        all_ranges = [extract_stem_range(f.stem) for f in flow_files]
        min_ts = min(start for start, _ in all_ranges)
        max_ts = max(end for _, end in all_ranges)

        output_stem = f"{min_ts}-to-{max_ts}"
        output_flows = session_dir / f"{output_stem}.flows"
        output_har = session_dir / f"{output_stem}.har"

        # Deduplicate flows
        seen = set()
        consolidated_flows: list[HTTPFlow] = []
        for flow_path in flow_files:
            try:
                with flow_path.open("rb") as f:
                    reader = FlowReader(f)
                    for flow in reader.stream():
                        flow = cast("HTTPFlow", flow)
                        key = (flow.request.method, flow.request.pretty_url)
                        if key not in seen:
                            seen.add(key)
                            consolidated_flows.append(flow)
                        else:
                            print(f"Duplicate flow found: {flow.request.pretty_url}")
            except Exception as e:
                print(f"⚠️ Failed to read {flow_path.name}: {e}")

        # Write consolidated .flows
        with output_flows.open("wb") as f:
            writer = FlowWriter(f)
            for flow in consolidated_flows:
                writer.add(flow)
        print(f"✅ Wrote {len(consolidated_flows)} deduplicated flows to {output_flows}")

        if with_har_export:
            # Convert to HAR
            try:
                subprocess.run(
                    ["mitmdump", "-nr", str(output_flows), "--set", f"hardump={output_har}"],
                    check=True,
                )
                print(f"📝 Wrote HAR file to {output_har}")
            except subprocess.CalledProcessError as e:
                print(f"❌ HAR export failed: {e}")

        # Delete source files
        for path in flow_files:
            try:
                path.unlink()
                print(f"🗑️ Deleted {path.name}")
            except Exception as e:
                print(f"⚠️ Could not delete {path.name}: {e}")
