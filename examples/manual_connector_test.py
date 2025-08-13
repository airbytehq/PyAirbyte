#!/usr/bin/env python3
"""Helper script for manually testing source connectors with small record counts."""

import json
import subprocess
import tempfile
from pathlib import Path

CONNECTOR_IMAGE = "airbyte/source-e2e-test:dev"
CONFIG = {
    "type": "INFINITE_FEED",
    "max_records": 5,
    "seed": 0,
    "message_interval": 1000,
}
CATALOG = {
    "streams": [
        {
            "stream": {
                "name": "data",
                "json_schema": {},
                "supported_sync_modes": ["full_refresh"],
            },
            "sync_mode": "full_refresh",
            "destination_sync_mode": "overwrite",
        }
    ]
}
OUTPUT_FILE = "/tmp/manual_source_output.jsonl"


def run_connector() -> None:
    """Run the source connector and capture its output."""
    config_file = Path(tempfile.mktemp(suffix=".json"))
    catalog_file = Path(tempfile.mktemp(suffix=".json"))

    config_file.write_text(json.dumps(CONFIG, indent=2))
    catalog_file.write_text(json.dumps(CATALOG, indent=2))

    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{config_file}:/tmp/config.json",
        "-v",
        f"{catalog_file}:/tmp/catalog.json",
        CONNECTOR_IMAGE,
        "read",
        "--config",
        "/tmp/config.json",
        "--catalog",
        "/tmp/catalog.json",
    ]

    with open(OUTPUT_FILE, "w") as f:
        subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, timeout=60)

    config_file.unlink(missing_ok=True)
    catalog_file.unlink(missing_ok=True)

    lines = Path(OUTPUT_FILE).read_text().strip().split("\n")
    print(f"Generated {len(lines)} lines in {OUTPUT_FILE}")
    for i, line in enumerate(lines[:3]):
        print(f"  {i + 1}: {line}")


if __name__ == "__main__":
    print("Manual Source Connector Test")
    run_connector()
    print("Complete!")
