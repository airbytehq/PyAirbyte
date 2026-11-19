# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import json
import sys

sample_spec = {
    "type": "SPEC",
    "spec": {
        "documentationUrl": "https://example.com",
        "connectionSpecification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "apiKey": {"type": "string"},
            },
        },
    },
}


def run() -> None:
    if sys.argv[1] == "spec":
        print(json.dumps(sample_spec))
