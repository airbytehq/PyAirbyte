# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A test of Polars in PyAirbyte.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_polars_poc.py
"""

from __future__ import annotations

from collections.abc import Iterator
from io import BytesIO, StringIO
from typing import TextIO

import airbyte as ab
import polars as pl
from airbyte import get_source
from airbyte._message_iterators import AirbyteMessageIterator
from airbyte._util.polars import PolarsStreamSchema
from airbyte.progress import ProgressTracker


def get_my_source() -> ab.Source:
    return get_source(
        "source-faker",
        config={},
        streams=["users"],
    )


def main() -> None:
    """Run the Polars proof of concept."""
    source = get_my_source()

    polars_stream_schema: PolarsStreamSchema = PolarsStreamSchema.from_json_schema(
        json_schema=source.configured_catalog.streams[0].stream.json_schema,
    )
    progress_tracker = ProgressTracker(
        source=source,
        cache=None,
        destination=None,
    )
    msg_iterator = AirbyteMessageIterator(
        msg
        for msg in source._get_airbyte_message_iterator(
            streams=["users"],
            progress_tracker=progress_tracker,
        )
    )
    # jsonl_iterator = (msg.model_dump_json() for msg in msg_iterator)
    # df = pl.read_ndjson(
    #     StringIO("\n".join(jsonl_iterator)),
    #     schema=polars_stream_schema.polars_schema,
    # )
    filelike = msg_iterator.as_filelike()
    print(filelike.readlines())
    df = pl.read_ndjson(
        filelike,
        schema=polars_stream_schema.polars_schema,
    )


if __name__ == "__main__":
    main()
