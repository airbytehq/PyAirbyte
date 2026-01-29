# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Example of using IcebergCache with nested records.

This example demonstrates how to use the IcebergCache to store data from a source
that contains nested records (objects and arrays). The SpaceX API source is used
because it returns complex nested data structures like launch details, rocket specs,
and capsule information.

Usage (from PyAirbyte root directory):
> uv run python ./examples/run_iceberg_cache.py

The Iceberg cache stores data as Parquet files with Iceberg metadata, which enables:
- Time travel queries (access historical versions of data)
- Schema evolution (safely add/remove columns)
- Efficient querying with partition pruning
- Compatibility with query engines like Spark, Trino, and DuckDB
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import airbyte as ab
from airbyte.caches import IcebergCache


def main() -> None:
    """Run the Iceberg cache example with nested records."""
    # Create a temporary directory for the Iceberg warehouse
    # In production, you would use a persistent path or cloud storage (S3, GCS, etc.)
    with tempfile.TemporaryDirectory() as temp_dir:
        warehouse_path = Path(temp_dir) / "iceberg_warehouse"
        catalog_db = Path(temp_dir) / "iceberg_catalog.db"

        print(f"Using warehouse path: {warehouse_path}")
        print(f"Using catalog database: {catalog_db}")

        # Create an IcebergCache with a local SQLite catalog
        # For production use with cloud storage, you would configure a REST catalog:
        #   cache = IcebergCache(
        #       catalog_type="rest",
        #       catalog_uri="https://my-catalog.example.com",
        #       warehouse_path="s3://my-bucket/warehouse",
        #       catalog_credential="my-api-key",
        #   )
        cache = IcebergCache(
            warehouse_path=warehouse_path,
            catalog_uri=f"sqlite:///{catalog_db}",
            namespace="spacex_data",
        )

        # Use the SpaceX API source which has nested records
        # The launches stream contains nested objects like:
        # - links (object with nested urls)
        # - rocket (object with specs)
        # - failures (array of objects)
        source = ab.get_source(
            "source-spacex-api",
            config={"id": "605b4b6aaa5433645e37d03f"},
            install_if_missing=True,
        )

        print("\nChecking source connection...")
        source.check()

        # Select streams that have nested data
        # - launches: contains nested objects (links, rocket, failures, cores, payloads)
        # - rockets: contains nested arrays (payload_weights, engines)
        # - capsules: contains nested arrays (missions)
        source.select_streams(["launches", "rockets", "capsules"])

        print("\nReading data into Iceberg cache...")
        result = source.read(cache=cache)

        print("\nRead complete. Streams synced:")
        for name in result.streams:
            print(f"  - {name}")

        # Demonstrate reading data back from the Iceberg cache
        print("\n--- Reading data back from Iceberg cache ---")

        # Get data as a pandas DataFrame
        print("\nLaunches (first 3 rows as DataFrame):")
        launches_df = cache.get_pandas_dataframe("launches")
        print(launches_df.head(3).to_string())

        # Show the columns to see how nested data is stored
        print(f"\nLaunches columns: {list(launches_df.columns)}")

        # Get data as a PyArrow dataset for efficient querying
        print("\nRockets (as Arrow dataset):")
        rockets_dataset = cache.get_arrow_dataset("rockets")
        print(f"  Schema: {rockets_dataset.schema}")
        print(f"  Row count: {rockets_dataset.count_rows()}")

        # Access the underlying Iceberg table for advanced operations
        print("\nCapsules (Iceberg table metadata):")
        capsules_table = cache.get_iceberg_table("capsules")
        print(f"  Table location: {capsules_table.location()}")
        print(f"  Current snapshot: {capsules_table.current_snapshot()}")
        print(f"  Schema: {capsules_table.schema()}")

        # Iterate over records using Arrow dataset
        print("\nFirst 2 capsule records (via Arrow):")
        capsules_arrow = cache.get_arrow_dataset("capsules")
        row_count = 0
        for batch in capsules_arrow.to_batches():
            for row in batch.to_pylist():
                if row_count >= 2:
                    break
                print(f"  {row}")
                row_count += 1
            if row_count >= 2:
                break

        print("\nExample complete!")


if __name__ == "__main__":
    main()
