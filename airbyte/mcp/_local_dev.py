# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local development MCP operations."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from airbyte.sources import get_available_connectors


def generate_pyairbyte_pipeline(
    source_connector_name: Annotated[
        str,
        Field(description="The name of the source connector (e.g., 'source-faker')."),
    ],
    destination_connector_name: Annotated[
        str,
        Field(description="The name of the destination connector (e.g., 'destination-duckdb')."),
    ],
    pipeline_name: Annotated[
        str,
        Field(description="A descriptive name for the pipeline."),
    ] = "my_pipeline",
) -> dict[str, str]:
    """Generate a PyAirbyte pipeline script with setup instructions.

    This tool creates a complete PyAirbyte pipeline script that extracts data from
    a source connector and loads it to a destination connector, along with setup
    instructions for running the pipeline.

    Returns a dictionary with 'code' and 'instructions' keys containing the
    generated pipeline script and setup instructions respectively.
    """
    available_connectors = get_available_connectors()

    if source_connector_name not in available_connectors:
        return {
            "error": (
                f"Source connector '{source_connector_name}' not found. "
                f"Available connectors: {', '.join(sorted(available_connectors))}"
            )
        }

    if destination_connector_name not in available_connectors:
        return {
            "error": (
                f"Destination connector '{destination_connector_name}' not found. "
                f"Available connectors: {', '.join(sorted(available_connectors))}"
            )
        }

    pipeline_code = f'''#!/usr/bin/env python3
"""
{pipeline_name} - PyAirbyte Pipeline
Generated pipeline for {source_connector_name} -> {destination_connector_name}
"""

import airbyte as ab

def main():
    """Main pipeline function."""
    source = ab.get_source(
        "{source_connector_name}",
        config={{
        }}
    )

    destination = ab.get_destination(
        "{destination_connector_name}",
        config={{
        }}
    )

    read_result = source.read()

    write_result = destination.write(read_result)

    print(f"Pipeline completed successfully!")
    print(f"Records processed: {{write_result.processed_records}}")

if __name__ == "__main__":
    main()
'''

    source_name = source_connector_name.replace("source-", "")
    dest_name = destination_connector_name.replace("destination-", "")

    setup_instructions = f"""# {pipeline_name} Setup Instructions

1. Install PyAirbyte:
   ```bash
   pip install airbyte
   ```

2. Install the required connectors:
   ```bash
   python -c "import airbyte as ab; ab.get_source('{source_connector_name}').install()"
   python -c "import airbyte as ab; ab.get_destination('{destination_connector_name}').install()"
   ```

## Configuration
1. Update the source configuration in the pipeline script with your actual connection details
2. Update the destination configuration in the pipeline script with your actual connection details
3. Refer to the Airbyte documentation for each connector's required configuration fields

## Running the Pipeline
```bash
python {pipeline_name.lower().replace(' ', '_')}.py
```

- Configure your source and destination connectors with actual credentials
- Add error handling and logging as needed
- Consider using environment variables for sensitive configuration
- Add stream selection if you only need specific data streams
- Set up scheduling using your preferred orchestration tool (Airflow, Dagster, etc.)

- Source connector docs: https://docs.airbyte.com/integrations/sources/{source_name}
- Destination connector docs: https://docs.airbyte.com/integrations/destinations/{dest_name}
- PyAirbyte docs: https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started
"""

    return {
        "code": pipeline_code,
        "instructions": setup_instructions,
        "filename": f"{pipeline_name.lower().replace(' ', '_')}.py",
    }


def register_local_dev_tools(app: FastMCP) -> None:
    """Register development tools with the FastMCP app."""
    app.tool(generate_pyairbyte_pipeline)
