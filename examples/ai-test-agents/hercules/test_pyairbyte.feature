Feature: PyAirbyte Basic Functionality Test

  Scenario: Test PyAirbyte source connector with faker
    Given I have Python installed on my system
    When I install PyAirbyte using "pip install airbyte"
    And I create a Python script with the following code:
      """
      import airbyte as ab
      
      # Create a source connector
      source = ab.get_source(
          "source-faker",
          config={"count": 10},
          install_if_missing=True
      )
      
      # Check the connection
      source.check()
      
      # Read data into a local cache
      cache = ab.new_local_cache()
      result = source.read(cache)
      
      # Get data from a stream
      df = cache["users"].to_pandas()
      
      # Validate we got data
      assert len(df) > 0, "No data was read from source"
      assert "id" in df.columns, "Expected 'id' column not found"
      
      print(f"Successfully read {len(df)} records from source-faker")
      """
    And I run the Python script
    Then the script should execute successfully
    And I should see output containing "Successfully read"
    And I should see output containing "records from source-faker"

  Scenario: Test PyAirbyte connector discovery
    Given I have PyAirbyte installed
    When I create a Python script to discover available connectors:
      """
      import airbyte as ab
      from airbyte.registry import get_available_connectors
      
      # Get list of available source connectors
      sources = get_available_connectors(connector_type="source")
      
      # Validate we have connectors
      assert len(sources) > 0, "No source connectors found"
      
      # Check that source-faker is available
      faker_found = any(c.name == "source-faker" for c in sources)
      assert faker_found, "source-faker not found in available connectors"
      
      print(f"Found {len(sources)} source connectors")
      print("source-faker is available")
      """
    And I run the Python script
    Then the script should execute successfully
    And I should see output containing "source connectors"
    And I should see output containing "source-faker is available"
