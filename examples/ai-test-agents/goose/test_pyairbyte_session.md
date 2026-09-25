# Goose Session: Test PyAirbyte Functionality

This session file contains a series of tasks for Goose to execute when testing PyAirbyte.

## Session Goal

Test basic PyAirbyte functionality including connector creation, data reading, and validation.

## Tasks

### Task 1: Test Basic Source Connector

Create and run a Python script that:

1. Imports the airbyte library
2. Creates a source-faker connector with the following configuration:
   - count: 10
3. Checks the connection using the `check()` method
4. Creates a local DuckDB cache
5. Reads data from the source into the cache
6. Retrieves the data from the "users" stream as a pandas DataFrame
7. Validates that:
   - At least 1 record was read
   - The DataFrame contains an "id" column
8. Prints a success message with the number of records read

Expected output: Script should execute successfully and print "Successfully read N records from source-faker"

### Task 2: Test Connector Discovery

Create and run a Python script that:

1. Imports the airbyte library
2. Uses `get_available_connectors()` to retrieve all available source connectors
3. Validates that:
   - At least one source connector is available
   - The "source-faker" connector is in the list
4. Prints the total number of source connectors found
5. Confirms that source-faker is available

Expected output: Script should print the number of connectors and confirm source-faker availability

### Task 3: Test Stream Selection

Create and run a Python script that:

1. Creates a source-faker connector
2. Discovers the available streams using `get_available_streams()`
3. Prints the list of available stream names
4. Selects only the "users" stream
5. Reads data from only the selected stream
6. Validates that only the "users" stream data is present in the cache

Expected output: Script should show available streams and confirm successful selective sync

### Task 4: Test Data Schema Validation

Create and run a Python script that:

1. Creates a source-faker connector
2. Reads data into a cache
3. Retrieves the "users" stream data
4. Validates the schema by checking for expected columns:
   - id
   - name (or similar name field)
   - email (or similar email field)
5. Prints the actual columns found
6. Validates that the data types are appropriate (e.g., id is numeric or string)

Expected output: Script should print the schema and confirm all expected columns are present

## Success Criteria

All four tasks should complete successfully with:
- No Python exceptions or errors
- All validation assertions passing
- Clear output messages confirming success
- Proper cleanup of resources

## Notes for Goose

- Use proper error handling in all scripts
- Print clear status messages for each step
- If any task fails, provide detailed error information
- Clean up resources (close connections, etc.) after each task
- Use the latest PyAirbyte API patterns
