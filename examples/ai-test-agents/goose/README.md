# Goose AI Developer Agent for PyAirbyte

This example demonstrates how to use [Goose](https://github.com/block/goose), an open-source AI developer agent from Block (formerly Square), to test and validate PyAirbyte functionality.

## About Goose

Goose is an AI developer agent that runs on your machine and helps with coding tasks. It can execute code, run tests, debug issues, and automate development workflows. Goose supports any LLM provider and integrates with MCP (Model Context Protocol) servers.

## Prerequisites

1. Goose installed on your system
2. Python 3.10 or higher
3. PyAirbyte installed
4. LLM API key (OpenAI, Anthropic, or other supported provider)

## Installation

### Install Goose

**Option 1: Using Homebrew (macOS/Linux)**
```bash
brew install block/goose/goose
```

**Option 2: Using pipx (Cross-platform)**
```bash
pipx install goose-ai
```

**Option 3: Using cargo (Rust)**
```bash
cargo install goose-cli
```

### Install PyAirbyte

```bash
pip install airbyte
```

## Usage

### Interactive Mode

Start Goose in interactive mode and ask it to test PyAirbyte:

```bash
goose session start
```

Then provide prompts like:

```
Test PyAirbyte by:
1. Creating a source-faker connector with count=10
2. Reading data into a local cache
3. Validating that data was successfully read
4. Printing the first few records
```

### Session File Mode

You can also create a session file with predefined tasks:

```bash
goose session start --plan test_pyairbyte_session.md
```

See `test_pyairbyte_session.md` for an example session plan.

### Using Goose with MCP

Goose can integrate with PyAirbyte's MCP server to access connector functionality:

1. Start PyAirbyte's MCP server:
```bash
airbyte-mcp
```

2. Configure Goose to use the MCP server (add to `~/.config/goose/profiles.yaml`):
```yaml
default:
  provider: openai
  processor: gpt-4o
  accelerator: gpt-4o-mini
  moderator: passive
  mcp_servers:
    airbyte:
      command: airbyte-mcp
```

3. Start Goose and it will have access to PyAirbyte's 44+ MCP tools

## Example Tasks

### Task 1: Basic Connector Test

Ask Goose to:
```
Write and run a Python script that:
1. Imports airbyte
2. Creates a source-faker connector
3. Checks the connection
4. Reads 10 records into a local cache
5. Validates the data and prints results
```

### Task 2: Connector Discovery

Ask Goose to:
```
Write a script to discover all available PyAirbyte source connectors
and verify that source-faker is in the list
```

### Task 3: Data Validation

Ask Goose to:
```
Create a test that:
1. Reads data from source-faker
2. Validates the schema of the returned data
3. Checks that all expected columns are present
4. Verifies data types are correct
```

## Example Session Output

When you run Goose with the test tasks, you should see output like:

```
🪿 Goose: I'll help you test PyAirbyte. Let me create a test script...

[Goose creates and runs a Python script]

✓ Successfully created source-faker connector
✓ Connection check passed
✓ Read 10 records into cache
✓ Data validation passed

Results:
- Stream: users
- Records: 10
- Columns: id, name, email, created_at
- All validations passed ✓
```

## Advantages of Using Goose

1. **Interactive Testing**: Goose can interactively test PyAirbyte and adapt based on results
2. **Code Generation**: Automatically generates test scripts and validation code
3. **Error Handling**: Can debug and fix issues it encounters during testing
4. **MCP Integration**: Can leverage PyAirbyte's MCP server for advanced operations
5. **Multi-step Workflows**: Can execute complex test scenarios with multiple steps

## Comparison with Hercules

While Hercules uses predefined Gherkin scenarios for testing, Goose provides:
- More flexible, conversational testing approach
- Ability to adapt tests based on intermediate results
- Code generation and debugging capabilities
- Integration with development tools and MCP servers

## Limitations

- Goose requires an LLM API key and makes API calls for each interaction
- Results may vary based on the LLM model used
- Less deterministic than traditional test frameworks
- Best suited for exploratory testing and development tasks

## Additional Resources

- [Goose Documentation](https://block.github.io/goose/)
- [Goose GitHub Repository](https://github.com/block/goose)
- [PyAirbyte Documentation](https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started)
- [PyAirbyte MCP Server](https://docs.airbyte.com/using-airbyte/pyairbyte/mcp-server)
