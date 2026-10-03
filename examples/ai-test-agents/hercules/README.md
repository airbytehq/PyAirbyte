# Hercules AI Test Agent for PyAirbyte

This example demonstrates how to use [Hercules](https://github.com/test-zeus-ai/testzeus-hercules), an open-source AI testing agent, to test PyAirbyte functionality.

## About Hercules

Hercules is the world's first open-source testing agent that uses Gherkin format for test scenarios. It can perform UI, API, and other types of testing without requiring manual scripting.

## Prerequisites

1. Python 3.11 or higher
2. Hercules installed in a separate virtual environment (due to dependency conflicts with PyAirbyte)
3. OpenAI API key or other LLM provider credentials

## Installation

Since Hercules has a dependency conflict with PyAirbyte's airbyte-cdk (psutil version), it should be installed in a separate virtual environment:

```bash
# Create a separate virtual environment for Hercules
python -m venv hercules-env
source hercules-env/bin/activate  # On Windows: hercules-env\Scripts\activate

# Install Hercules
pip install testzeus-hercules

# Install Playwright (required by Hercules)
playwright install --with-deps
```

## Usage

1. Set your LLM API key:
```bash
export OPENAI_API_KEY="your-api-key-here"
```

2. Run Hercules with the test feature file:
```bash
testzeus-hercules --input-file test_pyairbyte.feature \
                  --output-path ./output \
                  --test-data-path ./test_data \
                  --llm-model gpt-4o \
                  --llm-model-api-key $OPENAI_API_KEY
```

## Test Scenario

The included `test_pyairbyte.feature` file contains a simple Gherkin scenario that tests basic PyAirbyte functionality:

- Installing PyAirbyte
- Creating a source connector
- Reading data from the source
- Validating the data

## Output

Hercules will generate:
- JUnit XML test results in `./output/`
- HTML test report in `./output/`
- Execution proofs (screenshots, videos, network logs) in `./proofs/`
- Detailed logs in `./log_files/`

## Limitations

Due to dependency conflicts (specifically psutil version requirements), Hercules cannot be installed in the same environment as PyAirbyte. This example demonstrates using Hercules in a separate environment to test PyAirbyte functionality.

## Alternative Approach

For integrated testing, consider:
1. Using Docker to run Hercules in an isolated container
2. Creating a CI/CD pipeline that runs Hercules tests separately
3. Using Hercules to test PyAirbyte's CLI or API endpoints rather than importing it directly
