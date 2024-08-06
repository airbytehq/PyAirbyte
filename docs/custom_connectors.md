
---

### Using Custom Connectors in PyAirbyte

#### Overview

Custom connectors in PyAirbyte allow users to extend the functionality of the platform by integrating additional data sources and destinations. This guide provides step-by-step instructions for creating and utilizing custom connectors.

#### Prerequisites

- **Python Version:** Ensure Python 3.7 or higher is installed.
- **PyAirbyte Installation:** Install PyAirbyte using the following command:
  ```sh
  pip install pyairbyte
  ```

#### Defining a Custom Connector

To create a custom connector, you need to extend either the `AbstractSource` class for source connectors or the `AbstractDestination` class for destination connectors.

**Example: Source Connector**

```python
from pyairbyte import AbstractSource

class CustomSource(AbstractSource):
    def check(self, logger, config):
        # Validate the connection configuration.
        pass

    def streams(self, config):
        # Return a list of available streams.
        pass

    def read_records(self, logger, config, catalog, state=None):
        # Fetch records from the source.
        pass
```

#### Implementing Required Methods

1. **`check` Method:** Validates the connection configuration to ensure that it is correct and that the connector can establish a connection.
2. **`streams` Method:** Returns a list of available streams from which data can be extracted.
3. **`read_records` Method:** Fetches records from the defined streams.

#### Testing Your Custom Connector

1. **Create Tests:** Use `pytest` to write a test suite for your custom connector.
2. **Validate Functionality:** Ensure that all methods are functioning as expected by running your tests.

#### Integrating with PyAirbyte

1. **Register the Custom Connector:** Use PyAirbyte's API or CLI to register your custom connector.
2. **Example Usage:**

   ```python
   from pyairbyte import AirbyteClient

   client = AirbyteClient()
   client.run_custom_connector(CustomSource())
   ```

#### Additional Resources

For more detailed examples and code snippets, please refer to the [notebook](https://colab.research.google.com/drive/1SPEmVy4xtalsxVDyAetIYoioi0oykPfp#scrollTo=ze2DPFblIXYF).

---

This guide offers a comprehensive overview of setting up and using custom connectors in PyAirbyte. For further information or assistance, please refer to the PyAirbyte documentation or community resources.