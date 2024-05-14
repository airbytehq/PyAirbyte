# PyAirbyte

PyAirbyte brings the power of Airbyte to every Python developer. PyAirbyte provides a set of utilities to use Airbyte connectors in Python.

[![PyPI version](https://badge.fury.io/py/airbyte.svg)](https://badge.fury.io/py/airbyte)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/airbyte)](https://pypi.org/project/airbyte/)
<!-- [![PyPI - License](https://img.shields.io/pypi/l/airbyte)](https://pypi.org/project/airbyte/) -->
[![PyPI - Wheel](https://img.shields.io/pypi/wheel/airbyte)](https://pypi.org/project/airbyte/)
<!-- [![PyPI - Status](https://img.shields.io/pypi/status/airbyte)](https://pypi.org/project/airbyte/) -->
[![PyPI - Implementation](https://img.shields.io/pypi/implementation/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Format](https://img.shields.io/pypi/format/airbyte)](https://pypi.org/project/airbyte/)
[![Star on GitHub](https://img.shields.io/github/stars/airbytehq/pyairbyte.svg?style=social&label=★%20on%20GitHub)](https://github.com/airbytehq/pyairbyte)

- [Getting Started](#getting-started)
- [Secrets Management](#secrets-management)
- [Connector compatibility](#connector-compatibility)
- [Contributing](#contributing)
- [Frequently asked Questions](#frequently-asked-questions)

## Getting Started

Watch this [Getting Started Loom video](https://www.loom.com/share/3de81ca3ce914feca209bf83777efa3f?sid=8804e8d7-096c-4aaa-a8a4-9eb93a44e850) or run one of our Quickstart tutorials below to see how you can use PyAirbyte in your python code.

* [Basic Demo](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Basic_Features_Demo.ipynb)
* [CoinAPI](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_CoinAPI_Demo.ipynb)
* [GA4](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_GA4_Demo.ipynb)
* [Shopify](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Shopify_Demo.ipynb)
* [GitHub](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Github_Incremental_Demo.ipynb)
* [Postgres (cache)](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Postgres_Custom_Cache_Demo.ipynb)


## Secrets Management

PyAirbyte can auto-import secrets from the following sources:

1. Environment variables.
2. Variables defined in a local `.env` ("Dotenv") file.
3. [Google Colab secrets](https://medium.com/@parthdasawant/how-to-use-secrets-in-google-colab-450c38e3ec75).
4. Manual entry via [`getpass`](https://docs.python.org/3.9/library/getpass.html).

_Note: You can also build your own secret manager by subclassing the `CustomSecretManager` implementation. For more information, see the `airbyte.secrets.CustomSecretManager` class definiton._

### Retrieving Secrets

```python
import airbyte as ab

source = ab.get_source("source-github")
source.set_config(
   "credentials": {
      "personal_access_token": ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN"),
   }
)
```

By default, PyAirbyte will search all available secrets sources. The `get_secret()` function also accepts an optional `sources` argument of specific source names (`SecretSourceEnum`) and/or secret manager objects to check.

By default, PyAirbyte will prompt the user for any requested secrets that are not provided via other secret managers. You can disable this prompt by passing `allow_prompt=False` to `get_secret()`.

For more information, see the `airbyte.secrets` module.

### Secrets Auto-Discovery

If you have a secret matching an expected name, PyAirbyte will automatically use it. For example, if you have a secret named `GITHUB_PERSONAL_ACCESS_TOKEN`, PyAirbyte will automatically use it when configuring the GitHub source.

The naming convention for secrets is as `{CONNECTOR_NAME}_{PROPERTY_NAME}`, for instance `SNOWFLAKE_PASSWORD` and `BIGQUERY_CREDENTIALS_PATH`.

PyAirbyte will also auto-discover secrets for interop with hosted Airbyte: `AIRBYTE_CLOUD_API_URL`, `AIRBYTE_CLOUD_API_KEY`, etc.

## Contributing

To learn how you can contribute to PyAirbyte, please see our [PyAirbyte Contributors Guide](./CONTRIBUTING.md).

## Frequently asked Questions

**1. Does PyAirbyte replace Airbyte?**
No.

**2. What is the PyAirbyte cache? Is it a destination?**
Yes, you can think of it as a built-in destination implementation, but we avoid the word "destination" in our docs to prevent confusion with our certified destinations list [here](https://docs.airbyte.com/integrations/destinations/).

**3. Does PyAirbyte work with data orchestration frameworks like Airflow, Dagster, and Snowpark,**
Yes, it should. Please give it a try and report any problems you see. Also, drop us a note if works for you!

**4. Can I use PyAirbyte to develop or test when developing Airbyte sources?**
Yes, you can, but only for Python-based sources.

**5. Can I develop traditional ETL pipelines with PyAirbyte?**
Yes. Just pick the cache type matching the destination - like SnowflakeCache for landing data in Snowflake.

**6. Can PyAirbyte import a connector from a local directory that has python project files, or does it have to be pip install**
Yes, PyAirbyte can use any local install that has a CLI - and will automatically find connectors by name if they are on PATH.

## Changelog and Release Notes

For a version history and list of all changes, please see our [GitHub Releases](https://github.com/airbytehq/PyAirbyte/releases) page.
