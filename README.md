# PyAirbyte

PyAirbyte brings the power of Airbyte to every Python developer. PyAirbyte provides a set of utilities to use Airbyte connectors in Python. It is meant to be used in situations where setting up an Airbyte server or cloud account is not possible or desirable.

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

_Note: Additional secret store options may be supported in the future. [More info here.](https://github.com/airbytehq/airbyte-lib-private-beta/discussions/5)_

### Retrieving Secrets

```python
from airbyte import get_secret, SecretSourceEnum

source = get_source("source-github")
source.set_config(
   "credentials": {
      "personal_access_token": get_secret("GITHUB_PERSONAL_ACCESS_TOKEN"),
   }
)
```

The `get_secret()` function accepts an optional `source` argument of enum type `SecretSourceEnum`. If omitted or set to `SecretSourceEnum.ANY`, PyAirbyte will search all available secrets sources. If `source` is set to a specific source, then only that source will be checked. If a list of `SecretSourceEnum` entries is passed, then the sources will be checked using the provided ordering.

By default, PyAirbyte will prompt the user for any requested secrets that are not provided via other secret managers. You can disable this prompt by passing `prompt=False` to `get_secret()`.

## Connector compatibility

To make a connector compatible with PyAirbyte, the following requirements must be met:

- The connector must be a Python package, with a `pyproject.toml` or a `setup.py` file.
- In the package, there must be a `run.py` file that contains a `run` method. This method should read arguments from the command line, and run the connector with them, outputting messages to stdout.
- The `pyproject.toml` or `setup.py` file must specify a command line entry point for the `run` method called `source-<connector name>`. This is usually done by adding a `console_scripts` section to the `pyproject.toml` file, or a `entry_points` section to the `setup.py` file. For example:

```toml
[tool.poetry.scripts]
source-my-connector = "my_connector.run:run"
```

```python
setup(
    ...
    entry_points={
        'console_scripts': [
            'source-my-connector = my_connector.run:run',
        ],
    },
    ...
)
```

To publish a connector to PyPI, specify the `pypi` section in the `metadata.yaml` file. For example:

```yaml
data:
 # ...
 remoteRegistries:
   pypi:
     enabled: true
     packageName: "airbyte-source-my-connector"
```

## Validating source connectors

To validate a source connector for compliance, the `airbyte-lib-validate-source` script can be used. It can be used like this:

```bash
airbyte-lib-validate-source —connector-dir . -—sample-config secrets/config.json
```

The script will install the python package in the provided directory, and run the connector against the provided config. The config should be a valid JSON file, with the same structure as the one that would be provided to the connector in Airbyte. The script will exit with a non-zero exit code if the connector fails to run.

For a more lightweight check, the `--validate-install-only` flag can be used. This will only check that the connector can be installed and returns a spec, no sample config required.

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
