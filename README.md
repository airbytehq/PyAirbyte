# PyAirbyte

PyAirbyte brings the power of Airbyte to every Python developer. PyAirbyte provides a set of utilities to use Airbyte connectors in Python.

[![PyPI version](https://badge.fury.io/py/airbyte.svg)](https://badge.fury.io/py/airbyte)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/airbyte)](https://pypi.org/project/airbyte/)
[![Star on GitHub](https://img.shields.io/github/stars/airbytehq/pyairbyte.svg?style=social&label=★%20on%20GitHub)](https://github.com/airbytehq/pyairbyte)

## Getting Started

Watch this [Getting Started Loom video](https://www.loom.com/share/3de81ca3ce914feca209bf83777efa3f?sid=8804e8d7-096c-4aaa-a8a4-9eb93a44e850) or run one of our Quickstart tutorials below to see how you can use PyAirbyte in your python code.

* [Basic Demo](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Basic_Features_Demo.ipynb)
* [CoinAPI](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_CoinAPI_Demo.ipynb)
* [GA4](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_GA4_Demo.ipynb)
* [Shopify](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Shopify_Demo.ipynb)
* [GitHub](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Github_Incremental_Demo.ipynb)
* [Postgres (cache)](https://github.com/airbytehq/quickstarts/blob/main/pyairbyte_notebooks/PyAirbyte_Postgres_Custom_Cache_Demo.ipynb)

## Contributing

To learn how you can contribute to PyAirbyte, please see our [PyAirbyte Contributors Guide](./docs/CONTRIBUTING.md).

## Frequently asked Questions

**1. Does PyAirbyte replace Airbyte?**
No. PyAirbyte is a Python library that allows you to use Airbyte connectors in Python, but it does not have orchestration
or scheduling capabilities, nor does is provide logging, alerting, or other features for managing pipelines in
production. Airbyte is a full-fledged data integration platform that provides connectors, orchestration, and scheduling capabilities.

**2. What is the PyAirbyte cache? Is it a destination?**
Yes and no. You can think of it as a built-in destination implementation, but we avoid the word "destination" in our docs to prevent confusion with our certified destinations list [here](https://docs.airbyte.com/integrations/destinations/).

**3. Does PyAirbyte work with data orchestration frameworks like Airflow, Dagster, and Snowpark,**
Yes, it should. Please give it a try and report any problems you see. Also, drop us a note if works for you!

**4. Can I use PyAirbyte to develop or test when developing Airbyte sources?**
Yes, you can. PyAirbyte makes it easy to test connectors in Python, and you can use it to develop new local connectors
as well as existing already-published ones.

**5. Can I develop traditional ETL pipelines with PyAirbyte?**
Yes. Just pick the cache type matching the destination - like SnowflakeCache for landing data in Snowflake.

**6. Can PyAirbyte import a connector from a local directory that has python project files, or does it have to be pip install**
Yes, PyAirbyte can use any local install that has a CLI - and will automatically find connectors by name if they are on PATH.

## Changelog and Release Notes

For a version history and list of all changes, please see our [GitHub Releases](https://github.com/airbytehq/PyAirbyte/releases) page.
