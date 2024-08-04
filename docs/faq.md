# PyAirbyte Frequently asked Questions

**1. Does PyAirbyte replace Airbyte?**

No. PyAirbyte is a Python library that allows you to use Airbyte connectors in Python but it does
not have orchestration or scheduling capabilities, nor does is provide logging, alerting, or other
features for managing data pipelines in production. Airbyte is a full-fledged data integration
platform that provides connectors, orchestration, and scheduling capabilities.

**2. What is the PyAirbyte cache? Is it a destination?**

Yes and no. You can think of it as a built-in destination implementation, but we avoid the word
"destination" in our docs to prevent confusion with our certified destinations list
[here](https://docs.airbyte.com/integrations/destinations/).

**3. Does PyAirbyte work with data orchestration frameworks like Airflow, Dagster, and Snowpark,
etc.?**

Yes, it should. Please give it a try and report any problems you see. Also, drop us a note if works
for you!

**4. Can I use PyAirbyte to develop or test when developing Airbyte sources?**

Yes, you can. PyAirbyte makes it easy to test connectors in Python, and you can use it to develop
new local connectors as well as existing already-published ones.

**5. Can I develop traditional ETL pipelines with PyAirbyte?**

Yes. Just pick the cache type matching the destination - like SnowflakeCache for landing data in
Snowflake.

**6. Can PyAirbyte import a connector from a local directory that has python project files, or does
it have to be installed from PyPi?**

Yes, PyAirbyte can use any local install that has a CLI - and will automatically find connectors b
name if they are on PATH.
