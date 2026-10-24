# Using Already-Installed Connectors

By default, `get_source()` and `get_destination()` will install a connector for you - either into
a Python virtual environment, as a Docker container, or by downloading a declarative YAML manifest.
Sometimes you don't want PyAirbyte to install anything at all, because you already have a working
copy of the connector:

- You are developing a "bring your own" (BYO) connector locally and want to run your working copy
  without publishing it anywhere.
- You already installed a connector CLI yourself (with `pip`, `pipx`, `uv tool install`, etc.), for
  example because you use it outside of PyAirbyte as well, and you don't want a second copy
  installed into a PyAirbyte-managed virtual environment.
- Your connector isn't published to PyPI or the Airbyte connector registry at all, so there is
  nothing for PyAirbyte to look up or install automatically.

In all of these cases, you can point PyAirbyte directly at the executable using the
`local_executable` argument, which is accepted by `get_source()`, `get_destination()`, and the
`pyab`/`pyairbyte` CLI.

## Using a Connector That's Already on Your `PATH`

If the connector's executable is already discoverable on your `PATH` and its name matches the
connector name (as is the case for any connector installed from its standard `airbyte-source-*` or
`airbyte-destination-*` package), you can pass `local_executable=True`:

```python
import airbyte as ab

source = ab.get_source(
    "source-faker",
    local_executable=True,
    config={"count": 100},
)
source.check()
```

For example, if you installed `source-faker` yourself as a standalone CLI tool with
[`uv tool`](https://docs.astral.sh/uv/guides/tools/) instead of letting PyAirbyte manage its own
copy:

```bash
uv tool install airbyte-source-faker
source-faker spec
```

...the snippet above will find and reuse that installation instead of creating a new virtual
environment.

You can also pass the executable name explicitly, which is equivalent to `local_executable=True`
when the name matches the connector name, but also lets you point to an executable with a
different name:

```python
source = ab.get_source(
    "source-faker",
    local_executable="source-faker",
    config={"count": 100},
)
```

Under the hood, PyAirbyte resolves the executable with the same logic as
[`shutil.which()`](https://docs.python.org/3/library/shutil.html#shutil.which), so anything you
could run directly from your shell will be found.

## Using a Connector at a Specific File Path

If the connector executable isn't on your `PATH`, or if you have multiple local copies and want to
select a specific one, pass a `Path` (or a string containing a `/`) instead. This is a common
pattern while developing a connector, where you install it into its own virtual environment
alongside the connector's source code and point PyAirbyte at that environment's binary:

```bash
python -m venv .venv-source-spacex-api
source .venv-source-spacex-api/bin/activate
pip install -e ../airbyte-integrations/connectors/source-spacex-api
```

```python
from pathlib import Path

import airbyte as ab

source = ab.get_source(
    "source-spacex-api",
    local_executable=Path(".venv-source-spacex-api/bin/source-spacex-api"),
    config={"id": "605b4b6aaa5433645e37d03f"},
)
source.check()
```

Note that `version` cannot be combined with `local_executable`: PyAirbyte assumes you know exactly
which build you want to run, so there's no version to resolve.

## Custom or Unregistered Connectors

`local_executable` also works for connectors that aren't published to PyPI, aren't in the Airbyte
connector registry, or don't otherwise follow Airbyte's naming conventions. Normally, PyAirbyte
looks up connector metadata in the registry to decide how to install it, and raises
`AirbyteConnectorNotRegisteredError` if the connector can't be found and no install method was
given. Setting `local_executable` counts as specifying an install method, so this registry lookup
failure is never raised - PyAirbyte will use your executable as-is:

```python
source = ab.get_source(
    "my-internal-source",
    local_executable="/opt/connectors/my-internal-source/bin/my-internal-source",
    config={"api_key": "..."},
)
```

The connector name (`"my-internal-source"` above) is used only for logging, caching table
prefixes, and similar bookkeeping - it does not need to match anything in the Airbyte registry.

## Using the `pyab` CLI

The `pyab` CLI (also installed as `pyairbyte`) accepts a path in place of a connector name for both
sources and destinations. A `source-` name is treated as a registered connector, while a value
containing `/` or starting with `.` is treated as a path to a local executable:

```bash
uv run pyab validate --connector ./.venv-source-spacex-api/bin/source-spacex-api
```

```bash
uv run pyab sync \
  --source ./.venv-source-spacex-api/bin/source-spacex-api \
  --destination destination-dev-null \
  --Sconfig ./spacex-config.json
```

## Troubleshooting

If PyAirbyte can't find the executable you specified, it raises
`AirbyteConnectorExecutableNotFoundError`. Double-check that:

- The path is correct and the file exists and is executable (`chmod +x` on Unix-like systems).
- If you passed a bare name instead of a path, that name is actually resolvable on your `PATH` in
  the same shell/process that runs your Python script (running `which <name>` should print the
  same executable you expect PyAirbyte to use).
