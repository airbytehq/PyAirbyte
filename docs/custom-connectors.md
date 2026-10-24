# Using Custom Connectors

PyAirbyte's `get_source()` and `get_destination()` functions can run any connector that is
published to the public [Airbyte connector registry](https://connectors.airbyte.com), but you are
not limited to that registry. This guide covers how to point PyAirbyte at a connector that isn't
registered, including:

- A low-code (YAML) connector exported from the Airbyte Connector Builder.
- A Python connector you are developing locally, or one installed from a private or in-progress
  branch.
- A connector executable that is already installed on your machine.
- A connector packaged as a custom Docker image.

## How PyAirbyte picks an installation method

When you call `get_source()` (or `get_destination()`), PyAirbyte looks at the following optional
args to decide how to run the connector:

- `source_manifest`: run a declarative (YAML) manifest directly. Sources only.
- `pip_url`: install the connector from PyPI, a local path, or a `git+https://...` URL, then run it
  in a Python virtual environment.
- `local_executable`: run a connector executable that is already installed, either by name (found
  on `PATH`) or by an explicit path.
- `docker_image`: run the connector as a Docker container, using either the default image name or
  one you specify.

You can only set **one** of these at a time. If you set more than one, PyAirbyte raises a
`PyAirbyteInputError` telling you to pick a single installation method.

If a connector name isn't found in the public registry and you don't set one of the args above,
PyAirbyte raises `AirbyteConnectorNotRegisteredError`. This is expected for custom connectors:
telling PyAirbyte how to install or run the connector (via one of the four args above) is what lets
it skip the registry lookup.

## Custom low-code (YAML) connectors

If you built a connector using the Airbyte Connector Builder (or you are working with any
declarative source built on the low-code CDK), you can run it directly from its YAML manifest
without publishing it anywhere, by passing the manifest to `source_manifest`.

`source_manifest` accepts:

- `True`, to auto-download the published manifest for a registered connector name.
- A `dict`, if you've already parsed the manifest YAML.
- A `Path`, to load the manifest from a local `.yaml`/`.yml`/`.json` file. If a `components.py` file
  exists next to the manifest file, it is automatically loaded and injected as custom Python
  components.
- A `str` URL, to download the manifest from the web.

For example, using a manifest copied from the Connector Builder's "As YAML" view:

```python
from typing import cast

import yaml

from airbyte import get_source


source_manifest_text = """
version: 0.85.0

type: DeclarativeSource

check:
  type: CheckStream
  stream_names:
    - characters

definitions:
  streams:
    characters:
      type: DeclarativeStream
      name: characters
      primary_key:
        - id
      retriever:
        type: SimpleRetriever
        requester:
          $ref: '#/definitions/base_requester'
          path: character/
          http_method: GET
          error_handler:
            type: CompositeErrorHandler
            error_handlers:
              - type: DefaultErrorHandler
                response_filters:
                  - type: HttpResponseFilter
                    action: SUCCESS
                    error_message_contains: There is nothing here
        record_selector:
          type: RecordSelector
          extractor:
            type: DpathExtractor
            field_path:
              - results
        paginator:
          type: DefaultPaginator
          page_token_option:
            type: RequestOption
            inject_into: request_parameter
            field_name: page
          pagination_strategy:
            type: PageIncrement
            start_from_page: 1
      schema_loader:
        type: InlineSchemaLoader
        schema:
          $ref: '#/schemas/characters'
  base_requester:
    type: HttpRequester
    url_base: https://rickandmortyapi.com/api

streams:
  - $ref: '#/definitions/streams/characters'

spec:
  type: Spec
  connection_specification:
    type: object
    $schema: http://json-schema.org/draft-07/schema#
    required: []
    properties: {}
    additionalProperties: true

metadata:
  autoImportSchema:
    characters: true

schemas:
  characters:
    type: object
    $schema: http://json-schema.org/schema#
    properties:
      type:
        type:
          - string
          - 'null'
      created:
        type:
          - string
          - 'null'
      episode:
        type:
          - array
          - 'null'
        items:
          type:
            - string
            - 'null'
      gender:
        type:
          - string
          - 'null'
      id:
        type: number
      image:
        type:
          - string
          - 'null'
      location:
        type:
          - object
          - 'null'
        properties:
          name:
            type:
              - string
              - 'null'
          url:
            type:
              - string
              - 'null'
      name:
        type:
          - string
          - 'null'
      origin:
        type:
          - object
          - 'null'
        properties:
          name:
            type:
              - string
              - 'null'
          url:
            type:
              - string
              - 'null'
      species:
        type:
          - string
          - 'null'
      status:
        type:
          - string
          - 'null'
      url:
        type:
          - string
          - 'null'
    required:
      - id
    additionalProperties: true
"""

source = get_source(
    "source-rick-and-morty",
    config={},
    source_manifest=cast(dict, yaml.safe_load(source_manifest_text)),
)
source.check()
source.select_all_streams()

result = source.read()
for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")
```

You can run this exact snippet with `uv run python examples/run_declarative_manifest_source.py`
from a checkout of this repo; that example file contains the full manifest shown above.

If you'd rather keep the manifest in its own file, save it as `manifest.yaml` and pass a `Path`
instead:

```python
from pathlib import Path

from airbyte import get_source

source = get_source(
    "source-rick-and-morty",
    config={},
    source_manifest=Path("manifest.yaml"),
)
```

If a `components.py` file exists alongside `manifest.yaml` in the same directory, it will be picked
up automatically, so custom Python components authored for the connector still work.

## Custom Python connectors

If you're developing a Python connector (for example, one built on the
[Airbyte Python CDK](https://github.com/airbytehq/airbyte-python-cdk)), point `pip_url` at your
project instead of a PyPI package name. `pip_url` is passed straight through to `uv pip install`
(or plain `pip install`, if the `AIRBYTE_NO_UV` environment variable is set), so anything that
installer accepts will work, including:

- A local directory path: `pip_url="./my-connectors/source-custom-api"`
- A Git URL, optionally with a branch and subdirectory:
  `pip_url="git+https://github.com/my-org/my-connectors.git@main#egg=source-custom-api&subdirectory=source-custom-api"`
- A private package index URL or an extra index flag, such as
  `pip_url="source-custom-api --extra-index-url https://my-index.example.com/simple"`

Your project needs to install a console-script entry point with the same name as the connector, so
PyAirbyte can find and run it after installation. A minimal `pyproject.toml`/`setup.py` needs
something like:

```python
setup(
    name="airbyte-source-custom-api",
    version="0.1.0",
    packages=["source_custom_api"],
    entry_points={
        "console_scripts": [
            "source-custom-api=source_custom_api.run:run",
        ],
    },
)
```

The entry point must implement the Airbyte connector protocol's `spec`, `check`, `discover`, and
`read` commands, reading and writing newline-delimited JSON Airbyte protocol messages on
stdin/stdout. This is exactly what the CDK's `ConnectorRunner`/`AirbyteEntrypoint` classes do for
you, so in practice you rarely write this protocol handling yourself.

Once your connector installs and exposes that entry point, run it with:

```python
from airbyte import get_source

source = get_source(
    "source-custom-api",
    config={"apiKey": "my-api-key"},
    pip_url="./my-connectors/source-custom-api",
)
source.check()
```

Because `version` is used to pick the version to install from the registry, it can't be combined
with `pip_url` — pin the version directly in your `pip_url` instead (for example, by appending
`@v1.2.3` to a Git URL).

## Already-installed connectors

If you've already installed a connector, either by activating its virtual environment or by
installing it with a tool like `uv tool install` or `pipx`, you don't need PyAirbyte to install
anything. Set `local_executable=True` to use the connector name as the executable name, or pass a
path directly:

```python
from airbyte import get_source

# Uses whichever `source-custom-api` executable is first on PATH:
source = get_source("source-custom-api", local_executable=True, config={"apiKey": "my-api-key"})

# Or, point directly at a specific executable:
source = get_source(
    "source-custom-api",
    local_executable="/home/me/my-connectors/source-custom-api/.venv/bin/source-custom-api",
    config={"apiKey": "my-api-key"},
)
```

`version` can't be combined with `local_executable`: PyAirbyte assumes whatever is installed is the
version you want to run, and it will not try to upgrade or downgrade it.

## Custom Docker images

If your connector is packaged as a Docker image that isn't published to a public registry (for
example, one you built locally with `docker build`), pass the image name and tag directly:

```bash
docker build -t my-org/source-custom-api:dev .
```

```python
from airbyte import get_source

source = get_source(
    "source-custom-api",
    docker_image="my-org/source-custom-api:dev",
    config={"apiKey": "my-api-key"},
)
```

Because the image already exists in your local Docker image cache after `docker build`, PyAirbyte
runs it directly with `docker run` and never tries to pull it from a registry.
