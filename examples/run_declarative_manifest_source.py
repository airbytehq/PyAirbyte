# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A test of PyAirbyte calling a declarative manifest.

Usage (from PyAirbyte root directory):
> poetry run python examples/run_declarative_manifest_source.py

"""

from __future__ import annotations

from typing import cast

import yaml

import airbyte as ab
from airbyte.experimental import get_source


# Copy-pasted from the Builder "Yaml" view:
SOURCE_MANIFEST_TEXT = """
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
      retriever:
        type: SimpleRetriever
        paginator:
          type: DefaultPaginator
          page_token_option:
            type: RequestOption
            field_name: page
            inject_into: request_parameter
          pagination_strategy:
            type: PageIncrement
            start_from_page: 1
        requester:
          $ref: '#/definitions/base_requester'
          path: character/
          http_method: GET
        record_selector:
          type: RecordSelector
          extractor:
            type: DpathExtractor
            field_path:
              - results
      primary_key:
        - id
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
    required:
      - id
    properties:
      type:
        type:
          - string
          - 'null'
      id:
        type: number
      url:
        type:
          - string
          - 'null'
      name:
        type:
          - string
          - 'null'
      image:
        type:
          - string
          - 'null'
      gender:
        type:
          - string
          - 'null'
      origin:
        type:
          - object
          - 'null'
        properties:
          url:
            type:
              - string
              - 'null'
          name:
            type:
              - string
              - 'null'
      status:
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
      species:
        type:
          - string
          - 'null'
      location:
        type:
          - object
          - 'null'
        properties:
          url:
            type:
              - string
              - 'null'
          name:
            type:
              - string
              - 'null'
    additionalProperties: true
"""

source_manifest_dict = cast(dict, yaml.safe_load(SOURCE_MANIFEST_TEXT))

print("Installing declarative source...")
source = get_source(
    "source-rick-and-morty",
    config={},
    source_manifest=source_manifest_dict,
)
source.check()
source.select_all_streams()

result = source.read()

for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")
