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
            start_from_page: 40
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
